package com.enterprise.performance.service.impl;

import com.enterprise.performance.config.CacheConfig;
import com.enterprise.performance.dto.TransactionRequest;
import com.enterprise.performance.dto.TransactionResponse;
import com.enterprise.performance.entity.Transaction;
import com.enterprise.performance.exception.GlobalExceptionHandler.ServiceDegradedException;
import com.enterprise.performance.exception.GlobalExceptionHandler.TransactionNotFoundException;
import com.enterprise.performance.repository.TransactionRepository;
import com.enterprise.performance.service.TransactionService;
import com.enterprise.performance.service.kafka.TransactionEventProducer;
import com.enterprise.performance.util.PerformanceMetrics;
import io.github.resilience4j.bulkhead.annotation.Bulkhead;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class TransactionServiceImpl implements TransactionService {

    private static final Logger log = LoggerFactory.getLogger(TransactionServiceImpl.class);
    private static final int MAX_PAGE_SIZE = 200;

    private final TransactionRepository repository;
    private final TransactionEventProducer producer;
    private final CacheManager l2CacheManager;
    private final PerformanceMetrics metrics;
    private final Executor virtualThreadExecutor;

    public TransactionServiceImpl(TransactionRepository repository,
                                  TransactionEventProducer producer,
                                  @Qualifier(CacheConfig.L2_MANAGER) CacheManager l2CacheManager,
                                  PerformanceMetrics metrics,
                                  @Qualifier("virtualThreadExecutor") Executor virtualThreadExecutor) {
        this.repository = repository;
        this.producer = producer;
        this.l2CacheManager = l2CacheManager;
        this.metrics = metrics;
        this.virtualThreadExecutor = virtualThreadExecutor;
    }

    // ------------------------------------------------------------------ write path

    @Override
    @Transactional
    @Bulkhead(name = "transactionWrite")
    @CircuitBreaker(name = "transactionWrite")
    @Retry(name = "transactionWrite", fallbackMethod = "createFallback")
    public TransactionResponse create(TransactionRequest request) {
        var existing = repository.findByReference(request.reference());
        if (existing.isPresent()) {
            return TransactionResponse.from(existing.get());
        }

        Instant now = Instant.now();
        Transaction tx = new Transaction();
        tx.setId(UUID.randomUUID());
        tx.setReference(request.reference());
        tx.setAccountId(request.accountId());
        tx.setAmount(request.amount());
        tx.setCurrency(request.currency());
        tx.setType(request.type());
        tx.setStatus(Transaction.Status.PENDING);
        tx.setDescription(request.description());
        tx.setCreatedAt(now);
        tx.setUpdatedAt(now);

        Transaction saved = repository.saveAndFlush(tx);
        TransactionResponse response = TransactionResponse.from(saved);
        publishAfterCommit(response);
        return response;
    }

    public TransactionResponse createFallback(TransactionRequest request, Throwable t) {
        if (t instanceof DataIntegrityViolationException dive) {
            throw dive;
        }
        log.warn("Write path degraded for reference {}: {}", request.reference(), t.toString());
        throw new ServiceDegradedException("Write path is temporarily unavailable, please retry", t);
    }

    private void publishAfterCommit(TransactionResponse response) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    producer.publish(response);
                }
            });
        } else {
            producer.publish(response);
        }
    }

    // ------------------------------------------------------------------ read path

    @Override
    @Cacheable(cacheNames = CacheConfig.TRANSACTIONS_CACHE,
            cacheManager = CacheConfig.L1_MANAGER,
            key = "#id",
            unless = "#result.status().name() == 'PENDING'")
    @Bulkhead(name = "transactionRead")
    @CircuitBreaker(name = "transactionRead")
    @Retry(name = "transactionRead", fallbackMethod = "getByIdFallback")
    public TransactionResponse getById(UUID id) {
        String key = id.toString();

        TransactionResponse fromL2 = readL2(key);
        if (fromL2 != null) {
            metrics.recordCacheHit("l2");
            return fromL2;
        }
        metrics.recordCacheMiss("l2");

        TransactionResponse fromDb = metrics.timeDbRead(() -> repository.findById(id)
                .map(TransactionResponse::from)
                .orElseThrow(() -> new TransactionNotFoundException(id)));

        writeL2(key, fromDb);
        return fromDb;
    }

    public TransactionResponse getByIdFallback(UUID id, Throwable t) {
        if (t instanceof TransactionNotFoundException notFound) {
            throw notFound;
        }
        log.warn("Read path degraded for id {}: {}", id, t.toString());
        throw new ServiceDegradedException("Read path is temporarily unavailable, please retry", t);
    }

    @Override
    @TimeLimiter(name = "transactionList")
    @Bulkhead(name = "transactionList")
    @CircuitBreaker(name = "transactionList", fallbackMethod = "listFallback")
    public CompletableFuture<List<TransactionResponse>> listByAccount(String accountId, Instant before, int limit) {
        int bounded = Math.max(1, Math.min(limit, MAX_PAGE_SIZE));
        Instant cursor = before != null ? before : Instant.now().plus(1, ChronoUnit.DAYS);
        return CompletableFuture.supplyAsync(
                () -> metrics.timeDbRead(() -> repository.findPageByAccount(accountId, cursor, bounded)
                        .stream()
                        .map(TransactionResponse::from)
                        .toList()),
                virtualThreadExecutor);
    }

    public CompletableFuture<List<TransactionResponse>> listFallback(String accountId, Instant before,
                                                                     int limit, Throwable t) {
        log.warn("List path degraded for account {}: {}", accountId, t.toString());
        return CompletableFuture.failedFuture(
                new ServiceDegradedException("Account history is temporarily unavailable, please retry", t));
    }

    // ------------------------------------------------------------------ L2 helpers (never fail the request)

    private TransactionResponse readL2(String key) {
        try {
            Cache cache = l2CacheManager.getCache(CacheConfig.TRANSACTIONS_CACHE);
            return cache == null ? null : cache.get(key, TransactionResponse.class);
        } catch (RuntimeException e) {
            log.warn("L2 cache read failed, falling back to database: {}", e.toString());
            return null;
        }
    }

    private void writeL2(String key, TransactionResponse value) {
        if (value.status() == Transaction.Status.PENDING) {
            return; // only terminal states are cached, so cached entries never go stale
        }
        try {
            Cache cache = l2CacheManager.getCache(CacheConfig.TRANSACTIONS_CACHE);
            if (cache != null) {
                cache.put(key, value);
            }
        } catch (RuntimeException e) {
            log.warn("L2 cache write failed: {}", e.toString());
        }
    }
}
