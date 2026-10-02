package com.enterprise.performance.service.kafka;

import com.enterprise.performance.config.CacheConfig;
import com.enterprise.performance.config.KafkaConfig;
import com.enterprise.performance.dto.TransactionResponse;
import com.enterprise.performance.entity.Transaction;
import com.enterprise.performance.repository.TransactionRepository;
import com.enterprise.performance.util.PerformanceMetrics;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Background worker: moves PENDING transactions to COMPLETED. The UPDATE is guarded by the
 * expected status, so redelivered events are harmless (at-least-once + idempotent handler).
 */
@Component
public class TransactionEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventConsumer.class);

    private final TransactionRepository repository;
    private final CacheManager l1CacheManager;
    private final CacheManager l2CacheManager;
    private final PerformanceMetrics metrics;

    public TransactionEventConsumer(TransactionRepository repository,
                                    @Qualifier(CacheConfig.L1_MANAGER) CacheManager l1CacheManager,
                                    @Qualifier(CacheConfig.L2_MANAGER) CacheManager l2CacheManager,
                                    PerformanceMetrics metrics) {
        this.repository = repository;
        this.l1CacheManager = l1CacheManager;
        this.l2CacheManager = l2CacheManager;
        this.metrics = metrics;
    }

    @KafkaListener(topics = KafkaConfig.TOPIC,
            groupId = "${app.kafka.consumer-group}",
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onTransactionCreated(TransactionResponse event) {
        int updated = repository.transition(
                event.id(),
                Transaction.Status.PENDING.name(),
                Transaction.Status.COMPLETED.name(),
                Instant.now());
        evict(l1CacheManager, event.id().toString());
        evict(l2CacheManager, event.id().toString());
        metrics.recordEventConsumed(updated > 0);
        log.debug("Processed transaction {} (rows updated: {})", event.id(), updated);
    }

    private void evict(CacheManager manager, String key) {
        try {
            Cache cache = manager.getCache(CacheConfig.TRANSACTIONS_CACHE);
            if (cache != null) {
                cache.evict(key);
                if (manager == l1CacheManager) {
                    cache.evict(java.util.UUID.fromString(key));
                }
            }
        } catch (RuntimeException e) {
            log.warn("Cache eviction failed for {}: {}", key, e.toString());
        }
    }
}
