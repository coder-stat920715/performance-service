package com.enterprise.performance.service;

import com.enterprise.performance.dto.TransactionRequest;
import com.enterprise.performance.dto.TransactionResponse;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface TransactionService {

    /** Command side: idempotent create (keyed by reference), event published after commit. */
    TransactionResponse create(TransactionRequest request);

    /** Query side: L1 Caffeine -> L2 Redis -> database. */
    TransactionResponse getById(UUID id);

    /** Query side: keyset-paginated account history, bounded by a TimeLimiter. */
    CompletableFuture<List<TransactionResponse>> listByAccount(String accountId, Instant before, int limit);
}
