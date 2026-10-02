package com.enterprise.performance.dto;

import com.enterprise.performance.entity.Transaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record TransactionResponse(
        UUID id,
        String reference,
        String accountId,
        BigDecimal amount,
        String currency,
        Transaction.Type type,
        Transaction.Status status,
        String description,
        Instant createdAt,
        Instant updatedAt) {

    public static TransactionResponse from(Transaction t) {
        return new TransactionResponse(
                t.getId(),
                t.getReference(),
                t.getAccountId(),
                t.getAmount(),
                t.getCurrency(),
                t.getType(),
                t.getStatus(),
                t.getDescription(),
                t.getCreatedAt(),
                t.getUpdatedAt());
    }
}
