package com.enterprise.performance.repository;

import com.enterprise.performance.entity.Transaction;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {

    /** Idempotency lookup, served by the unique index on reference: O(log N). */
    Optional<Transaction> findByReference(String reference);

    /**
     * Keyset pagination on (account_id, created_at DESC): O(log N + k), no OFFSET scan,
     * single statement (no N+1).
     */
    @Query(value = """
            SELECT * FROM transactions
            WHERE account_id = :accountId AND created_at < :before
            ORDER BY created_at DESC
            LIMIT :limit
            """, nativeQuery = true)
    List<Transaction> findPageByAccount(@Param("accountId") String accountId,
                                        @Param("before") Instant before,
                                        @Param("limit") int limit);

    /** Idempotent state transition used by the Kafka consumer. Returns rows changed (0 or 1). */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE transactions
            SET status = :newStatus, updated_at = :now, version = version + 1
            WHERE id = :id AND status = :expectedStatus
            """, nativeQuery = true)
    int transition(@Param("id") UUID id,
                   @Param("expectedStatus") String expectedStatus,
                   @Param("newStatus") String newStatus,
                   @Param("now") Instant now);
}
