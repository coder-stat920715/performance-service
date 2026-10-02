# performance-service: HLD & LLD

## Part 1: High-Level Design

### 1.1 Request flow

```
                         +--------------------+
   Clients ------------> | API Gateway / L7   |  TLS, rate limit, routing
                         | Load Balancer      |
                         +---------+----------+
                                   |
              +--------------------+--------------------+
              |                    |                    |
        +-----v-----+        +-----v-----+        +-----v-----+
        | Instance 1|        | Instance 2|  ...   | Instance N|   Java 21, virtual threads
        +-----+-----+        +-----------+        +-----------+
              |
   WRITE PATH (command)                       READ PATH (query)
   POST /api/v1/transactions                  GET /api/v1/transactions/{id}
        |                                          |
   Bulkhead -> CircuitBreaker -> Retry        L1 Caffeine (@Cacheable, 30 s, 10k entries)
        |                                          | miss
   @Transactional                              Bulkhead -> CircuitBreaker -> Retry
   idempotent insert (unique reference)            |
        |                                      L2 Redis (TTL 5 min, JSON)
   PostgreSQL PRIMARY                              | miss
        |                                      PostgreSQL (read-only tx; replica-ready)
   afterCommit()                                   |
        |                                      populate L2, then L1 (terminal states only)
   Kafka topic transactions.events
        |
   Consumer group (3 threads/instance, DLT after 3 retries)
        |
   UPDATE status PENDING -> COMPLETED (idempotent) + cache eviction
```

### 1.2 CQRS separation
- **Command side**: `create()` is the only writer. It is idempotent on `reference`, transactional, and publishes its event only after the DB commit (no dual-write inconsistency window for phantom events).
- **Query side**: `getById()` runs through L1 -> L2 -> DB; `listByAccount()` uses keyset pagination on `(account_id, created_at DESC)` and runs on a virtual thread under a TimeLimiter.
- Reads run in `readOnly` transactions, so pointing the read datasource at a replica is a pure deployment change.

### 1.3 Caching layers
| Tier | Tech | Scope | TTL | Notes |
|------|------|-------|-----|-------|
| L1 | Caffeine | per JVM | 30 s | `@Cacheable`, sits outside the resilience aspects, so hits never touch the circuit breaker |
| L2 | Redis | cluster-wide | 300 s | Typed Jackson serializer, key prefix `perf:`; failures degrade to DB |
| L3 | PostgreSQL | source of truth | n/a | Only terminal-state rows are cached, so PENDING rows never go stale |

### 1.4 Async event bus
Producer: idempotent, `acks=all`, LZ4, 5 ms linger, keyed by `accountId` (per-account ordering). Consumer: manual-commit semantics via container, `ErrorHandlingDeserializer`, exponential back-off (500 ms x2, 3 retries), then `transactions.events.DLT`.

### 1.5 Circuit breaker state machine
```
          failure rate >= 50% OR slow calls >= 80%
 CLOSED -------------------------------------------> OPEN
   ^                                                   |
   | success rate recovers                             | wait-duration elapsed (auto)
   |                                                   v
   +------------------------------------------- HALF_OPEN (5 trial calls)
                      trial failures -> back to OPEN
```

## Part 2: Low-Level Design

### 2.1 Database schema
See `src/main/resources/db/schema.sql`. Lookup complexity:
- `PRIMARY KEY (id)`: O(log N) B-tree point read.
- `UNIQUE (reference)`: O(log N) idempotency check and race protection.
- `(account_id, created_at DESC)`: O(log N + k) keyset pagination, no OFFSET scans.
- Partial index `WHERE status = 'PENDING'`: tiny index for backlog sweeps.

N+1 avoidance: the entity has no lazy associations, list endpoints are single statements, and Hibernate batch settings (`jdbc.batch_size=50`, `order_inserts`, `default_batch_fetch_size=32`) are enabled for any future associations.

### 2.2 Component flow
```
TransactionController
   |-- create  --> TransactionServiceImpl.create --> TransactionRepository (JPA) --> PostgreSQL
   |                         '--afterCommit--> TransactionEventProducer --> Kafka
   |-- getById --> [L1 @Cacheable] TransactionServiceImpl.getById --> L2 Redis --> TransactionRepository
   '-- list    --> TransactionServiceImpl.listByAccount (virtual thread, TimeLimiter) --> TransactionRepository (native keyset)

Kafka --> TransactionEventConsumer --> TransactionRepository.transition --> evict L1/L2
GlobalExceptionHandler: RFC 7807 ProblemDetail for every error
PerformanceMetrics: Micrometer timers/counters (cache tiers, DB reads, events)
```

### 2.3 Resilience configuration matrix
| Instance | CB window | Min calls | Failure thr. | Slow call | Open wait | Half-open | Retry | Bulkhead | TimeLimiter |
|----------|-----------|-----------|--------------|-----------|-----------|-----------|-------|----------|-------------|
| transactionRead | COUNT 50 | 20 | 50% | >1 s @ 80% | 10 s | 5 | 3 x, 100 ms, x2 | 100 concurrent, 0 wait | n/a |
| transactionWrite | COUNT 50 | 20 | 50% | >2 s @ 80% | 15 s | 5 | 3 x, 200 ms, x2 | 50 concurrent, 500 ms wait | n/a |
| transactionList | TIME 30 s | 20 | 50% | >1.5 s @ 80% | 10 s | 5 | n/a | 40 concurrent, 0 wait | 2 s, cancel |

Fallback is attached only to the outermost decorator of each method so that inner fallbacks never swallow exceptions the retry should see.

## Running
```bash
docker compose up --build
curl -u perf:change-me -X POST localhost:8080/api/v1/transactions \
  -H 'Content-Type: application/json' \
  -d '{"reference":"ref-1","accountId":"acc-1","amount":10.50,"currency":"USD","type":"CREDIT","description":"test"}'
curl -u perf:change-me localhost:8080/api/v1/transactions/<id>
curl -u perf:change-me "localhost:8080/api/v1/transactions/account/acc-1?limit=20"
```
Zipkin UI: http://localhost:9411, metrics: `/actuator/prometheus`, breakers: `/actuator/circuitbreakers`.
