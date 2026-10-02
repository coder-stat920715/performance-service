# performance-service: Postman Testing Guide

## 1. Prerequisites
- Docker + Docker Compose, Postman (desktop or web with the agent).
- Unzip `microservice-performance-architecture.zip` and start everything:

```bash
cd performance-service
docker compose up --build -d
docker compose logs -f app        # wait for "Started PerformanceApplication"
curl localhost:8080/actuator/health   # {"status":"UP"}
```

Default credentials: `perf` / `change-me` (HTTP Basic). Services: app `:8080`, Postgres `:5432`, Redis `:6379`, Kafka `:9092`, Zipkin `:9411`.

## 2. Import into Postman
1. **Import** both files: `performance-service.postman_collection.json` and `performance-service.postman_environment.json`.
2. Select the environment **performance-service local** (top right).
3. Variables: `baseUrl`, `username`, `password` come from the environment. `accountId`, `reference`, `transactionId`, `cursor` are collection variables the scripts fill in.

## 3. Running the tests
**Recommended:** right-click the collection > **Run collection**, tick folders **00 to 05**, iterations `1`, delay `0 ms`, keep "Persist variables" on. Folder 06 is manual (section 5).

Two requests intentionally wait, so a full run takes about 40 seconds:
- *Poll until COMPLETED* re-runs itself up to 10 times (500 ms apart).
- *After L1 expiry* waits 31 s before sending.

If you click Send manually instead of using the Runner: run requests top to bottom, and for the poll request just press Send until `status` shows `COMPLETED`.

## 4. What each request proves

### 00 Health & Security
| Request | Expect | Verifies |
|---|---|---|
| Health (no auth) | 200, `status: UP` | App, Postgres, Redis, breakers healthy |
| Readiness probe | 200 UP | Orchestrator probe |
| API without credentials | 401 | Stateless security filter chain |
| API with wrong password | 401 | Credential check |

### 01 Write path
| Request | Expect | Verifies |
|---|---|---|
| Create transaction | 201, `status: PENDING`, `Location` header | Bulkhead > CircuitBreaker > Retry > transactional insert; event published after commit. Saves `transactionId` |
| Idempotent replay | 201, **same id** | Unique `reference` lookup prevents duplicates |

### 02 Read path (async + caching)
| Request | Expect | Verifies |
|---|---|---|
| Get immediately | 200, PENDING or COMPLETED | PENDING rows are not cached |
| Poll until COMPLETED | COMPLETED within a few seconds | Kafka consumer processed the event and updated the row |
| Served from cache | 200, fast (< 250 ms) | L1 Caffeine hit (COMPLETED rows are cached) |
| After L1 expiry | 200 | L1 miss, **L2 Redis hit** |
| Unknown id | 404 ProblemDetail | Not-found is not counted as a breaker failure |
| Malformed id | 400 | Type-mismatch handler |

### 03 Query path
Three seed requests create extra rows for the same `accountId`, then:
| Request | Expect |
|---|---|
| Page 1 (limit 2) | 1 to 2 items, newest first, saves `cursor` |
| Page 2 (`before={{cursor}}`) | only rows older than the cursor (keyset pagination, no OFFSET) |
| limit=0 | exactly 1 item (clamped) |
| limit=1000 | at most 200 items (capped) |
| Unknown account | `[]` |

### 04 Validation
Empty body, negative amount, lowercase currency, more than 4 decimals, bad enum and malformed JSON all return **400** (RFC 7807 body; field problems are listed under `errors`).

### 05 Observability
- **Prometheus**: custom counters `perf_kafka_published_total`, `perf_kafka_consumed_total`, `perf_cache_requests_total`, timer `perf_db_read_seconds_count`, plus Hikari max=20 / min=10 and the breaker state gauge.
- **Caffeine hits**: `/actuator/metrics/cache.gets?tag=result:hit` (value increases after "served from cache").
- **Circuit breakers**: `transactionRead`, `transactionWrite`, `transactionList` all `CLOSED`.
- **Breaker events**: recent successes, failures, transitions.

## 5. Manual deep-dive checks (terminal)

**Database row and indexes**
```bash
docker compose exec postgres psql -U perf -d perfdb -c "select id,reference,status,version from transactions order by created_at desc limit 5;"
docker compose exec postgres psql -U perf -d perfdb -c "\d transactions"
docker compose exec postgres psql -U perf -d perfdb -c "explain select * from transactions where account_id='acc-postman-1' and created_at < now() order by created_at desc limit 5;"
```
The plan should use `idx_transactions_account_created`.

**Redis (L2) entries**
```bash
docker compose exec redis redis-cli --scan --pattern 'perf:transactions::*'
docker compose exec redis redis-cli ttl "perf:transactions::<transactionId>"   # about 300
```
Only COMPLETED rows appear here.

**Kafka events**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactions.events --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group performance-service-group
```
`LAG` should be 0 when the consumer is caught up. Dead letters: topic `transactions.events.DLT`.

**Tracing**: open http://localhost:9411 and Run Query. Sampling defaults to 10%. For a demo, restart the app with `TRACING_SAMPLE_RATE=1.0`.

## 6. Failure-injection scenarios

### A. Redis down (graceful degradation)
1. `docker compose stop redis`
2. Create a new transaction, wait until COMPLETED, then run *Get by id* after 31 s (L1 expired).
3. **Expect 200**. The L2 read fails, is logged as a warning, and the app falls back to Postgres.
4. `docker compose start redis`.

### B. Postgres down (retry, breaker, fallback)
1. Create a transaction and note its id. Wait 5+ minutes (or `docker compose exec redis redis-cli flushall` and wait 31 s) so no cache holds it.
2. `docker compose stop postgres`
3. Run **06 > Read during Postgres outage**. It takes about 6 s at first (3 attempts x 2 s connection timeout), then returns **503** with `Retry-After: 5`.
4. Run it 10 to 20 more times, or use the Runner with 25 iterations. After the breaker's minimum of 20 calls with at least 50% failures, `GET /actuator/circuitbreakers` shows `transactionRead` as **OPEN** and responses become **instant 503s**.
5. Run **Write during Postgres outage**: 503 with `Retry-After`.
6. `docker compose start postgres`. After 10 s (read) or 15 s (write) the breaker goes HALF_OPEN, 5 trial calls succeed, and it returns to CLOSED. Verify with `/actuator/circuitbreakerevents`.

Note: COMPLETED transactions still cached in L1 (30 s) or L2 (300 s) keep returning 200 during the outage, which is the intended benefit of the cache tiers.

### C. Kafka down
`docker compose stop kafka`, create a transaction: the API still returns **201** (publish failure is logged and counted in `perf_kafka_published_total{result="failure"}`). The row stays PENDING. Note that the event is not re-sent automatically in this reference implementation; production setups add a transactional outbox or a sweep of PENDING rows (the partial index `idx_transactions_pending` exists for this).

### D. Bulkhead / load
Postman is not a load tool, but you can use the Runner with 200 iterations and 0 ms delay on *Get by id*, or from a terminal:
```bash
hey -n 20000 -c 200 -A application/json -a perf:change-me http://localhost:8080/api/v1/transactions/<transactionId>
```
Watch `hikaricp_connections_active` (never above 20), `resilience4j_bulkhead_available_concurrent_calls`, and p95/p99 latency in `/actuator/prometheus`. Cached reads should stay in single-digit milliseconds.

## 7. Troubleshooting
| Symptom | Cause / fix |
|---|---|
| 401 on everything | Environment not selected, or password differs from `APP_PASSWORD` |
| Poll never reaches COMPLETED | Kafka not ready or consumer lag. Check `docker compose logs app` and the consumer group |
| 503 on first requests | App started before Postgres/Kafka were ready. Retry after the health check shows UP |
| Cache test fails on timing | Run on an idle machine, or raise the 250 ms threshold in the request's Tests tab |
| Prometheus test fails | Run folders 01 to 03 first so the custom counters exist |
| App container restarts | `docker compose logs app`. Usually Kafka is still booting; `restart: on-failure` retries |
