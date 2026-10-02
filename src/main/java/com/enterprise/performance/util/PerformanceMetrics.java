package com.enterprise.performance.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;
import org.springframework.stereotype.Component;

@Component
public class PerformanceMetrics {

    private final MeterRegistry registry;
    private final Timer dbReadTimer;

    public PerformanceMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.dbReadTimer = Timer.builder("perf.db.read")
                .description("Latency of database read operations")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .register(registry);
    }

    public void recordCacheHit(String tier) {
        registry.counter("perf.cache.requests", "tier", tier, "result", "hit").increment();
    }

    public void recordCacheMiss(String tier) {
        registry.counter("perf.cache.requests", "tier", tier, "result", "miss").increment();
    }

    public <T> T timeDbRead(Supplier<T> action) {
        return dbReadTimer.record(action);
    }

    public void recordEventPublished(boolean success) {
        registry.counter("perf.kafka.published", "result", success ? "success" : "failure").increment();
    }

    public void recordEventConsumed(boolean applied) {
        registry.counter("perf.kafka.consumed", "result", applied ? "applied" : "skipped").increment();
    }
}
