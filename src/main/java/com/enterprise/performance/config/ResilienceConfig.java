package com.enterprise.performance.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Resilience4j thresholds live in application.yml (see the matrix in ARCHITECTURE.md).
 * This class adds circuit breaker state-transition logging and the virtual-thread executor
 * used by the TimeLimiter-protected async query path.
 */
@Configuration
public class ResilienceConfig {

    private static final Logger log = LoggerFactory.getLogger(ResilienceConfig.class);

    @Bean(name = "virtualThreadExecutor", destroyMethod = "close")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public RegistryEventConsumer<CircuitBreaker> circuitBreakerEventLogger() {
        return new RegistryEventConsumer<>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> event) {
                event.getAddedEntry().getEventPublisher()
                        .onStateTransition(e -> log.warn("CircuitBreaker '{}' transitioned {} -> {}",
                                e.getCircuitBreakerName(),
                                e.getStateTransition().getFromState(),
                                e.getStateTransition().getToState()))
                        .onCallNotPermitted(e -> log.debug("CircuitBreaker '{}' rejected a call",
                                e.getCircuitBreakerName()));
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> event) {
                log.info("CircuitBreaker '{}' removed", event.getRemovedEntry().getName());
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> event) {
                log.info("CircuitBreaker '{}' replaced", event.getNewEntry().getName());
            }
        };
    }
}
