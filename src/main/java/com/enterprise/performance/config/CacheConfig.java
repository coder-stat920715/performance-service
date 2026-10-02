package com.enterprise.performance.config;

import com.enterprise.performance.dto.TransactionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.Ordered;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Two-tier cache: L1 Caffeine (per JVM, annotation driven) and L2 Redis (distributed).
 * The cache aspect has the highest precedence so L1 hits short-circuit the resilience aspects.
 */
@Configuration
@EnableCaching(order = Ordered.HIGHEST_PRECEDENCE)
public class CacheConfig {

    public static final String L1_MANAGER = "l1CacheManager";
    public static final String L2_MANAGER = "l2CacheManager";
    public static final String TRANSACTIONS_CACHE = "transactions";

    @Bean(name = L1_MANAGER)
    @Primary
    public CacheManager l1CacheManager(
            @Value("${app.cache.l1.max-size:10000}") long maxSize,
            @Value("${app.cache.l1.ttl-seconds:30}") long ttlSeconds) {
        CaffeineCacheManager manager = new CaffeineCacheManager(TRANSACTIONS_CACHE);
        manager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                .recordStats());
        manager.setAllowNullValues(false);
        return manager;
    }

    @Bean(name = L2_MANAGER)
    public CacheManager l2CacheManager(
            RedisConnectionFactory connectionFactory,
            ObjectMapper objectMapper,
            @Value("${app.cache.l2.ttl-seconds:300}") long ttlSeconds) {
        Jackson2JsonRedisSerializer<TransactionResponse> valueSerializer =
                new Jackson2JsonRedisSerializer<>(objectMapper, TransactionResponse.class);

        RedisCacheConfiguration configuration = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofSeconds(ttlSeconds))
                .disableCachingNullValues()
                .prefixCacheNameWith("perf:")
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(valueSerializer));

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(configuration)
                .withCacheConfiguration(TRANSACTIONS_CACHE, configuration)
                .build();
    }
}
