package com.enterprise.performance.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Explicit HikariCP pool. Pool settings (maximum-pool-size: 20, minimum-idle: 10, timeouts,
 * leak detection) are bound from {@code spring.datasource.hikari.*} in application.yml.
 * The pool is intentionally small: with virtual threads, many tasks queue cheaply on the pool
 * instead of exhausting platform threads, and PostgreSQL performs best with few active connections.
 */
@Configuration
public class HikariConfig {

    @Bean
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public HikariDataSource dataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();
    }
}
