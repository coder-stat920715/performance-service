package com.enterprise.performance.service.kafka;

import com.enterprise.performance.config.KafkaConfig;
import com.enterprise.performance.dto.TransactionResponse;
import com.enterprise.performance.util.PerformanceMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionEventProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventProducer.class);

    private final KafkaTemplate<String, TransactionResponse> kafkaTemplate;
    private final PerformanceMetrics metrics;

    public TransactionEventProducer(KafkaTemplate<String, TransactionResponse> kafkaTemplate,
                                    PerformanceMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
    }

    /** Keyed by accountId so all events of one account land on one partition (ordering preserved). */
    public void publish(TransactionResponse event) {
        try {
            kafkaTemplate.send(KafkaConfig.TOPIC, event.accountId(), event)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            metrics.recordEventPublished(false);
                            log.error("Failed to publish event for transaction {}", event.id(), ex);
                        } else {
                            metrics.recordEventPublished(true);
                            log.debug("Published transaction {} to partition {} offset {}",
                                    event.id(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (RuntimeException e) {
            metrics.recordEventPublished(false);
            log.error("Kafka send rejected for transaction {}", event.id(), e);
        }
    }
}
