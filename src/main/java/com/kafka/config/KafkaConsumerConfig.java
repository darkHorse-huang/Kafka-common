package com.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka consumer configuration.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:kafka-common-group}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.enable-auto-commit:false}")
    private Boolean enableAutoCommit;

    @Value("${spring.kafka.consumer.max-poll-records:500}")
    private Integer maxPollRecords;

    @Value("${spring.kafka.consumer.concurrency:3}")
    private Integer concurrency;

    @Value("${spring.kafka.consumer.session-timeout-ms:30000}")
    private Integer sessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat-interval-ms:3000}")
    private Integer heartbeatIntervalMs;

    /**
     * Consumer factory configuration.
     *
     * @return ConsumerFactory instance
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        // Key deserializer
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Value deserializer with error handling
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "java.lang.Object");
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // Isolation level
        configProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    /**
     * Concurrent Kafka listener container factory configuration.
     *
     * @param consumerFactory the consumer factory
     * @param kafkaTemplate    the kafka template for DLT
     * @return ConcurrentKafkaListenerContainerFactory instance
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // Set concurrency (number of concurrent consumer threads)
        factory.setConcurrency(concurrency);

        // Configure acknowledgment mode
        // MANUAL_IMMEDIATE: Acknowledge immediately after processing
        // MANUAL: Acknowledge after batch processing
        // BATCH: Auto-acknowledge after batch
        ContainerProperties containerProperties = factory.getContainerProperties();
        if (enableAutoCommit) {
            // Auto commit mode
            containerProperties.setAckMode(ContainerProperties.AckMode.BATCH);
        } else {
            // Manual commit mode
            containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        }

        // Configure error handler with dead letter queue support
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    // DLT topic naming: original-topic-dlt
                    String originalTopic = record.topic();
                    return new org.apache.kafka.common.TopicPartition(originalTopic + "-dlt", record.partition());
                }
        );

        // Back off strategy: fixed delay between retries
        BackOff backOff = new FixedBackOff(1000L, 3L); // 1 second delay, 3 retries

        // Error handler configuration
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                backOff
        );

        // Add exception types that should not be retried (will go directly to DLT)
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                IllegalStateException.class
        );

        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}

