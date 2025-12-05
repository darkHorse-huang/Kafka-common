package com.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${kafka.producer.transaction-id-prefix:kafka-tx-}")
    private String transactionIdPrefix;

    @Value("${kafka.producer.retries:3}")
    private Integer retries;

    @Value("${kafka.producer.acks:all}")
    private String acks;

    @Value("${kafka.producer.batch-size:16384}")
    private Integer batchSize;

    @Value("${kafka.producer.linger-ms:1}")
    private Integer lingerMs;

    @Value("${kafka.producer.buffer-memory:33554432}")
    private Long bufferMemory;

    /**
     * Producer factory configuration.
     *
     * @return ProducerFactory instance
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // Idempotence configuration
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        // Retry configuration
        configProps.put(ProducerConfig.RETRIES_CONFIG, retries);
        configProps.put(ProducerConfig.ACKS_CONFIG, acks);
        
        // Performance configuration
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        
        // Transaction configuration
        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionIdPrefix + System.currentTimeMillis());
        
        // Compression
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Kafka template configuration.
     *
     * @param producerFactory the producer factory
     * @return KafkaTemplate instance
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Kafka transaction manager configuration.
     *
     * @param producerFactory the producer factory
     * @return KafkaTransactionManager instance
     */
    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }

    /**
     * ObjectMapper bean for JSON serialization.
     *
     * @return ObjectMapper instance
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}

