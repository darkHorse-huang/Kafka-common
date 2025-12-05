package com.kafka.config;

import com.kafka.core.producer.EnhancedKafkaTemplate;
import com.kafka.metrics.KafkaMetrics;
import com.kafka.metrics.KafkaTraceInterceptor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Auto-configuration for Kafka common components.
 * This configuration is automatically loaded when Spring Boot detects the necessary dependencies.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Configuration
@ConditionalOnClass({KafkaTemplate.class, EnhancedKafkaTemplate.class})
@AutoConfigureAfter(KafkaAutoConfiguration.class)
public class KafkaAutoConfiguration {

    /**
     * Enhanced Kafka template bean.
     * This bean provides enhanced Kafka message sending capabilities with automatic
     * message wrapping, header management, and monitoring integration.
     *
     * @param kafkaTemplate the Kafka template
     * @param objectMapper  the object mapper for JSON serialization
     * @return EnhancedKafkaTemplate instance
     */
    @Bean
    @ConditionalOnMissingBean
    public EnhancedKafkaTemplate enhancedKafkaTemplate(
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new EnhancedKafkaTemplate(kafkaTemplate, objectMapper);
    }

    /**
     * Kafka metrics bean.
     * This bean is only created when monitoring is enabled via configuration property.
     *
     * @return KafkaMetrics instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            name = "kafka.monitoring.enabled",
            havingValue = "true",
            matchIfMissing = false
    )
    public KafkaMetrics kafkaMetrics() {
        return new KafkaMetrics();
    }

    /**
     * Kafka trace interceptor bean.
     * This bean is only created when trace is enabled via configuration property.
     *
     * @return KafkaTraceInterceptor instance
     */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(
            name = "kafka.trace.enabled",
            havingValue = "true",
            matchIfMissing = false
    )
    public KafkaTraceInterceptor kafkaTraceInterceptor() {
        return new KafkaTraceInterceptor();
    }
}

