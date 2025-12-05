package com.kafka.core.producer;

import com.kafka.core.exception.MessageSendException;
import com.kafka.core.model.KafkaHeaders;
import com.kafka.core.model.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enhanced Kafka template with automatic message wrapping, header management,
 * and monitoring integration.
 * 
 * <p>This class provides enhanced functionality over the standard KafkaTemplate:
 * <ul>
 *   <li>Automatic message wrapping with KafkaMessage</li>
 *   <li>Automatic header management (traceId, spanId, etc.)</li>
 *   <li>Micrometer metrics integration</li>
 *   <li>Transactional message sending</li>
 *   <li>Batch message sending</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * // Send a single message
 * enhancedKafkaTemplate.send("topic", order, orderId, headers)
 *     .whenComplete((result, ex) -> {
 *         if (ex != null) {
 *             log.error("Send failed", ex);
 *         }
 *     });
 * 
 * // Send transactionally
 * enhancedKafkaTemplate.sendTransactional("topic", order, orderId, () -> {
 *     orderRepository.save(order);
 * });
 * }</pre>
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EnhancedKafkaTemplate {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Autowired(required = false)
    @Nullable
    private MeterRegistry meterRegistry;

    // Cache for Timer builders to improve performance
    private final Map<String, Timer.Builder> timerBuilderCache = new ConcurrentHashMap<>();
    private final Map<String, Counter.Builder> counterBuilderCache = new ConcurrentHashMap<>();

    /**
     * Send a message to Kafka topic asynchronously.
     * 
     * <p>This method automatically:
     * <ul>
     *   <li>Wraps the data in a KafkaMessage</li>
     *   <li>Adds standard headers (msgId, bizKey, traceId, spanId)</li>
     *   <li>Records metrics (if MeterRegistry is available)</li>
     *   <li>Logs the operation</li>
     * </ul>
     *
     * @param topic   the topic name (must not be null or empty)
     * @param data    the business data to send (must not be null)
     * @param bizKey  the business key for partitioning (must not be null or empty)
     * @param headers additional custom headers (can be null)
     * @param <T>     the type of business data
     * @return CompletableFuture with send result
     * @throws MessageSendException if message preparation fails
     * @throws IllegalArgumentException if topic or bizKey is null or empty
     */
    public <T> CompletableFuture<SendResult<String, Object>> send(
            String topic,
            T data,
            String bizKey,
            Map<String, String> headers) {
        
        // Input validation
        if (topic == null || topic.trim().isEmpty()) {
            log.error("Topic cannot be null or empty");
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (data == null) {
            log.error("Data cannot be null for topic: {}", topic);
            throw new IllegalArgumentException("Data cannot be null");
        }
        if (bizKey == null || bizKey.trim().isEmpty()) {
            log.error("BizKey cannot be null or empty for topic: {}", topic);
            throw new IllegalArgumentException("BizKey cannot be null or empty");
        }

        Timer.Sample sample = meterRegistry != null ? Timer.start(meterRegistry) : null;
        long startTime = System.currentTimeMillis();

        try {
            log.debug("Preparing message for topic: {}, bizKey: {}", topic, bizKey);

            // Create KafkaMessage wrapper
            KafkaMessage<T> message = KafkaMessage.create(data);
            message.setBizKey(bizKey);
            if (headers != null && !headers.isEmpty()) {
                message.getExtFields().putAll(headers);
            }

            // Convert to JSON
            String messageValue = objectMapper.writeValueAsString(message);

            // Build message headers - reuse MessageBuilder
            MessageBuilder<String> messageBuilder = MessageBuilder.withPayload(messageValue)
                    .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, topic)
                    .setHeader(KafkaHeaders.MSG_ID, message.getMsgId())
                    .setHeader(KafkaHeaders.BIZ_KEY, bizKey);

            if (message.getMsgType() != null) {
                messageBuilder.setHeader(KafkaHeaders.MSG_TYPE, message.getMsgType());
            }
            if (message.getSource() != null) {
                messageBuilder.setHeader(KafkaHeaders.SOURCE, message.getSource());
            }

            // Get traceId from MDC
            String traceId = MDC.get("traceId");
            if (traceId != null && !traceId.isEmpty()) {
                messageBuilder.setHeader(KafkaHeaders.TRACE_ID, traceId);
            }

            String spanId = MDC.get("spanId");
            if (spanId != null && !spanId.isEmpty()) {
                messageBuilder.setHeader(KafkaHeaders.SPAN_ID, spanId);
            }

            // Add custom headers
            if (headers != null && !headers.isEmpty()) {
                headers.forEach((key, value) -> {
                    if (key != null && value != null) {
                        messageBuilder.setHeader(key, value);
                    }
                });
            }

            Message<String> kafkaMessage = messageBuilder.build();

            log.info("Sending message to topic: {}, msgId: {}, bizKey: {}, traceId: {}",
                    topic, message.getMsgId(), bizKey, traceId);

            // Send message
            ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(kafkaMessage);

            // Convert to CompletableFuture
            CompletableFuture<SendResult<String, Object>> completableFuture = new CompletableFuture<>();

            future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
                @Override
                public void onSuccess(SendResult<String, Object> result) {
                    long duration = System.currentTimeMillis() - startTime;
                    
                    // Record metrics
                    if (sample != null && meterRegistry != null) {
                        recordSuccessMetrics(sample, topic, duration);
                    }

                    log.info("Message sent successfully to topic: {}, msgId: {}, offset: {}, partition: {}, duration: {}ms",
                            topic, message.getMsgId(), 
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition(),
                            duration);
                    completableFuture.complete(result);
                }

                @Override
                public void onFailure(Throwable ex) {
                    long duration = System.currentTimeMillis() - startTime;
                    
                    // Record metrics
                    if (sample != null && meterRegistry != null) {
                        recordFailureMetrics(sample, topic, duration);
                    }

                    log.error("Failed to send message to topic: {}, msgId: {}, bizKey: {}, duration: {}ms",
                            topic, message.getMsgId(), bizKey, duration, ex);
                    completableFuture.completeExceptionally(
                            new MessageSendException("Failed to send message to topic: " + topic, ex));
                }
            });

            return completableFuture;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            
            // Record metrics
            if (sample != null && meterRegistry != null) {
                recordErrorMetrics(sample, topic, duration);
            }

            log.error("Error preparing message for topic: {}, bizKey: {}, duration: {}ms", 
                    topic, bizKey, duration, e);
            throw new MessageSendException("Error preparing message", e);
        }
    }

    /**
     * Send a message transactionally with business logic.
     * 
     * <p>The business logic and message sending are executed within the same transaction.
     * If either fails, both are rolled back.
     * 
     * <p>Note: This requires a KafkaTransactionManager to be configured.
     *
     * @param topic          the topic name (must not be null or empty)
     * @param data           the business data to send (must not be null)
     * @param bizKey         the business key (must not be null or empty)
     * @param businessLogic  the business logic to execute within transaction (can be null)
     * @param <T>            the type of business data
     * @throws MessageSendException if transaction fails
     * @throws IllegalArgumentException if topic, data, or bizKey is invalid
     */
    @Transactional(transactionManager = "kafkaTransactionManager")
    public <T> void sendTransactional(
            String topic,
            T data,
            String bizKey,
            Runnable businessLogic) {
        
        long startTime = System.currentTimeMillis();
        log.info("Starting transactional send to topic: {}, bizKey: {}", topic, bizKey);

        try {
            // Execute business logic first
            if (businessLogic != null) {
                log.debug("Executing business logic for transactional send, topic: {}, bizKey: {}", topic, bizKey);
                businessLogic.run();
            }

            // Send message within transaction
            send(topic, data, bizKey, null).join();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Transactional send completed for topic: {}, bizKey: {}, duration: {}ms", 
                    topic, bizKey, duration);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Transactional send failed for topic: {}, bizKey: {}, duration: {}ms", 
                    topic, bizKey, duration, e);
            throw new MessageSendException("Transactional send failed", e);
        }
    }

    /**
     * Send a batch of messages to Kafka topic.
     * 
     * <p>Messages are sent asynchronously in parallel. The method returns immediately
     * without waiting for all sends to complete. Use the returned CompletableFuture
     * to track completion.
     * 
     * <p>Business keys are automatically generated as: {bizKeyPrefix}-{index}
     *
     * @param topic        the topic name (must not be null or empty)
     * @param dataList     the list of business data (must not be null or empty)
     * @param bizKeyPrefix the prefix for business keys (must not be null or empty)
     * @param <T>          the type of business data
     * @throws IllegalArgumentException if parameters are invalid
     */
    public <T> void sendBatch(
            String topic,
            List<T> dataList,
            String bizKeyPrefix) {
        
        // Input validation
        if (topic == null || topic.trim().isEmpty()) {
            log.error("Topic cannot be null or empty for batch send");
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (dataList == null || dataList.isEmpty()) {
            log.warn("Empty data list for batch send to topic: {}", topic);
            return;
        }
        if (bizKeyPrefix == null || bizKeyPrefix.trim().isEmpty()) {
            log.error("BizKeyPrefix cannot be null or empty for batch send to topic: {}", topic);
            throw new IllegalArgumentException("BizKeyPrefix cannot be null or empty");
        }

        long startTime = System.currentTimeMillis();
        int batchSize = dataList.size();
        
        log.info("Sending batch of {} messages to topic: {}, bizKeyPrefix: {}", 
                batchSize, topic, bizKeyPrefix);

        List<CompletableFuture<SendResult<String, Object>>> futures = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            T data = dataList.get(i);
            if (data == null) {
                log.warn("Skipping null data at index {} in batch for topic: {}", i, topic);
                continue;
            }
            String bizKey = bizKeyPrefix + "-" + i;
            futures.add(send(topic, data, bizKey, null));
        }

        // Wait for all sends to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((result, throwable) -> {
                    long duration = System.currentTimeMillis() - startTime;
                    if (throwable != null) {
                        log.error("Batch send partially failed for topic: {}, bizKeyPrefix: {}, batchSize: {}, duration: {}ms",
                                topic, bizKeyPrefix, batchSize, duration, throwable);
                    } else {
                        log.info("Batch send completed successfully for topic: {}, bizKeyPrefix: {}, batchSize: {}, duration: {}ms",
                                topic, bizKeyPrefix, batchSize, duration);
                    }
                });
    }

    /**
     * Record success metrics.
     * 
     * @param sample the timer sample
     * @param topic  the topic name
     * @param durationMillis the duration in milliseconds
     */
    private void recordSuccessMetrics(Timer.Sample sample, String topic, long durationMillis) {
        try {
            String timerKey = "success-" + topic;
            Timer.Builder timerBuilder = timerBuilderCache.computeIfAbsent(timerKey, 
                    k -> Timer.builder("kafka.message.send.duration")
                            .tag("topic", topic)
                            .tag("status", "success"));
            sample.stop(timerBuilder.register(meterRegistry));

            String counterKey = "success-" + topic;
            Counter.Builder counterBuilder = counterBuilderCache.computeIfAbsent(counterKey,
                    k -> Counter.builder("kafka.message.send")
                            .tag("topic", topic)
                            .tag("status", "success"));
            counterBuilder.register(meterRegistry).increment();
        } catch (Exception e) {
            log.warn("Failed to record success metrics for topic: {}", topic, e);
        }
    }

    /**
     * Record failure metrics.
     * 
     * @param sample the timer sample
     * @param topic  the topic name
     * @param durationMillis the duration in milliseconds
     */
    private void recordFailureMetrics(Timer.Sample sample, String topic, long durationMillis) {
        try {
            String timerKey = "failure-" + topic;
            Timer.Builder timerBuilder = timerBuilderCache.computeIfAbsent(timerKey,
                    k -> Timer.builder("kafka.message.send.duration")
                            .tag("topic", topic)
                            .tag("status", "failure"));
            sample.stop(timerBuilder.register(meterRegistry));

            String counterKey = "failure-" + topic;
            Counter.Builder counterBuilder = counterBuilderCache.computeIfAbsent(counterKey,
                    k -> Counter.builder("kafka.message.send")
                            .tag("topic", topic)
                            .tag("status", "failure"));
            counterBuilder.register(meterRegistry).increment();
        } catch (Exception e) {
            log.warn("Failed to record failure metrics for topic: {}", topic, e);
        }
    }

    /**
     * Record error metrics.
     * 
     * @param sample the timer sample
     * @param topic  the topic name
     * @param durationMillis the duration in milliseconds
     */
    private void recordErrorMetrics(Timer.Sample sample, String topic, long durationMillis) {
        try {
            String timerKey = "error-" + topic;
            Timer.Builder timerBuilder = timerBuilderCache.computeIfAbsent(timerKey,
                    k -> Timer.builder("kafka.message.send.duration")
                            .tag("topic", topic)
                            .tag("status", "error"));
            sample.stop(timerBuilder.register(meterRegistry));

            String counterKey = "error-" + topic;
            Counter.Builder counterBuilder = counterBuilderCache.computeIfAbsent(counterKey,
                    k -> Counter.builder("kafka.message.send")
                            .tag("topic", topic)
                            .tag("status", "error"));
            counterBuilder.register(meterRegistry).increment();
        } catch (Exception e) {
            log.warn("Failed to record error metrics for topic: {}", topic, e);
        }
    }
}
