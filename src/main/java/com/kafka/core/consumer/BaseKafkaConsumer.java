package com.kafka.core.consumer;

import com.kafka.core.exception.MessageParseException;
import com.kafka.core.exception.RetryableException;
import com.kafka.core.model.KafkaHeaders;
import com.kafka.core.model.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Base Kafka consumer with template method pattern.
 * 
 * <p>This abstract class provides a complete framework for Kafka message consumption with:
 * <ul>
 *   <li>Automatic message parsing</li>
 *   <li>Retry logic with configurable max retry count</li>
 *   <li>Dead letter queue (DLT) handling</li>
 *   <li>Exception handling and classification</li>
 *   <li>Header extraction and management</li>
 * </ul>
 * 
 * <p>Subclasses only need to implement the {@link #doProcess(Object, Map)} method
 * to define the business logic. All other functionality is handled automatically.
 * 
 * <p>Example usage:
 * <pre>{@code
 * @Component
 * public class OrderConsumer extends BaseKafkaConsumer<Order> {
 *     
 *     public OrderConsumer(ObjectMapper objectMapper, 
 *                          KafkaTemplate<String, Object> kafkaTemplate,
 *                          int maxRetryCount) {
 *         super(objectMapper, kafkaTemplate, maxRetryCount);
 *     }
 * 
 *     @Override
 *     protected void doProcess(Order data, Map<String, String> headers) throws Exception {
 *         // Implement business logic
 *         processOrder(data);
 *     }
 * 
 *     @Override
 *     protected Class<Order> getDataClass() {
 *         return Order.class;
 *     }
 * }
 * }</pre>
 * 
 * <p>Exception handling:
 * <ul>
 *   <li>Throw {@link RetryableException} for temporary errors (will trigger retry)</li>
 *   <li>Throw other exceptions for permanent errors (will send to DLT)</li>
 * </ul>
 *
 * @param <T> the type of business data
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
public abstract class BaseKafkaConsumer<T> implements AcknowledgingMessageListener<String, Object> {

    /**
     * ObjectMapper for JSON serialization/deserialization.
     */
    protected final ObjectMapper objectMapper;
    
    /**
     * KafkaTemplate for sending messages to retry/DLT topics.
     */
    protected final KafkaTemplate<String, Object> kafkaTemplate;
    
    /**
     * Maximum number of retry attempts.
     */
    protected final int maxRetryCount;

    /**
     * Cached TypeFactory for performance optimization.
     */
    private final TypeFactory typeFactory;

    /**
     * Constructor for BaseKafkaConsumer.
     *
     * @param objectMapper    the object mapper for JSON operations (must not be null)
     * @param kafkaTemplate   the kafka template for DLT/retry (must not be null)
     * @param maxRetryCount   maximum number of retry attempts (must be >= 0)
     * @throws IllegalArgumentException if objectMapper or kafkaTemplate is null, or maxRetryCount < 0
     */
    protected BaseKafkaConsumer(
            ObjectMapper objectMapper,
            KafkaTemplate<String, Object> kafkaTemplate,
            int maxRetryCount) {
        if (objectMapper == null) {
            throw new IllegalArgumentException("ObjectMapper cannot be null");
        }
        if (kafkaTemplate == null) {
            throw new IllegalArgumentException("KafkaTemplate cannot be null");
        }
        if (maxRetryCount < 0) {
            throw new IllegalArgumentException("MaxRetryCount must be >= 0");
        }
        
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.maxRetryCount = maxRetryCount;
        this.typeFactory = objectMapper.getTypeFactory();
        
        log.debug("BaseKafkaConsumer initialized with maxRetryCount: {}", maxRetryCount);
    }

    /**
     * Interface method that delegates to the template method.
     * This method is called by Spring Kafka when a message is received.
     *
     * @param record the consumer record (must not be null)
     * @param ack    the acknowledgment (can be null for auto-commit mode)
     */
    @Override
    public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        if (record == null) {
            log.error("Received null consumer record");
            return;
        }
        process(record, ack);
    }

    /**
     * Template method for processing messages.
     * 
     * <p>This is the main template method that defines the processing flow:
     * <ol>
     *   <li>Extract message metadata (msgId, bizKey, traceId)</li>
     *   <li>Parse the message</li>
     *   <li>Check retry count</li>
     *   <li>Execute business logic (via doProcess)</li>
     *   <li>Handle exceptions (retry or DLT)</li>
     *   <li>Acknowledge the message</li>
     * </ol>
     *
     * @param record the consumer record (must not be null)
     * @param ack    the acknowledgment (can be null)
     */
    protected void process(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        long startTime = System.currentTimeMillis();
        String msgId = getHeader(record, KafkaHeaders.MSG_ID);
        String bizKey = getHeader(record, KafkaHeaders.BIZ_KEY);
        String traceId = getHeader(record, KafkaHeaders.TRACE_ID);

        log.info("Received message from topic: {}, partition: {}, offset: {}, msgId: {}, bizKey: {}, traceId: {}",
                record.topic(), record.partition(), record.offset(), msgId, bizKey, traceId);

        try {
            // Parse message
            KafkaMessage<T> message = parseMessage(record);
            T data = message.getData();
            Map<String, String> headers = extractHeaders(record);

            // Get retry count
            int retryCount = getRetryCount(record);
            log.debug("Processing message, msgId: {}, bizKey: {}, retryCount: {}/{}", 
                    msgId, bizKey, retryCount, maxRetryCount);

            // Process message
            if (retryCount < maxRetryCount) {
                try {
                    doProcess(data, headers);
                    long duration = System.currentTimeMillis() - startTime;
                    log.info("Message processed successfully, msgId: {}, bizKey: {}, duration: {}ms", 
                            msgId, bizKey, duration);
                    if (ack != null) {
                        ack.acknowledge();
                    }
                } catch (RetryableException e) {
                    long duration = System.currentTimeMillis() - startTime;
                    log.warn("Retryable exception occurred, msgId: {}, bizKey: {}, retryCount: {}/{}, duration: {}ms",
                            msgId, bizKey, retryCount, maxRetryCount, duration, e);
                    handleRetry(record, e, retryCount);
                    if (ack != null) {
                        ack.acknowledge();
                    }
                } catch (Exception e) {
                    long duration = System.currentTimeMillis() - startTime;
                    log.error("Non-retryable exception occurred, msgId: {}, bizKey: {}, duration: {}ms", 
                            msgId, bizKey, duration, e);
                    handleDlt(record, e, "PROCESSING_ERROR");
                    if (ack != null) {
                        ack.acknowledge();
                    }
                }
            } else {
                long duration = System.currentTimeMillis() - startTime;
                log.error("Max retry count exceeded, msgId: {}, bizKey: {}, retryCount: {}, duration: {}ms", 
                        msgId, bizKey, retryCount, duration);
                handleDlt(record, new RuntimeException("Max retry count exceeded: " + retryCount), "MAX_RETRY_EXCEEDED");
                if (ack != null) {
                    ack.acknowledge();
                }
            }
        } catch (MessageParseException e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Failed to parse message, msgId: {}, bizKey: {}, duration: {}ms", 
                    msgId, bizKey, duration, e);
            handleDlt(record, e, "PARSE_ERROR");
            if (ack != null) {
                ack.acknowledge();
            }
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            log.error("Unexpected error processing message, msgId: {}, bizKey: {}, duration: {}ms", 
                    msgId, bizKey, duration, e);
            handleDlt(record, e, "UNEXPECTED_ERROR");
            if (ack != null) {
                ack.acknowledge();
            }
        }
    }

    /**
     * Abstract method to be implemented by subclasses for actual business logic processing.
     * 
     * <p>This method is called after the message has been successfully parsed.
     * Subclasses should implement their business logic here.
     * 
     * <p>Exception handling:
     * <ul>
     *   <li>Throw {@link RetryableException} for temporary errors that should be retried</li>
     *   <li>Throw other exceptions for permanent errors that should go to DLT</li>
     * </ul>
     *
     * @param data    the business data extracted from the message (must not be null)
     * @param headers the message headers (never null, may be empty)
     * @throws Exception if processing fails
     */
    protected abstract void doProcess(T data, Map<String, String> headers) throws Exception;

    /**
     * Parse message from consumer record.
     * 
     * <p>This method handles both String and Object value types.
     * For String values, it uses the string directly.
     * For Object values, it serializes to JSON first, then deserializes.
     *
     * @param record the consumer record (must not be null)
     * @return parsed KafkaMessage (never null)
     * @throws MessageParseException if parsing fails
     * @throws IllegalArgumentException if record is null
     */
    protected KafkaMessage<T> parseMessage(ConsumerRecord<String, Object> record) throws MessageParseException {
        if (record == null) {
            throw new IllegalArgumentException("ConsumerRecord cannot be null");
        }

        try {
            Object value = record.value();
            if (value == null) {
                log.error("Message value is null, topic: {}, partition: {}, offset: {}", 
                        record.topic(), record.partition(), record.offset());
                throw new MessageParseException("Message value is null");
            }

            String jsonValue;
            if (value instanceof String) {
                jsonValue = (String) value;
                log.debug("Message value is String, length: {}", jsonValue.length());
            } else {
                log.debug("Message value is Object, serializing to JSON, type: {}", value.getClass().getName());
                jsonValue = objectMapper.writeValueAsString(value);
            }

            KafkaMessage<T> message = objectMapper.readValue(jsonValue,
                    typeFactory.constructParametricType(KafkaMessage.class, getDataClass()));
            
            log.debug("Successfully parsed message, msgId: {}, dataType: {}", 
                    message.getMsgId(), getDataClass().getName());
            return message;
        } catch (MessageParseException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to parse message, topic: {}, partition: {}, offset: {}", 
                    record.topic(), record.partition(), record.offset(), e);
            throw new MessageParseException("Failed to parse message: " + e.getMessage(), e);
        }
    }

    /**
     * Get the class type of business data.
     * 
     * <p>This method should be overridden by subclasses to return the actual data type.
     * The default implementation returns Object.class, which may not work correctly
     * for generic deserialization.
     *
     * @return the class type (never null)
     */
    @SuppressWarnings("unchecked")
    protected Class<T> getDataClass() {
        return (Class<T>) Object.class;
    }

    /**
     * Get header value from consumer record.
     * 
     * <p>This method extracts a single header value from the consumer record.
     * Header values are converted from byte arrays to strings using UTF-8 encoding.
     *
     * @param record the consumer record (must not be null)
     * @param key    the header key (must not be null)
     * @return the header value, or null if not found
     * @throws IllegalArgumentException if record or key is null
     */
    protected String getHeader(ConsumerRecord<String, Object> record, String key) {
        if (record == null) {
            throw new IllegalArgumentException("ConsumerRecord cannot be null");
        }
        if (key == null) {
            throw new IllegalArgumentException("Header key cannot be null");
        }

        if (record.headers() == null) {
            log.debug("Record headers is null, key: {}", key);
            return null;
        }

        org.apache.kafka.common.header.Header header = record.headers().lastHeader(key);
        if (header == null || header.value() == null) {
            log.debug("Header not found, key: {}", key);
            return null;
        }

        try {
            return new String(header.value(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Failed to convert header value to string, key: {}", key, e);
            return new String(header.value());
        }
    }

    /**
     * Extract all headers from consumer record.
     * 
     * <p>This method converts all headers from the consumer record into a Map.
     * Header values are converted from byte arrays to strings using UTF-8 encoding.
     *
     * @param record the consumer record (must not be null)
     * @return map of headers (never null, may be empty)
     * @throws IllegalArgumentException if record is null
     */
    protected Map<String, String> extractHeaders(ConsumerRecord<String, Object> record) {
        if (record == null) {
            throw new IllegalArgumentException("ConsumerRecord cannot be null");
        }

        Map<String, String> headers = new HashMap<>();
        if (record.headers() == null) {
            log.debug("Record headers is null, returning empty map");
            return headers;
        }

        record.headers().forEach(header -> {
            if (header != null && header.value() != null) {
                try {
                    headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                } catch (Exception e) {
                    log.warn("Failed to convert header value to string, key: {}", header.key(), e);
                    headers.put(header.key(), new String(header.value()));
                }
            }
        });

        log.debug("Extracted {} headers from record", headers.size());
        return headers;
    }

    /**
     * Get retry count from headers.
     * 
     * <p>This method extracts the retry count from the message headers.
     * If the header is not present or cannot be parsed, it returns 0.
     *
     * @param record the consumer record (must not be null)
     * @return retry count (0 if not found or invalid)
     */
    protected int getRetryCount(ConsumerRecord<String, Object> record) {
        String retryCountStr = getHeader(record, KafkaHeaders.RETRY_COUNT);
        if (retryCountStr == null || retryCountStr.trim().isEmpty()) {
            return 0;
        }
        try {
            int count = Integer.parseInt(retryCountStr);
            return Math.max(0, count); // Ensure non-negative
        } catch (NumberFormatException e) {
            log.warn("Invalid retry count format: {}, defaulting to 0", retryCountStr);
            return 0;
        }
    }

    /**
     * Handle retry logic by sending message to retry topic.
     * 
     * <p>This method sends the failed message to a retry topic with an incremented retry count.
     * The retry topic name is: {originalTopic}-retry
     *
     * @param record     the consumer record (must not be null)
     * @param exception  the exception that occurred (must not be null)
     * @param retryCount the current retry count (must be >= 0)
     */
    protected void handleRetry(ConsumerRecord<String, Object> record, Exception exception, int retryCount) {
        String retryTopic = record.topic() + "-retry";
        int newRetryCount = retryCount + 1;
        String msgId = getHeader(record, KafkaHeaders.MSG_ID);

        log.info("Sending message to retry topic: {}, msgId: {}, retryCount: {}/{}",
                retryTopic, msgId, newRetryCount, maxRetryCount);

        try {
            Message<Object> retryMessage = MessageBuilder.withPayload(record.value())
                    .copyHeaders(record.headers())
                    .setHeader(KafkaHeaders.RETRY_COUNT, String.valueOf(newRetryCount))
                    .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, retryTopic)
                    .build();

            kafkaTemplate.send(retryMessage);
            log.info("Successfully sent message to retry topic: {}, msgId: {}, retryCount: {}",
                    retryTopic, msgId, newRetryCount);
        } catch (Exception e) {
            log.error("Failed to send message to retry topic: {}, msgId: {}, retryCount: {}",
                    retryTopic, msgId, newRetryCount, e);
        }
    }

    /**
     * Handle dead letter queue by sending message to DLT topic.
     * 
     * <p>This method sends the failed message to a dead letter topic (DLT) with
     * comprehensive error information. The DLT topic name is: {originalTopic}-dlt
     * 
     * <p>The DLT payload includes:
     * <ul>
     *   <li>Original topic, partition, offset</li>
     *   <li>Original message value</li>
     *   <li>Error code and message</li>
     *   <li>Exception class name</li>
     *   <li>Timestamp</li>
     * </ul>
     *
     * @param record     the consumer record (must not be null)
     * @param exception  the exception that occurred (must not be null)
     * @param errorCode  the error code (must not be null)
     */
    protected void handleDlt(ConsumerRecord<String, Object> record, Exception exception, String errorCode) {
        String dltTopic = record.topic() + "-dlt";
        String msgId = getHeader(record, KafkaHeaders.MSG_ID);

        log.error("Sending message to DLT topic: {}, msgId: {}, errorCode: {}, exception: {}",
                dltTopic, msgId, errorCode, exception.getClass().getSimpleName(), exception);

        try {
            Map<String, Object> dltPayload = new HashMap<>();
            dltPayload.put("originalTopic", record.topic());
            dltPayload.put("originalPartition", record.partition());
            dltPayload.put("originalOffset", record.offset());
            dltPayload.put("originalKey", record.key());
            dltPayload.put("originalValue", record.value());
            dltPayload.put("errorCode", errorCode);
            dltPayload.put("errorMessage", exception.getMessage());
            dltPayload.put("exceptionClass", exception.getClass().getName());
            dltPayload.put("stackTrace", getStackTrace(exception));
            dltPayload.put("timestamp", System.currentTimeMillis());
            dltPayload.put("msgId", msgId);
            dltPayload.put("bizKey", getHeader(record, KafkaHeaders.BIZ_KEY));

            Message<Map<String, Object>> dltMessage = MessageBuilder.withPayload(dltPayload)
                    .copyHeaders(record.headers())
                    .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, dltTopic)
                    .setHeader("errorCode", errorCode)
                    .build();

            kafkaTemplate.send(dltMessage);
            log.info("Successfully sent message to DLT topic: {}, msgId: {}, errorCode: {}",
                    dltTopic, msgId, errorCode);
        } catch (Exception e) {
            log.error("Failed to send message to DLT topic: {}, msgId: {}, errorCode: {}",
                    dltTopic, msgId, errorCode, e);
        }
    }

    /**
     * Get stack trace as string.
     * 
     * @param exception the exception
     * @return stack trace string
     */
    private String getStackTrace(Exception exception) {
        if (exception == null) {
            return null;
        }
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        exception.printStackTrace(pw);
        return sw.toString();
    }
}
