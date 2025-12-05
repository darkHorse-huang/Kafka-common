package com.kafka.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Kafka message model with generic data type.
 * 
 * <p>This class represents a standardized message format for Kafka messages.
 * It includes metadata fields and a generic data payload.
 * 
 * <p>Standard fields:
 * <ul>
 *   <li>msgId: Unique message identifier (auto-generated if not provided)</li>
 *   <li>bizKey: Business key for partitioning and routing</li>
 *   <li>msgType: Message type identifier</li>
 *   <li>source: Source system identifier</li>
 *   <li>version: Message format version</li>
 *   <li>timestamp: Message creation timestamp</li>
 *   <li>extFields: Extended fields for custom metadata</li>
 *   <li>data: Business data payload (generic type)</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * // Create message with factory method
 * KafkaMessage<Order> message = KafkaMessage.create(order);
 * message.setBizKey(orderId);
 * message.setMsgType("ORDER_CREATED");
 * 
 * // Or use builder
 * KafkaMessage<Order> message = KafkaMessage.<Order>builder()
 *     .msgId(UUID.randomUUID().toString())
 *     .bizKey(orderId)
 *     .msgType("ORDER_CREATED")
 *     .source("order-service")
 *     .version("1.0")
 *     .timestamp(System.currentTimeMillis())
 *     .data(order)
 *     .build();
 * }</pre>
 *
 * @param <T> the type of business data
 * @author m.huang
 * @since 1.0.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessage<T> implements Serializable {

    /**
     * Serial version UID for serialization compatibility.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Message ID - unique identifier for the message.
     * Auto-generated if not provided.
     */
    private String msgId;

    /**
     * Business key - used for partitioning and business logic routing.
     */
    private String bizKey;

    /**
     * Message type - identifies the type of message (e.g., "ORDER_CREATED", "PAYMENT_PROCESSED").
     */
    private String msgType;

    /**
     * Source - identifies the system that created the message.
     */
    private String source;

    /**
     * Version - message format version for compatibility.
     */
    private String version;

    /**
     * Timestamp - message creation timestamp in milliseconds.
     */
    private Long timestamp;

    /**
     * Extended fields - map for custom metadata and additional information.
     * Defaults to empty map.
     */
    @Builder.Default
    private Map<String, Object> extFields = new HashMap<>();

    /**
     * Business data - the actual payload data (generic type).
     */
    private T data;

    /**
     * Static factory method to create a KafkaMessage with data.
     * 
     * <p>This method automatically:
     * <ul>
     *   <li>Generates a unique msgId (UUID)</li>
     *   <li>Sets the current timestamp</li>
     *   <li>Initializes extFields to an empty map</li>
     * </ul>
     * 
     * <p>Other fields (bizKey, msgType, source, version) should be set separately if needed.
     *
     * @param data the business data (must not be null)
     * @param <T>  the type of business data
     * @return a new KafkaMessage instance with auto-generated msgId and timestamp
     * @throws IllegalArgumentException if data is null
     */
    public static <T> KafkaMessage<T> create(T data) {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        return KafkaMessage.<T>builder()
                .msgId(UUID.randomUUID().toString())
                .timestamp(System.currentTimeMillis())
                .data(data)
                .extFields(new HashMap<>())
                .build();
    }
}
