package com.kafka.util;

import com.kafka.core.model.KafkaHeaders;
import com.kafka.core.model.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Kafka message operations.
 * Provides methods for header management, serialization, and deserialization.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
public class KafkaMessageUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private KafkaMessageUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Build headers map from KafkaMessage.
     *
     * @param message the Kafka message
     * @return map of headers
     */
    public static Map<String, String> buildHeaders(KafkaMessage<?> message) {
        Map<String, String> headers = new HashMap<>();

        if (message == null) {
            return headers;
        }

        // Add standard headers
        if (message.getMsgId() != null) {
            headers.put(KafkaHeaders.MSG_ID, message.getMsgId());
        }
        if (message.getBizKey() != null) {
            headers.put(KafkaHeaders.BIZ_KEY, message.getBizKey());
        }
        if (message.getMsgType() != null) {
            headers.put(KafkaHeaders.MSG_TYPE, message.getMsgType());
        }
        if (message.getSource() != null) {
            headers.put(KafkaHeaders.SOURCE, message.getSource());
        }
        if (message.getVersion() != null) {
            headers.put("version", message.getVersion());
        }
        if (message.getTimestamp() != null) {
            headers.put("timestamp", String.valueOf(message.getTimestamp()));
        }

        // Add extended fields as headers
        if (message.getExtFields() != null) {
            message.getExtFields().forEach((key, value) -> {
                if (value != null) {
                    headers.put(key, value.toString());
                }
            });
        }

        return headers;
    }

    /**
     * Extract headers from ConsumerRecord to a Map.
     *
     * @param record the consumer record
     * @return map of headers
     */
    public static Map<String, String> extractHeaders(ConsumerRecord<String, Object> record) {
        Map<String, String> headers = new HashMap<>();

        if (record == null || record.headers() == null) {
            return headers;
        }

        record.headers().forEach(header -> {
            if (header.value() != null) {
                headers.put(header.key(), new String(header.value()));
            }
        });

        return headers;
    }

    /**
     * Serialize an object to JSON string.
     *
     * @param obj the object to serialize
     * @return JSON string
     * @throws RuntimeException if serialization fails
     */
    public static String serialize(Object obj) {
        if (obj == null) {
            return null;
        }

        try {
            if (obj instanceof String) {
                return (String) obj;
            }
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Failed to serialize object: {}", obj.getClass().getName(), e);
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    /**
     * Deserialize JSON string to an object of the specified class.
     *
     * @param json  the JSON string
     * @param clazz the target class
     * @param <T>   the type of the object
     * @return the deserialized object
     * @throws RuntimeException if deserialization fails
     */
    public static <T> T deserialize(String json, Class<T> clazz) {
        if (json == null || json.isEmpty()) {
            return null;
        }

        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            log.error("Failed to deserialize JSON to class: {}", clazz.getName(), e);
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }

    /**
     * Deserialize JSON string to KafkaMessage.
     *
     * @param json        the JSON string
     * @param dataType    the type of business data
     * @param <T>         the type of business data
     * @return the deserialized KafkaMessage
     * @throws RuntimeException if deserialization fails
     */
    public static <T> KafkaMessage<T> deserializeMessage(String json, Class<T> dataType) {
        if (json == null || json.isEmpty()) {
            return null;
        }

        try {
            return objectMapper.readValue(json,
                    objectMapper.getTypeFactory().constructParametricType(KafkaMessage.class, dataType));
        } catch (Exception e) {
            log.error("Failed to deserialize JSON to KafkaMessage with data type: {}", dataType.getName(), e);
            throw new RuntimeException("Failed to deserialize KafkaMessage", e);
        }
    }
}

