package com.kafka.core.exception;

/**
 * Base exception for Kafka common operations.
 *
 * @author m.huang
 * @since 1.0.0
 */
public class KafkaCommonException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public KafkaCommonException() {
        super();
    }

    public KafkaCommonException(String message) {
        super(message);
    }

    public KafkaCommonException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaCommonException(Throwable cause) {
        super(cause);
    }
}

