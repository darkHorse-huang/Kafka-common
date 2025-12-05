package com.kafka.core.exception;

/**
 * Exception thrown when message parsing fails.
 *
 * @author m.huang
 * @since 1.0.0
 */
public class MessageParseException extends KafkaCommonException {

    private static final long serialVersionUID = 1L;

    public MessageParseException() {
        super();
    }

    public MessageParseException(String message) {
        super(message);
    }

    public MessageParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessageParseException(Throwable cause) {
        super(cause);
    }
}

