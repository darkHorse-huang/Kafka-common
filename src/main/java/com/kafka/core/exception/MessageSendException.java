package com.kafka.core.exception;

/**
 * Exception thrown when message sending fails.
 *
 * @author m.huang
 * @since 1.0.0
 */
public class MessageSendException extends KafkaCommonException {

    private static final long serialVersionUID = 1L;

    public MessageSendException() {
        super();
    }

    public MessageSendException(String message) {
        super(message);
    }

    public MessageSendException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessageSendException(Throwable cause) {
        super(cause);
    }
}

