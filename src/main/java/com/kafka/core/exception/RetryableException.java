package com.kafka.core.exception;

/**
 * Exception that indicates the operation can be retried.
 *
 * @author m.huang
 * @since 1.0.0
 */
public class RetryableException extends KafkaCommonException {

    private static final long serialVersionUID = 1L;

    public RetryableException() {
        super();
    }

    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetryableException(Throwable cause) {
        super(cause);
    }
}

