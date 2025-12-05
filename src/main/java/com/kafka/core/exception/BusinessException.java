package com.kafka.core.exception;

/**
 * Exception thrown for business logic errors.
 *
 * @author m.huang
 * @since 1.0.0
 */
public class BusinessException extends KafkaCommonException {

    private static final long serialVersionUID = 1L;

    public BusinessException() {
        super();
    }

    public BusinessException(String message) {
        super(message);
    }

    public BusinessException(String message, Throwable cause) {
        super(message, cause);
    }

    public BusinessException(Throwable cause) {
        super(cause);
    }
}

