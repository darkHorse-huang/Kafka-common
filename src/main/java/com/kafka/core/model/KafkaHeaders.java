package com.kafka.core.model;

/**
 * Kafka message headers constants.
 *
 * @author m.huang
 * @since 1.0.0
 */
public final class KafkaHeaders {

    private KafkaHeaders() {
        // Utility class, prevent instantiation
    }

    /**
     * Message ID header key
     */
    public static final String MSG_ID = "msgId";

    /**
     * Business key header key
     */
    public static final String BIZ_KEY = "bizKey";

    /**
     * Message type header key
     */
    public static final String MSG_TYPE = "msgType";

    /**
     * Source header key
     */
    public static final String SOURCE = "source";

    /**
     * Retry count header key
     */
    public static final String RETRY_COUNT = "retryCount";

    /**
     * Trace ID header key
     */
    public static final String TRACE_ID = "traceId";

    /**
     * Span ID header key
     */
    public static final String SPAN_ID = "spanId";
}

