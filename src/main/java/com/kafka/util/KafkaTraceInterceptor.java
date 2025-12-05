package com.kafka.util;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.UUID;

/**
 * Kafka trace interceptor for distributed tracing.
 * Manages trace ID and span ID in MDC context.
 */
@Slf4j
public class KafkaTraceInterceptor {

    private static final String TRACE_ID_KEY = "traceId";
    private static final String SPAN_ID_KEY = "spanId";

    /**
     * Start a new trace context.
     * Generates a new trace ID and span ID.
     *
     * @return the trace ID
     */
    public String startTrace() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        
        MDC.put(TRACE_ID_KEY, traceId);
        MDC.put(SPAN_ID_KEY, spanId);
        
        log.debug("Started trace: traceId={}, spanId={}", traceId, spanId);
        return traceId;
    }

    /**
     * Continue an existing trace context.
     * Uses the provided trace ID and generates a new span ID.
     *
     * @param traceId the trace ID to continue
     * @return the new span ID
     */
    public String continueTrace(String traceId) {
        String spanId = UUID.randomUUID().toString();
        
        MDC.put(TRACE_ID_KEY, traceId);
        MDC.put(SPAN_ID_KEY, spanId);
        
        log.debug("Continued trace: traceId={}, spanId={}", traceId, spanId);
        return spanId;
    }

    /**
     * Get the current trace ID from MDC.
     *
     * @return the trace ID, or null if not set
     */
    public String getTraceId() {
        return MDC.get(TRACE_ID_KEY);
    }

    /**
     * Get the current span ID from MDC.
     *
     * @return the span ID, or null if not set
     */
    public String getSpanId() {
        return MDC.get(SPAN_ID_KEY);
    }

    /**
     * Clear the trace context.
     */
    public void clearTrace() {
        MDC.remove(TRACE_ID_KEY);
        MDC.remove(SPAN_ID_KEY);
        log.debug("Cleared trace context");
    }

    /**
     * Set trace context from external source.
     *
     * @param traceId the trace ID
     * @param spanId  the span ID
     */
    public void setTrace(String traceId, String spanId) {
        if (traceId != null) {
            MDC.put(TRACE_ID_KEY, traceId);
        }
        if (spanId != null) {
            MDC.put(SPAN_ID_KEY, spanId);
        }
        log.debug("Set trace context: traceId={}, spanId={}", traceId, spanId);
    }
}

