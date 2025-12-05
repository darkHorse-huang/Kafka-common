package com.kafka.metrics;

import com.kafka.core.model.KafkaHeaders;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.MDC;

import java.util.Map;
import java.util.UUID;

/**
 * Kafka trace interceptor for distributed tracing.
 * Implements both ProducerInterceptor and ConsumerInterceptor to handle trace ID propagation.
 * Integrates with MDC (Mapped Diagnostic Context) for logging correlation.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
public class KafkaTraceInterceptor implements ProducerInterceptor<String, Object>, ConsumerInterceptor<String, Object> {

    private static final String MDC_TRACE_ID_KEY = "traceId";
    private static final String MDC_SPAN_ID_KEY = "spanId";

    /**
     * Producer interceptor: inject trace ID into message headers before sending.
     *
     * @param record the producer record
     * @return the record with trace headers added
     */
    @Override
    public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
        try {
            // Get trace ID from MDC or generate a new one
            String traceId = MDC.get(MDC_TRACE_ID_KEY);
            if (traceId == null || traceId.isEmpty()) {
                traceId = UUID.randomUUID().toString();
                MDC.put(MDC_TRACE_ID_KEY, traceId);
                log.debug("Generated new traceId for producer: {}", traceId);
            }

            // Generate span ID
            String spanId = UUID.randomUUID().toString();
            MDC.put(MDC_SPAN_ID_KEY, spanId);

            // Add trace headers to the record
            record.headers().add(KafkaHeaders.TRACE_ID, traceId.getBytes());
            record.headers().add(KafkaHeaders.SPAN_ID, spanId.getBytes());

            log.debug("Injected trace headers into producer record: topic={}, traceId={}, spanId={}",
                    record.topic(), traceId, spanId);
        } catch (Exception e) {
            log.warn("Failed to inject trace headers into producer record", e);
        }

        return record;
    }

    /**
     * Producer interceptor: called after record is acknowledged.
     *
     * @param record         the producer record
     * @param recordMetadata the record metadata
     * @param exception     the exception if any
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception exception) {
        // Clean up MDC after sending if needed
        // Note: We might want to keep the trace ID for correlation
        if (exception != null) {
            log.error("Producer record failed: topic={}, partition={}, offset={}",
                    recordMetadata != null ? recordMetadata.topic() : "unknown",
                    recordMetadata != null ? recordMetadata.partition() : -1,
                    recordMetadata != null ? recordMetadata.offset() : -1,
                    exception);
        }
    }

    /**
     * Consumer interceptor: extract trace ID from message headers and inject into MDC.
     *
     * @param records the consumer records
     * @return the records (unchanged)
     */
    @Override
    public ConsumerRecords<String, Object> onConsume(ConsumerRecords<String, Object> records) {
        if (records == null || records.isEmpty()) {
            return records;
        }

        try {
            // Process each record to extract trace information
            for (ConsumerRecord<String, Object> record : records) {
                String traceId = extractTraceId(record);
                String spanId = extractSpanId(record);

                if (traceId != null && !traceId.isEmpty()) {
                    // Set trace context in MDC for this consumer thread
                    MDC.put(MDC_TRACE_ID_KEY, traceId);
                    if (spanId != null && !spanId.isEmpty()) {
                        MDC.put(MDC_SPAN_ID_KEY, spanId);
                    } else {
                        // Generate new span ID if not present
                        String newSpanId = UUID.randomUUID().toString();
                        MDC.put(MDC_SPAN_ID_KEY, newSpanId);
                        log.debug("Generated new spanId for consumer: traceId={}, spanId={}", traceId, newSpanId);
                    }

                    log.debug("Extracted trace headers from consumer record: topic={}, partition={}, offset={}, traceId={}, spanId={}",
                            record.topic(), record.partition(), record.offset(), traceId, MDC.get(MDC_SPAN_ID_KEY));
                } else {
                    // No trace ID in message, generate a new one
                    String newTraceId = UUID.randomUUID().toString();
                    String newSpanId = UUID.randomUUID().toString();
                    MDC.put(MDC_TRACE_ID_KEY, newTraceId);
                    MDC.put(MDC_SPAN_ID_KEY, newSpanId);
                    log.debug("No trace ID in message, generated new trace context: traceId={}, spanId={}",
                            newTraceId, newSpanId);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to extract trace headers from consumer records", e);
        }

        return records;
    }

    /**
     * Consumer interceptor: called when offsets are committed.
     *
     * @param offsets the offsets being committed
     */
    @Override
    public void onCommit(Map<org.apache.kafka.common.TopicPartition, OffsetAndMetadata> offsets) {
        // Optionally log trace information during commit
        if (log.isDebugEnabled()) {
            String traceId = MDC.get(MDC_TRACE_ID_KEY);
            log.debug("Committing offsets with trace context: traceId={}, offsets={}", traceId, offsets);
        }
    }

    /**
     * Extract trace ID from consumer record headers.
     *
     * @param record the consumer record
     * @return the trace ID, or null if not found
     */
    private String extractTraceId(ConsumerRecord<String, Object> record) {
        if (record.headers() == null) {
            return null;
        }

        org.apache.kafka.common.header.Header header = record.headers().lastHeader(KafkaHeaders.TRACE_ID);
        if (header != null && header.value() != null) {
            return new String(header.value());
        }

        return null;
    }

    /**
     * Extract span ID from consumer record headers.
     *
     * @param record the consumer record
     * @return the span ID, or null if not found
     */
    private String extractSpanId(ConsumerRecord<String, Object> record) {
        if (record.headers() == null) {
            return null;
        }

        org.apache.kafka.common.header.Header header = record.headers().lastHeader(KafkaHeaders.SPAN_ID);
        if (header != null && header.value() != null) {
            return new String(header.value());
        }

        return null;
    }

    /**
     * Configure the interceptor.
     *
     * @param configs the configuration map
     */
    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Configuring KafkaTraceInterceptor");
    }

    /**
     * Close the interceptor.
     */
    @Override
    public void close() {
        // Clean up MDC
        MDC.clear();
        log.debug("KafkaTraceInterceptor closed");
    }
}

