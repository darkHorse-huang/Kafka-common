package com.kafka.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka metrics collection and reporting using Micrometer.
 * Provides comprehensive metrics for Kafka message production and consumption.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
public class KafkaMetrics {

    @Autowired(required = false)
    @Nullable
    private MeterRegistry meterRegistry;

    // Queue size tracking
    private final ConcurrentHashMap<String, AtomicLong> queueSizes = new ConcurrentHashMap<>();

    /**
     * Record message send metric (success or failure).
     *
     * @param topic   the topic name
     * @param success whether the send was successful
     */
    public void recordMessageSend(String topic, boolean success) {
        if (meterRegistry != null) {
            Counter.builder("kafka.message.send")
                    .tag("topic", topic)
                    .tag("status", success ? "success" : "failure")
                    .register(meterRegistry)
                    .increment();
        }
    }

    /**
     * Record message consume metric (success, failure, or retry).
     *
     * @param topic   the topic name
     * @param status  the consumption status (success, failure, retry)
     */
    public void recordMessageConsume(String topic, String status) {
        if (meterRegistry != null) {
            Counter.builder("kafka.message.consume")
                    .tag("topic", topic)
                    .tag("status", status)
                    .register(meterRegistry)
                    .increment();
        }
    }

    /**
     * Record message consume metric with boolean success flag.
     *
     * @param topic   the topic name
     * @param success whether the consumption was successful
     */
    public void recordMessageConsume(String topic, boolean success) {
        recordMessageConsume(topic, success ? "success" : "failure");
    }

    /**
     * Record message retry metric.
     *
     * @param topic the topic name
     */
    public void recordMessageRetry(String topic) {
        recordMessageConsume(topic, "retry");
    }

    /**
     * Record message processing time.
     *
     * @param topic          the topic name
     * @param durationMillis the processing duration in milliseconds
     */
    public void recordProcessingTime(String topic, long durationMillis) {
        if (meterRegistry != null) {
            Timer.builder("kafka.message.processing.time")
                    .tag("topic", topic)
                    .register(meterRegistry)
                    .record(durationMillis, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Record message processing time using Timer.Sample.
     *
     * @param topic the topic name
     * @return Timer.Sample for timing measurement
     */
    public Timer.Sample startProcessingTimer(String topic) {
        if (meterRegistry != null) {
            return Timer.start(meterRegistry);
        }
        return null;
    }

    /**
     * Stop processing timer and record the duration.
     *
     * @param sample the timer sample
     * @param topic  the topic name
     */
    public void stopProcessingTimer(Timer.Sample sample, String topic) {
        if (sample != null && meterRegistry != null) {
            sample.stop(Timer.builder("kafka.message.processing.time")
                    .tag("topic", topic)
                    .register(meterRegistry));
        }
    }

    /**
     * Update queue size for a topic.
     *
     * @param topic the topic name
     * @param size  the queue size
     */
    public void updateQueueSize(String topic, long size) {
        queueSizes.computeIfAbsent(topic, k -> new AtomicLong(0)).set(size);
        
        if (meterRegistry != null) {
            Gauge.builder("kafka.queue.size", () -> queueSizes.get(topic))
                    .tag("topic", topic)
                    .register(meterRegistry);
        }
    }

    /**
     * Increment queue size for a topic.
     *
     * @param topic the topic name
     */
    public void incrementQueueSize(String topic) {
        queueSizes.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
    }

    /**
     * Decrement queue size for a topic.
     *
     * @param topic the topic name
     */
    public void decrementQueueSize(String topic) {
        AtomicLong size = queueSizes.get(topic);
        if (size != null) {
            size.decrementAndGet();
        }
    }

    /**
     * Get current queue size for a topic.
     *
     * @param topic the topic name
     * @return the queue size
     */
    public long getQueueSize(String topic) {
        AtomicLong size = queueSizes.get(topic);
        return size != null ? size.get() : 0;
    }
}
