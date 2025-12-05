package com.kafka.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for marking Kafka consumer methods.
 * This annotation is used to configure Kafka message consumption behavior.
 *
 * @author m.huang
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaConsumer {

    /**
     * The topic name to consume from.
     * This is a required field.
     *
     * @return the topic name
     */
    String topic();

    /**
     * The consumer group ID.
     * If not specified, the default group ID from configuration will be used.
     *
     * @return the consumer group ID, empty string if not specified
     */
    String groupId() default "";

    /**
     * The number of concurrent consumer threads.
     * Default is 1.
     *
     * @return the concurrency level
     */
    int concurrency() default 1;

    /**
     * Whether to enable batch consumption.
     * Default is false (single message consumption).
     *
     * @return true if batch consumption is enabled, false otherwise
     */
    boolean batch() default false;

    /**
     * The batch size for batch consumption.
     * Only effective when batch() is true.
     * Default is 10.
     *
     * @return the batch size
     */
    int batchSize() default 10;

    /**
     * The acknowledgment mode for message processing.
     * Options: "MANUAL_IMMEDIATE", "MANUAL", "BATCH", "RECORD", "TIME", "COUNT", "COUNT_TIME"
     * Default is "MANUAL_IMMEDIATE".
     *
     * @return the acknowledgment mode
     */
    String ackMode() default "MANUAL_IMMEDIATE";
}

