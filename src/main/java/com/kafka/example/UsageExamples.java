package com.kafka.example;

import com.kafka.core.exception.RetryableException;
import com.kafka.core.producer.EnhancedKafkaTemplate;
import com.kafka.core.consumer.BaseKafkaConsumer;
import com.kafka.util.KafkaMessageUtils;
import com.kafka.util.RetryTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Complete usage examples for kafka-common library.
 * 
 * <p>This class demonstrates:
 * <ul>
 *   <li>How to use EnhancedKafkaTemplate to send messages</li>
 *   <li>How to extend BaseKafkaConsumer to consume messages</li>
 *   <li>How to use utility classes</li>
 *   <li>How to handle errors and retries</li>
 * </ul>
 *
 * @author m.huang
 * @since 1.0.0
 */
public class UsageExamples {

    // ============================================
    // Example 1: Basic Message Sending
    // ============================================

    /**
     * Example service showing basic message sending.
     */
    @Slf4j
    @Service
    @RequiredArgsConstructor
    public static class OrderService {
        private final EnhancedKafkaTemplate enhancedKafkaTemplate;

        /**
         * Example 1: Send a single message asynchronously.
         */
        public void sendOrderExample1(Order order) {
            // Prepare custom headers
            Map<String, String> headers = new HashMap<>();
            headers.put("orderType", order.getType());
            headers.put("priority", String.valueOf(order.getPriority()));

            // Send message
            CompletableFuture<SendResult<String, Object>> future = enhancedKafkaTemplate.send(
                    "order-events",
                    order,
                    order.getOrderId(),
                    headers
            );

            // Handle result
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to send order: {}", order.getOrderId(), throwable);
                    // Handle failure: retry, alert, etc.
                } else {
                    log.info("Order sent successfully: orderId={}, offset={}",
                            order.getOrderId(),
                            result.getRecordMetadata().offset());
                }
            });
        }

        /**
         * Example 2: Send message synchronously (wait for result).
         */
        public void sendOrderExample2(Order order) {
            try {
                CompletableFuture<SendResult<String, Object>> future = enhancedKafkaTemplate.send(
                        "order-events",
                        order,
                        order.getOrderId(),
                        null
                );

                // Wait for completion (blocks until done)
                SendResult<String, Object> result = future.get();
                log.info("Order sent: offset={}", result.getRecordMetadata().offset());
            } catch (Exception e) {
                log.error("Error sending order", e);
                throw new RuntimeException("Failed to send order", e);
            }
        }

        /**
         * Example 3: Send message transactionally.
         */
        public void sendOrderExample3(Order order) {
            enhancedKafkaTemplate.sendTransactional(
                    "order-events",
                    order,
                    order.getOrderId(),
                    () -> {
                        // Business logic executed within transaction
                        log.info("Saving order to database: {}", order.getOrderId());
                        // orderRepository.save(order);
                        // inventoryService.reserve(order);
                    }
            );
        }

        /**
         * Example 4: Send batch of messages.
         */
        public void sendOrderBatchExample(List<Order> orders) {
            enhancedKafkaTemplate.sendBatch(
                    "order-events",
                    orders,
                    "batch-order"
            );
        }
    }

    // ============================================
    // Example 2: Message Consumption
    // ============================================

    /**
     * Example consumer showing how to extend BaseKafkaConsumer.
     */
    @Slf4j
    @Component
    public static class OrderConsumer extends BaseKafkaConsumer<Order> {

        private final OrderService orderService;

        /**
         * Constructor with required dependencies.
         */
        public OrderConsumer(
                ObjectMapper objectMapper,
                KafkaTemplate<String, Object> kafkaTemplate,
                int maxRetryCount,
                OrderService orderService) {
            super(objectMapper, kafkaTemplate, maxRetryCount);
            this.orderService = orderService;
        }

        /**
         * Implement business logic for processing orders.
         */
        @Override
        protected void doProcess(Order data, Map<String, String> headers) throws Exception {
            log.info("Processing order: orderId={}, customerId={}, amount={}",
                    data.getOrderId(), data.getCustomerId(), data.getAmount());

            // Extract information from headers
            String msgId = headers.get("msgId");
            String traceId = headers.get("traceId");
            log.debug("Message context: msgId={}, traceId={}", msgId, traceId);

            // 1. Validate order
            validateOrder(data);

            // 2. Process order
            processOrder(data);

            // 3. Handle temporary errors (will trigger retry)
            if (isTemporaryError(data)) {
                throw new RetryableException("Temporary error, will retry");
            }

            log.info("Order processed successfully: orderId={}", data.getOrderId());
        }

        /**
         * Specify the data class type.
         */
        @Override
        protected Class<Order> getDataClass() {
            return Order.class;
        }

        private void validateOrder(Order order) {
            if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
                throw new IllegalArgumentException("Order ID cannot be null or empty");
            }
            if (order.getAmount() == null || order.getAmount() <= 0) {
                throw new IllegalArgumentException("Order amount must be positive");
            }
        }

        private void processOrder(Order order) {
            // Business logic here
            log.debug("Processing order: {}", order.getOrderId());
        }

        private boolean isTemporaryError(Order order) {
            // Example: retry if order amount is too high (simulating rate limiting)
            return order.getAmount() != null && order.getAmount() > 10000;
        }
    }

    // ============================================
    // Example 3: Using Utility Classes
    // ============================================

    /**
     * Example showing how to use utility classes.
     */
    @Slf4j
    @Service
    public static class UtilityExamples {

        /**
         * Example: Using KafkaMessageUtils.
         */
        public void utilityExample1(ConsumerRecord<String, Object> record) {
            // Extract headers
            Map<String, String> headers = KafkaMessageUtils.extractHeaders(record);
            log.info("Extracted headers: {}", headers);

            // Deserialize message
            Order order = KafkaMessageUtils.deserialize(
                    record.value().toString(),
                    Order.class
            );
            log.info("Deserialized order: {}", order);
        }

        /**
         * Example: Using RetryTemplate.
         */
        public void utilityExample2() {
            // Create retry template with custom configuration
            RetryTemplate<String> retryTemplate = RetryTemplate.<String>builder()
                    .maxRetries(5)
                    .initialDelayMs(500)
                    .multiplier(2.0)
                    .maxDelayMs(5000)
                    .retryIf(e -> e instanceof RetryableException)
                    .build();

            // Execute with retry
            try {
                String result = retryTemplate.execute(() -> {
                    // Operation that might fail
                    return performOperation();
                });
                log.info("Operation succeeded: {}", result);
            } catch (Exception e) {
                log.error("Operation failed after retries", e);
            }
        }

        private String performOperation() {
            // Simulate operation
            return "result";
        }
    }

    // ============================================
    // Example 4: Error Handling
    // ============================================

    /**
     * Example showing error handling patterns.
     */
    @Slf4j
    @Service
    public static class ErrorHandlingExamples {

        private final EnhancedKafkaTemplate enhancedKafkaTemplate;

        public ErrorHandlingExamples(EnhancedKafkaTemplate enhancedKafkaTemplate) {
            this.enhancedKafkaTemplate = enhancedKafkaTemplate;
        }

        /**
         * Example: Handle send failures gracefully.
         */
        public void errorHandlingExample1(Order order) {
            enhancedKafkaTemplate.send("order-events", order, order.getOrderId(), null)
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            // Log error
                            log.error("Failed to send order: {}", order.getOrderId(), throwable);

                            // Option 1: Retry with RetryTemplate
                            retrySend(order);

                            // Option 2: Send to fallback queue
                            // sendToFallbackQueue(order);

                            // Option 3: Alert monitoring system
                            // alertMonitoring(throwable);
                        }
                    });
        }

        private void retrySend(Order order) {
            RetryTemplate<Void> retryTemplate = RetryTemplate.<Void>builder()
                    .maxRetries(3)
                    .initialDelayMs(1000)
                    .build();

            try {
                retryTemplate.execute(() -> {
                    enhancedKafkaTemplate.send("order-events", order, order.getOrderId(), null)
                            .get(); // Wait for completion
                    return null;
                });
            } catch (Exception e) {
                log.error("Retry failed for order: {}", order.getOrderId(), e);
            }
        }
    }

    // ============================================
    // Data Models
    // ============================================

    /**
     * Example Order model.
     */
    @Data
    public static class Order {
        private String orderId;
        private String customerId;
        private String type;
        private Integer priority;
        private Double amount;
    }
}

