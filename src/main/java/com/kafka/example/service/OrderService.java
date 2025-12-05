package com.kafka.example.service;

import com.kafka.core.exception.MessageSendException;
import com.kafka.core.producer.EnhancedKafkaTemplate;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Example service demonstrating how to use EnhancedKafkaTemplate.
 * 
 * <p>This service shows various ways to send messages to Kafka:
 * <ul>
 *   <li>Asynchronous message sending</li>
 *   <li>Synchronous message sending</li>
 *   <li>Transactional message sending</li>
 *   <li>Batch message sending</li>
 * </ul>
 * 
 * <p>This is a reference implementation. In production, you should:
 * <ul>
 *   <li>Add proper error handling</li>
 *   <li>Implement retry logic for failures</li>
 *   <li>Add monitoring and alerting</li>
 *   <li>Validate input parameters</li>
 * </ul>
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final EnhancedKafkaTemplate enhancedKafkaTemplate;

    /**
     * Example 1: Send a single order message asynchronously.
     * 
     * <p>This is the recommended approach for most use cases as it doesn't block
     * the calling thread.
     *
     * @param order the order to send (must not be null)
     * @throws IllegalArgumentException if order is null
     */
    public void sendOrder(Order order) {
        if (order == null) {
            log.error("Order cannot be null");
            throw new IllegalArgumentException("Order cannot be null");
        }
        if (order.getOrderId() == null || order.getOrderId().trim().isEmpty()) {
            log.error("Order ID cannot be null or empty");
            throw new IllegalArgumentException("Order ID cannot be null or empty");
        }

        try {
            log.debug("Preparing to send order: {}", order.getOrderId());

            // Prepare custom headers
            Map<String, String> headers = new HashMap<>();
            if (order.getType() != null) {
                headers.put("orderType", order.getType());
            }
            if (order.getPriority() != null) {
                headers.put("priority", String.valueOf(order.getPriority()));
            }

            // Send message asynchronously
            CompletableFuture<SendResult<String, Object>> future = enhancedKafkaTemplate.send(
                    "order-events",
                    order,
                    order.getOrderId(),  // bizKey
                    headers
            );

            // Handle result
            future.whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to send order: orderId={}", order.getOrderId(), throwable);
                    // In production, you might want to:
                    // - Retry the send
                    // - Send to a fallback queue
                    // - Alert monitoring system
                } else {
                    log.info("Order sent successfully: orderId={}, offset={}, partition={}",
                            order.getOrderId(),
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition());
                }
            });

        } catch (Exception e) {
            log.error("Error sending order: orderId={}", order.getOrderId(), e);
            throw new MessageSendException("Failed to send order: " + order.getOrderId(), e);
        }
    }

    /**
     * Example 2: Send order synchronously (wait for result).
     * 
     * <p>This method blocks until the message is sent. Use this only when
     * you need to ensure the message is sent before proceeding.
     * 
     * <p>⚠️ Warning: This blocks the calling thread. Use with caution.
     *
     * @param order the order to send (must not be null)
     * @throws MessageSendException if send fails
     * @throws IllegalArgumentException if order is null
     */
    public void sendOrderSync(Order order) {
        if (order == null) {
            log.error("Order cannot be null");
            throw new IllegalArgumentException("Order cannot be null");
        }

        try {
            log.debug("Sending order synchronously: orderId={}", order.getOrderId());

            CompletableFuture<SendResult<String, Object>> future = enhancedKafkaTemplate.send(
                    "order-events",
                    order,
                    order.getOrderId(),
                    null
            );

            // Wait for completion (blocks until done)
            SendResult<String, Object> result = future.get();
            log.info("Order sent synchronously: orderId={}, offset={}, partition={}",
                    order.getOrderId(),
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());

        } catch (Exception e) {
            log.error("Error sending order synchronously: orderId={}", order.getOrderId(), e);
            throw new MessageSendException("Failed to send order synchronously: " + order.getOrderId(), e);
        }
    }

    /**
     * Example 3: Send order transactionally with business logic.
     * 
     * <p>This method executes business logic and message sending within a single
     * transaction. If either fails, both are rolled back.
     * 
     * <p>Note: This requires a KafkaTransactionManager to be configured.
     *
     * @param order the order to send (must not be null)
     * @throws MessageSendException if transaction fails
     * @throws IllegalArgumentException if order is null
     */
    public void sendOrderTransactional(Order order) {
        if (order == null) {
            log.error("Order cannot be null");
            throw new IllegalArgumentException("Order cannot be null");
        }

        log.info("Starting transactional send for order: orderId={}", order.getOrderId());

        enhancedKafkaTemplate.sendTransactional(
                "order-events",
                order,
                order.getOrderId(),
                () -> {
                    // Business logic executed within transaction
                    log.debug("Executing business logic in transaction: orderId={}", order.getOrderId());
                    
                    // Example: save to database
                    // orderRepository.save(order);
                    
                    // Example: update inventory
                    // inventoryService.reserve(order);
                    
                    // Example: validate business rules
                    // validateOrder(order);
                    
                    log.debug("Business logic completed: orderId={}", order.getOrderId());
                }
        );

        log.info("Transactional send completed for order: orderId={}", order.getOrderId());
    }

    /**
     * Example 4: Send batch of orders.
     * 
     * <p>This method sends multiple orders in parallel. Business keys are
     * automatically generated as: {bizKeyPrefix}-{index}
     * 
     * <p>The method returns immediately without waiting for all sends to complete.
     * Use the CompletableFuture callbacks to track completion.
     *
     * @param orders list of orders to send (must not be null or empty)
     * @throws IllegalArgumentException if orders is null or empty
     */
    public void sendOrderBatch(List<Order> orders) {
        if (orders == null || orders.isEmpty()) {
            log.warn("Order list is null or empty, skipping batch send");
            return;
        }

        log.info("Sending batch of {} orders", orders.size());

        enhancedKafkaTemplate.sendBatch(
                "order-events",
                orders,
                "batch-order"  // bizKey prefix
        );

        log.info("Batch send initiated for {} orders", orders.size());
    }

    /**
     * Example Order model for demonstration purposes.
     * 
     * <p>In production, this would typically be in a separate model package.
     */
    @Data
    public static class Order {
        /**
         * Order ID - unique identifier for the order.
         */
        private String orderId;
        
        /**
         * Customer ID - identifier of the customer who placed the order.
         */
        private String customerId;
        
        /**
         * Order type - type of order (e.g., "STANDARD", "PREMIUM", "EXPRESS").
         */
        private String type;
        
        /**
         * Priority - order priority level (1-10, higher is more urgent).
         */
        private Integer priority;
        
        /**
         * Amount - total order amount.
         */
        private Double amount;
    }
}
