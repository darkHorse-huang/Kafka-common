package com.kafka.example.consumer;

import com.kafka.core.consumer.BaseKafkaConsumer;
import com.kafka.core.exception.RetryableException;
import com.kafka.example.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Example consumer demonstrating how to extend BaseKafkaConsumer.
 * 
 * <p>This consumer processes order messages from Kafka and demonstrates:
 * <ul>
 *   <li>How to extend BaseKafkaConsumer</li>
 *   <li>How to implement business logic</li>
 *   <li>How to handle exceptions (retryable vs non-retryable)</li>
 *   <li>How to extract and use message headers</li>
 * </ul>
 * 
 * <p>This is a reference implementation. In production, you should:
 * <ul>
 *   <li>Add proper validation</li>
 *   <li>Implement idempotency checks</li>
 *   <li>Add monitoring and metrics</li>
 *   <li>Handle edge cases</li>
 * </ul>
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
@Component
public class OrderConsumer extends BaseKafkaConsumer<OrderService.Order> {

    private final OrderService orderService;
    private final int maxRetryCount;

    /**
     * Constructor with required dependencies.
     * 
     * <p>The maxRetryCount can be configured via application properties:
     * <pre>{@code
     * kafka:
     *   retry:
     *     max-attempts: 3
     * }</pre>
     *
     * @param objectMapper    the object mapper for JSON deserialization (must not be null)
     * @param kafkaTemplate   the kafka template for DLT/retry (must not be null)
     * @param maxRetryCount   maximum number of retry attempts (must be >= 0)
     * @param orderService    the order service for business logic (must not be null)
     * @throws IllegalArgumentException if any parameter is null or maxRetryCount < 0
     */
    public OrderConsumer(
            ObjectMapper objectMapper,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${kafka.retry.max-attempts:3}") int maxRetryCount,
            OrderService orderService) {
        super(objectMapper, kafkaTemplate, maxRetryCount);
        this.orderService = orderService;
        this.maxRetryCount = maxRetryCount;
        
        log.info("OrderConsumer initialized with maxRetryCount: {}", maxRetryCount);
    }

    /**
     * Implement the business logic for processing order messages.
     * 
     * <p>This method is called by the template method in BaseKafkaConsumer
     * after the message has been successfully parsed.
     * 
     * <p>Exception handling:
     * <ul>
     *   <li>Throw {@link RetryableException} for temporary errors (will trigger retry)</li>
     *   <li>Throw other exceptions for permanent errors (will send to DLT)</li>
     * </ul>
     *
     * @param data    the order data extracted from the message (must not be null)
     * @param headers the message headers (never null, may be empty)
     * @throws RetryableException if a temporary error occurs that should be retried
     * @throws Exception if a permanent error occurs that should go to DLT
     */
    @Override
    protected void doProcess(OrderService.Order data, Map<String, String> headers) throws Exception {
        log.info("Processing order: orderId={}, customerId={}, amount={}",
                data.getOrderId(), data.getCustomerId(), data.getAmount());

        // Extract information from headers
        String msgId = headers.get("msgId");
        String traceId = headers.get("traceId");
        String spanId = headers.get("spanId");
        log.debug("Message context: msgId={}, traceId={}, spanId={}", msgId, traceId, spanId);

        // Step 1: Validate order
        validateOrder(data);

        // Step 2: Check for duplicate processing (idempotency)
        if (isAlreadyProcessed(data.getOrderId())) {
            log.warn("Order already processed, skipping: orderId={}", data.getOrderId());
            return; // Idempotent: already processed, just return
        }

        // Step 3: Process order
        try {
            processOrder(data);
            log.info("Order processed successfully: orderId={}", data.getOrderId());
        } catch (TemporaryException e) {
            // Temporary error: throw RetryableException to trigger retry
            log.warn("Temporary error processing order: orderId={}, will retry", data.getOrderId(), e);
            throw new RetryableException("Temporary error processing order: " + data.getOrderId(), e);
        } catch (ValidationException e) {
            // Validation error: should not retry
            log.error("Validation error processing order: orderId={}", data.getOrderId(), e);
            throw new IllegalArgumentException("Validation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Get the data class type for deserialization.
     * 
     * <p>This method must be overridden to return the actual data type.
     * The default implementation returns Object.class, which won't work
     * correctly for generic deserialization.
     *
     * @return the Order class
     */
    @Override
    protected Class<OrderService.Order> getDataClass() {
        return OrderService.Order.class;
    }

    /**
     * Validate order data.
     * 
     * @param order the order to validate
     * @throws IllegalArgumentException if validation fails
     */
    private void validateOrder(OrderService.Order order) {
        if (order.getOrderId() == null || order.getOrderId().trim().isEmpty()) {
            throw new IllegalArgumentException("Order ID cannot be null or empty");
        }
        if (order.getCustomerId() == null || order.getCustomerId().trim().isEmpty()) {
            throw new IllegalArgumentException("Customer ID cannot be null or empty");
        }
        if (order.getAmount() == null || order.getAmount() <= 0) {
            throw new IllegalArgumentException("Order amount must be positive");
        }
        log.debug("Order validation passed: orderId={}", order.getOrderId());
    }

    /**
     * Check if order has already been processed (idempotency check).
     * 
     * <p>In production, this would typically check a database or cache.
     *
     * @param orderId the order ID to check
     * @return true if already processed, false otherwise
     */
    private boolean isAlreadyProcessed(String orderId) {
        // Example: check database or cache
        // return orderRepository.existsById(orderId);
        return false; // For demo purposes, always return false
    }

    /**
     * Process the order (business logic).
     * 
     * <p>This method contains the actual business logic for processing an order.
     * In production, this might:
     * <ul>
     *   <li>Save order to database</li>
     *   <li>Update inventory</li>
     *   <li>Send notifications</li>
     *   <li>Trigger downstream processes</li>
     * </ul>
     *
     * @param order the order to process
     * @throws TemporaryException if a temporary error occurs
     * @throws ValidationException if validation fails
     */
    private void processOrder(OrderService.Order order) throws TemporaryException, ValidationException {
        log.debug("Executing business logic for order: orderId={}", order.getOrderId());
        
        // Example business logic
        // orderService.saveOrder(order);
        // inventoryService.updateInventory(order);
        // notificationService.sendConfirmation(order);
        
        // Simulate temporary error for demonstration
        if (order.getAmount() != null && order.getAmount() > 10000) {
            log.warn("Order amount too high, simulating rate limiting: orderId={}, amount={}", 
                    order.getOrderId(), order.getAmount());
            throw new TemporaryException("Rate limit exceeded for high-value orders");
        }
    }

    /**
     * Temporary exception for demonstration purposes.
     * In production, use appropriate exception types.
     */
    private static class TemporaryException extends Exception {
        TemporaryException(String message) {
            super(message);
        }
    }

    /**
     * Validation exception for demonstration purposes.
     * In production, use appropriate exception types.
     */
    private static class ValidationException extends Exception {
        ValidationException(String message) {
            super(message);
        }
    }
}
