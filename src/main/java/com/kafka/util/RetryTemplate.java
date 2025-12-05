package com.kafka.util;

import com.kafka.core.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Retry template with exponential backoff strategy.
 * 
 * <p>This class provides a flexible retry mechanism with configurable:
 * <ul>
 *   <li>Maximum retry attempts</li>
 *   <li>Initial delay</li>
 *   <li>Exponential backoff multiplier</li>
 *   <li>Maximum delay cap</li>
 *   <li>Custom retry conditions</li>
 * </ul>
 * 
 * <p>The retry delay follows an exponential backoff pattern:
 * <pre>
 * delay = initialDelay * (multiplier ^ (attempt - 1))
 * delay = min(delay, maxDelay)
 * </pre>
 * 
 * <p>Example usage:
 * <pre>{@code
 * // Create retry template
 * RetryTemplate<String> template = RetryTemplate.<String>builder()
 *     .maxRetries(5)
 *     .initialDelayMs(500)
 *     .multiplier(2.0)
 *     .maxDelayMs(5000)
 *     .retryIf(e -> e instanceof RetryableException)
 *     .build();
 * 
 * // Execute with retry
 * String result = template.execute(() -> {
 *     return performOperation();
 * });
 * }</pre>
 * 
 * <p>This class is thread-safe and can be used concurrently.
 *
 * @param <T> the return type of the operation
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
public class RetryTemplate<T> {

    /**
     * Default maximum number of retries.
     */
    private static final int DEFAULT_MAX_RETRIES = 3;
    
    /**
     * Default initial delay in milliseconds.
     */
    private static final long DEFAULT_INITIAL_DELAY_MS = 1000L;
    
    /**
     * Default backoff multiplier.
     */
    private static final double DEFAULT_MULTIPLIER = 2.0;
    
    /**
     * Default maximum delay in milliseconds.
     */
    private static final long DEFAULT_MAX_DELAY_MS = 10000L;

    /**
     * Maximum number of retry attempts.
     */
    private final int maxRetries;
    
    /**
     * Initial delay in milliseconds.
     */
    private final long initialDelayMs;
    
    /**
     * Backoff multiplier for exponential delay.
     */
    private final double multiplier;
    
    /**
     * Maximum delay in milliseconds.
     */
    private final long maxDelayMs;
    
    /**
     * Predicate to determine if an exception should trigger a retry.
     */
    private Predicate<Exception> retryCondition;

    /**
     * Private constructor. Use builder to create instances.
     *
     * @param maxRetries     maximum number of retries (must be >= 0)
     * @param initialDelayMs initial delay in milliseconds (must be >= 0)
     * @param multiplier     backoff multiplier (must be > 0)
     * @param maxDelayMs     maximum delay in milliseconds (must be >= initialDelayMs)
     * @throws IllegalArgumentException if parameters are invalid
     */
    private RetryTemplate(int maxRetries, long initialDelayMs, double multiplier, long maxDelayMs) {
        if (maxRetries < 0) {
            throw new IllegalArgumentException("MaxRetries must be >= 0");
        }
        if (initialDelayMs < 0) {
            throw new IllegalArgumentException("InitialDelayMs must be >= 0");
        }
        if (multiplier <= 0) {
            throw new IllegalArgumentException("Multiplier must be > 0");
        }
        if (maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("MaxDelayMs must be >= InitialDelayMs");
        }
        
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.multiplier = multiplier;
        this.maxDelayMs = maxDelayMs;
        this.retryCondition = this::defaultRetryCondition;
        
        log.debug("RetryTemplate created: maxRetries={}, initialDelayMs={}, multiplier={}, maxDelayMs={}",
                maxRetries, initialDelayMs, multiplier, maxDelayMs);
    }

    /**
     * Create a new RetryTemplate builder.
     *
     * @param <T> the return type
     * @return a new builder instance
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Create a RetryTemplate with default settings.
     * 
     * <p>Default settings:
     * <ul>
     *   <li>maxRetries: 3</li>
     *   <li>initialDelayMs: 1000</li>
     *   <li>multiplier: 2.0</li>
     *   <li>maxDelayMs: 10000</li>
     *   <li>retryCondition: RetryableException</li>
     * </ul>
     *
     * @param <T> the return type
     * @return a new RetryTemplate instance with default settings
     */
    public static <T> RetryTemplate<T> defaultTemplate() {
        return new RetryTemplate<>(DEFAULT_MAX_RETRIES, DEFAULT_INITIAL_DELAY_MS, DEFAULT_MULTIPLIER, DEFAULT_MAX_DELAY_MS);
    }

    /**
     * Execute an operation with retry logic.
     * 
     * <p>This method will:
     * <ol>
     *   <li>Execute the operation</li>
     *   <li>If it fails and should retry, wait for the calculated delay</li>
     *   <li>Retry up to maxRetries times</li>
     *   <li>Throw the last exception if all retries fail</li>
     * </ol>
     *
     * @param operation the operation to execute (must not be null)
     * @return the result of the operation
     * @throws IllegalArgumentException if operation is null
     * @throws Exception if all retries fail
     */
    public T execute(Supplier<T> operation) throws Exception {
        if (operation == null) {
            throw new IllegalArgumentException("Operation cannot be null");
        }

        Exception lastException = null;
        long startTime = System.currentTimeMillis();

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    long delay = calculateDelay(attempt);
                    log.debug("Retrying operation, attempt: {}/{}, delay: {}ms", attempt, maxRetries, delay);
                    Thread.sleep(delay);
                }

                log.debug("Executing operation, attempt: {}/{}", attempt + 1, maxRetries + 1);
                T result = operation.get();
                
                long duration = System.currentTimeMillis() - startTime;
                if (attempt > 0) {
                    log.info("Operation succeeded after {} retries, duration: {}ms", attempt, duration);
                } else {
                    log.debug("Operation succeeded on first attempt, duration: {}ms", duration);
                }
                
                return result;
            } catch (Exception e) {
                lastException = e;
                long duration = System.currentTimeMillis() - startTime;

                if (attempt < maxRetries && shouldRetry(e)) {
                    log.warn("Operation failed, will retry. Attempt: {}/{}, Error: {}, duration: {}ms",
                            attempt + 1, maxRetries + 1, e.getClass().getSimpleName(), duration);
                    continue;
                } else {
                    long totalDuration = System.currentTimeMillis() - startTime;
                    if (attempt >= maxRetries) {
                        log.error("Operation failed after {} attempts, duration: {}ms", 
                                attempt + 1, totalDuration, e);
                    } else {
                        log.error("Operation failed with non-retryable exception, attempt: {}/{}, duration: {}ms",
                                attempt + 1, maxRetries + 1, totalDuration, e);
                    }
                    throw e;
                }
            }
        }

        if (lastException != null) {
            throw lastException;
        }

        throw new RuntimeException("Operation failed without exception");
    }

    /**
     * Execute an operation with retry logic (void return type).
     * 
     * <p>This is a convenience method for operations that don't return a value.
     *
     * @param operation the operation to execute (must not be null)
     * @throws IllegalArgumentException if operation is null
     * @throws Exception if all retries fail
     */
    public void execute(Runnable operation) throws Exception {
        if (operation == null) {
            throw new IllegalArgumentException("Operation cannot be null");
        }
        execute(() -> {
            operation.run();
            return null;
        });
    }

    /**
     * Set custom retry condition.
     * 
     * <p>This method allows you to change the retry condition after the template
     * has been created. The condition is evaluated for each exception.
     *
     * @param condition the condition predicate (must not be null)
     * @return this RetryTemplate instance for method chaining
     * @throws IllegalArgumentException if condition is null
     */
    public RetryTemplate<T> retryIf(Predicate<Exception> condition) {
        if (condition == null) {
            throw new IllegalArgumentException("Retry condition cannot be null");
        }
        this.retryCondition = condition;
        log.debug("Retry condition updated");
        return this;
    }

    /**
     * Check if the exception should trigger a retry.
     *
     * @param exception the exception to check (must not be null)
     * @return true if should retry, false otherwise
     */
    private boolean shouldRetry(Exception exception) {
        if (exception == null) {
            return false;
        }
        try {
            return retryCondition.test(exception);
        } catch (Exception e) {
            log.warn("Error evaluating retry condition", e);
            return false;
        }
    }

    /**
     * Default retry condition: retry on RetryableException.
     *
     * @param exception the exception to check
     * @return true if exception is RetryableException
     */
    private boolean defaultRetryCondition(Exception exception) {
        return exception instanceof RetryableException;
    }

    /**
     * Calculate delay for the given attempt number using exponential backoff.
     * 
     * <p>Formula: delay = initialDelay * (multiplier ^ (attempt - 1))
     * The result is capped at maxDelay.
     *
     * @param attempt the attempt number (1-based)
     * @return the delay in milliseconds
     */
    private long calculateDelay(int attempt) {
        if (attempt <= 1) {
            return initialDelayMs;
        }
        
        long delay = (long) (initialDelayMs * Math.pow(multiplier, attempt - 1));
        return Math.min(delay, maxDelayMs);
    }

    /**
     * Builder for RetryTemplate.
     * 
     * <p>This builder provides a fluent API for creating RetryTemplate instances.
     *
     * @param <T> the return type
     */
    public static class Builder<T> {
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private long initialDelayMs = DEFAULT_INITIAL_DELAY_MS;
        private double multiplier = DEFAULT_MULTIPLIER;
        private long maxDelayMs = DEFAULT_MAX_DELAY_MS;
        private Predicate<Exception> retryCondition;

        /**
         * Set maximum number of retries.
         *
         * @param maxRetries maximum retries (must be >= 0)
         * @return this builder
         * @throws IllegalArgumentException if maxRetries < 0
         */
        public Builder<T> maxRetries(int maxRetries) {
            if (maxRetries < 0) {
                throw new IllegalArgumentException("MaxRetries must be >= 0");
            }
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Set initial delay in milliseconds.
         *
         * @param initialDelayMs initial delay (must be >= 0)
         * @return this builder
         * @throws IllegalArgumentException if initialDelayMs < 0
         */
        public Builder<T> initialDelayMs(long initialDelayMs) {
            if (initialDelayMs < 0) {
                throw new IllegalArgumentException("InitialDelayMs must be >= 0");
            }
            this.initialDelayMs = initialDelayMs;
            return this;
        }

        /**
         * Set backoff multiplier.
         *
         * @param multiplier multiplier value (must be > 0)
         * @return this builder
         * @throws IllegalArgumentException if multiplier <= 0
         */
        public Builder<T> multiplier(double multiplier) {
            if (multiplier <= 0) {
                throw new IllegalArgumentException("Multiplier must be > 0");
            }
            this.multiplier = multiplier;
            return this;
        }

        /**
         * Set maximum delay in milliseconds.
         *
         * @param maxDelayMs maximum delay (must be >= initialDelayMs)
         * @return this builder
         * @throws IllegalArgumentException if maxDelayMs < initialDelayMs
         */
        public Builder<T> maxDelayMs(long maxDelayMs) {
            if (maxDelayMs < initialDelayMs) {
                throw new IllegalArgumentException("MaxDelayMs must be >= InitialDelayMs");
            }
            this.maxDelayMs = maxDelayMs;
            return this;
        }

        /**
         * Set custom retry condition.
         *
         * @param condition the condition predicate (must not be null)
         * @return this builder
         * @throws IllegalArgumentException if condition is null
         */
        public Builder<T> retryIf(Predicate<Exception> condition) {
            if (condition == null) {
                throw new IllegalArgumentException("Retry condition cannot be null");
            }
            this.retryCondition = condition;
            return this;
        }

        /**
         * Build the RetryTemplate instance.
         *
         * @return a new RetryTemplate instance
         */
        public RetryTemplate<T> build() {
            RetryTemplate<T> template = new RetryTemplate<>(maxRetries, initialDelayMs, multiplier, maxDelayMs);
            if (retryCondition != null) {
                template.retryCondition = retryCondition;
            }
            return template;
        }
    }
}
