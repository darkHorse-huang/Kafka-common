package com.kafka.aspect;

import com.kafka.annotation.KafkaConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Aspect for dynamically registering Kafka listeners based on @KafkaConsumer annotation.
 *
 * @author m.huang
 * @since 1.0.0
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class KafkaConsumerAspect {

    private final ApplicationContext applicationContext;
    private final ConsumerFactory<String, Object> consumerFactory;

    // Store container references for management
    private final Map<String, ConcurrentMessageListenerContainer<String, Object>> containers = new ConcurrentHashMap<>();

    /**
     * Post construct: scan and register all @KafkaConsumer annotated methods.
     */
    @PostConstruct
    public void registerAllConsumers() {
        log.info("Scanning for @KafkaConsumer annotated methods...");
        
        // Get all beans from application context
        String[] beanNames = applicationContext.getBeanNamesForType(Object.class);
        
        for (String beanName : beanNames) {
            Object bean = applicationContext.getBean(beanName);
            Class<?> beanClass = bean.getClass();
            
            // Check all methods in the bean
            Method[] methods = beanClass.getDeclaredMethods();
            for (Method method : methods) {
                KafkaConsumer annotation = method.getAnnotation(KafkaConsumer.class);
                if (annotation != null) {
                    try {
                        registerKafkaListener(bean, method, annotation);
                    } catch (Exception e) {
                        log.error("Failed to register Kafka listener for method: {}.{}", 
                                beanClass.getName(), method.getName(), e);
                    }
                }
            }
        }
        
        log.info("Registered {} Kafka listeners", containers.size());
    }

    /**
     * Around advice for @KafkaConsumer annotation.
     * This intercepts method calls and ensures the listener is registered.
     *
     * @param joinPoint the join point
     * @return the method result
     * @throws Throwable if an error occurs
     */
    @Around("@annotation(com.kafka.annotation.KafkaConsumer)")
    public Object aroundKafkaConsumer(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        KafkaConsumer annotation = method.getAnnotation(KafkaConsumer.class);
        
        if (annotation != null) {
            String containerId = generateContainerId(method, annotation);
            
            // Check if listener is already registered
            if (!containers.containsKey(containerId)) {
                Object target = joinPoint.getTarget();
                registerKafkaListener(target, method, annotation);
            }
        }
        
        // Proceed with method execution (though for Kafka listeners, this might not be called)
        return joinPoint.proceed();
    }

    /**
     * Register a Kafka listener for the given method.
     *
     * @param bean       the bean instance
     * @param method     the method to register
     * @param annotation the @KafkaConsumer annotation
     */
    private void registerKafkaListener(Object bean, Method method, KafkaConsumer annotation) {
        String containerId = generateContainerId(method, annotation);
        
        if (containers.containsKey(containerId)) {
            log.warn("Kafka listener already registered for container: {}", containerId);
            return;
        }

        log.info("Registering Kafka listener: topic={}, groupId={}, concurrency={}, batch={}",
                annotation.topic(), annotation.groupId(), annotation.concurrency(), annotation.batch());

        try {
            // Create container factory with annotation parameters
            ConcurrentKafkaListenerContainerFactory<String, Object> factory = createContainerFactory(annotation);

            // Create container properties
            ContainerProperties containerProperties = new ContainerProperties(annotation.topic());
            containerProperties.setAckMode(parseAckMode(annotation.ackMode()));
            
            // Set group ID if specified
            if (!annotation.groupId().isEmpty()) {
                containerProperties.setGroupId(annotation.groupId());
            }

            // Create message listener based on batch mode
            if (annotation.batch()) {
                // Batch listener
                BatchAcknowledgingMessageListener<String, Object> batchListener = createBatchMessageListener(bean, method);
                containerProperties.setMessageListener(batchListener);
            } else {
                // Single message listener
                AcknowledgingMessageListener<String, Object> singleListener = createSingleMessageListener(bean, method);
                containerProperties.setMessageListener(singleListener);
            }

            // Create container
            ConcurrentMessageListenerContainer<String, Object> container = 
                    new ConcurrentMessageListenerContainer<>(
                            factory.getConsumerFactory(), 
                            containerProperties);
            container.setConcurrency(annotation.concurrency());

            // Set container ID
            container.setBeanName(containerId);

            // Start container
            container.start();

            // Store container reference
            containers.put(containerId, container);

            log.info("Successfully registered and started Kafka listener container: {}", containerId);
        } catch (Exception e) {
            log.error("Failed to register Kafka listener for method: {}.{}", 
                    bean.getClass().getName(), method.getName(), e);
            throw new RuntimeException("Failed to register Kafka listener", e);
        }
    }

    /**
     * Create container factory based on annotation parameters.
     *
     * @param annotation the @KafkaConsumer annotation
     * @return configured container factory
     */
    private ConcurrentKafkaListenerContainerFactory<String, Object> createContainerFactory(KafkaConsumer annotation) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        
        // Create consumer factory with group ID if specified
        ConsumerFactory<String, Object> consumerFactoryToUse = consumerFactory;
        if (!annotation.groupId().isEmpty()) {
            // Create new consumer factory with custom group ID
            if (consumerFactory instanceof DefaultKafkaConsumerFactory) {
                DefaultKafkaConsumerFactory<String, Object> defaultFactory = 
                        (DefaultKafkaConsumerFactory<String, Object>) consumerFactory;
                Map<String, Object> configProps = new HashMap<>(defaultFactory.getConfigurationProperties());
                configProps.put(ConsumerConfig.GROUP_ID_CONFIG, annotation.groupId());
                consumerFactoryToUse = new DefaultKafkaConsumerFactory<>(configProps);
            } else {
                // Fallback: use existing factory if not DefaultKafkaConsumerFactory
                log.warn("ConsumerFactory is not DefaultKafkaConsumerFactory, using default group ID");
            }
        }
        
        factory.setConsumerFactory(consumerFactoryToUse);
        factory.setConcurrency(annotation.concurrency());
        
        // Configure batch consumption if enabled
        factory.setBatchListener(annotation.batch());
        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(parseAckMode(annotation.ackMode()));

        return factory;
    }

    /**
     * Create batch message listener that invokes the annotated method.
     *
     * @param bean   the bean instance
     * @param method the method to invoke
     * @return batch message listener
     */
    private BatchAcknowledgingMessageListener<String, Object> createBatchMessageListener(
            Object bean, Method method) {
        
        method.setAccessible(true);
        
        return new BatchAcknowledgingMessageListener<String, Object>() {
            @Override
            public void onMessage(java.util.List<ConsumerRecord<String, Object>> records, Acknowledgment ack) {
                try {
                    // Invoke method with records and acknowledgment
                    if (method.getParameterCount() == 2) {
                        method.invoke(bean, records, ack);
                    } else if (method.getParameterCount() == 1) {
                        method.invoke(bean, records);
                    } else {
                        method.invoke(bean);
                    }
                } catch (Exception e) {
                    log.error("Error invoking batch consumer method: {}.{}", 
                            bean.getClass().getName(), method.getName(), e);
                    throw new RuntimeException("Error processing batch messages", e);
                }
            }
        };
    }

    /**
     * Create single message listener that invokes the annotated method.
     *
     * @param bean   the bean instance
     * @param method the method to invoke
     * @return single message listener
     */
    private AcknowledgingMessageListener<String, Object> createSingleMessageListener(
            Object bean, Method method) {
        
        method.setAccessible(true);
        
        return new AcknowledgingMessageListener<String, Object>() {
            @Override
            public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment ack) {
                try {
                    // Invoke method with record and acknowledgment
                    if (method.getParameterCount() == 2) {
                        method.invoke(bean, record, ack);
                    } else if (method.getParameterCount() == 1) {
                        method.invoke(bean, record);
                    } else {
                        method.invoke(bean);
                    }
                } catch (Exception e) {
                    log.error("Error invoking consumer method: {}.{}", 
                            bean.getClass().getName(), method.getName(), e);
                    throw new RuntimeException("Error processing message", e);
                }
            }
        };
    }

    /**
     * Parse acknowledgment mode from string.
     *
     * @param ackModeStr the acknowledgment mode string
     * @return ContainerProperties.AckMode
     */
    private ContainerProperties.AckMode parseAckMode(String ackModeStr) {
        try {
            return ContainerProperties.AckMode.valueOf(ackModeStr);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid ack mode: {}, using MANUAL_IMMEDIATE", ackModeStr);
            return ContainerProperties.AckMode.MANUAL_IMMEDIATE;
        }
    }

    /**
     * Generate unique container ID.
     *
     * @param method     the method
     * @param annotation the annotation
     * @return container ID
     */
    private String generateContainerId(Method method, KafkaConsumer annotation) {
        String groupId = annotation.groupId().isEmpty() ? "default-group" : annotation.groupId();
        return String.format("%s-%s-%s", annotation.topic(), groupId, method.getName());
    }

    /**
     * Get all registered container references.
     *
     * @return map of container IDs to containers
     */
    public Map<String, ConcurrentMessageListenerContainer<String, Object>> getContainers() {
        return new HashMap<>(containers);
    }

    /**
     * Stop and remove a container.
     *
     * @param containerId the container ID
     */
    public void stopContainer(String containerId) {
        ConcurrentMessageListenerContainer<String, Object> container = containers.remove(containerId);
        if (container != null) {
            container.stop();
            log.info("Stopped and removed container: {}", containerId);
        }
    }

    /**
     * Stop all containers.
     */
    public void stopAllContainers() {
        containers.forEach((id, container) -> {
            try {
                container.stop();
                log.info("Stopped container: {}", id);
            } catch (Exception e) {
                log.error("Error stopping container: {}", id, e);
            }
        });
        containers.clear();
    }
}

