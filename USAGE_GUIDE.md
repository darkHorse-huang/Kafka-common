# Kafka Common 使用指南

本文档提供 kafka-common 库的完整使用指南，包括详细的使用示例和最佳实践。

## 目录

- [快速开始](#快速开始)
- [消息发送](#消息发送)
- [消息消费](#消息消费)
- [工具类使用](#工具类使用)
- [错误处理](#错误处理)
- [性能优化](#性能优化)
- [常见问题](#常见问题)

## 快速开始

### 1. 添加依赖

```xml
<dependency>
    <groupId>com.company</groupId>
    <artifactId>kafka-common</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. 配置 Kafka

在 `application.yml` 中配置：

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092

kafka:
  monitoring:
    enabled: true
  trace:
    enabled: true
```

### 3. 注入 EnhancedKafkaTemplate

```java
@Service
@RequiredArgsConstructor
public class OrderService {
    private final EnhancedKafkaTemplate enhancedKafkaTemplate;
}
```

## 消息发送

### 基本发送

```java
// 异步发送
CompletableFuture<SendResult<String, Object>> future = 
    enhancedKafkaTemplate.send("order-events", order, orderId, headers);

future.whenComplete((result, ex) -> {
    if (ex != null) {
        log.error("Send failed", ex);
    } else {
        log.info("Sent: offset={}", result.getRecordMetadata().offset());
    }
});
```

### 同步发送

```java
try {
    SendResult<String, Object> result = 
        enhancedKafkaTemplate.send("order-events", order, orderId, null).get();
    log.info("Sent: offset={}", result.getRecordMetadata().offset());
} catch (Exception e) {
    log.error("Send failed", e);
}
```

### 事务性发送

```java
enhancedKafkaTemplate.sendTransactional(
    "order-events",
    order,
    orderId,
    () -> {
        // 业务逻辑：保存到数据库
        orderRepository.save(order);
        // 消息发送会自动包含在事务中
    }
);
```

### 批量发送

```java
List<Order> orders = getOrders();
enhancedKafkaTemplate.sendBatch("order-events", orders, "batch-order");
```

## 消息消费

### 继承 BaseKafkaConsumer

```java
@Component
public class OrderConsumer extends BaseKafkaConsumer<Order> {
    
    public OrderConsumer(
            ObjectMapper objectMapper,
            KafkaTemplate<String, Object> kafkaTemplate,
            int maxRetryCount) {
        super(objectMapper, kafkaTemplate, maxRetryCount);
    }

    @Override
    protected void doProcess(Order data, Map<String, String> headers) throws Exception {
        // 实现业务逻辑
        processOrder(data);
    }

    @Override
    protected Class<Order> getDataClass() {
        return Order.class;
    }
}
```

### 异常处理

```java
@Override
protected void doProcess(Order data, Map<String, String> headers) throws Exception {
    try {
        // 业务逻辑
        processOrder(data);
    } catch (TemporaryException e) {
        // 临时错误，会触发重试
        throw new RetryableException("Temporary error", e);
    } catch (ValidationException e) {
        // 验证错误，直接发送到 DLT
        throw new BusinessException("Validation failed", e);
    }
}
```

## 工具类使用

### KafkaMessageUtils

```java
// 提取头信息
Map<String, String> headers = KafkaMessageUtils.extractHeaders(record);

// 序列化
String json = KafkaMessageUtils.serialize(order);

// 反序列化
Order order = KafkaMessageUtils.deserialize(json, Order.class);

// 反序列化 KafkaMessage
KafkaMessage<Order> message = KafkaMessageUtils.deserializeMessage(json, Order.class);
```

### RetryTemplate

```java
// 创建重试模板
RetryTemplate<String> retryTemplate = RetryTemplate.<String>builder()
    .maxRetries(5)
    .initialDelayMs(500)
    .multiplier(2.0)
    .maxDelayMs(5000)
    .retryIf(e -> e instanceof RetryableException)
    .build();

// 执行操作
String result = retryTemplate.execute(() -> {
    return performOperation();
});
```

## 错误处理

### 发送失败处理

```java
enhancedKafkaTemplate.send("topic", data, bizKey, null)
    .whenComplete((result, throwable) -> {
        if (throwable != null) {
            // 记录错误
            log.error("Send failed", throwable);
            
            // 重试
            retrySend(data, bizKey);
            
            // 或发送到备用队列
            sendToFallbackQueue(data);
        }
    });
```

### 消费失败处理

BaseKafkaConsumer 自动处理：
- `RetryableException`: 自动重试
- 其他异常: 发送到 DLT
- 超过最大重试次数: 发送到 DLT

## 性能优化

### 1. 批量发送

使用 `sendBatch()` 方法批量发送消息，提高吞吐量。

### 2. 异步发送

使用异步发送，不阻塞主线程：

```java
// ✅ 推荐
CompletableFuture<SendResult> future = enhancedKafkaTemplate.send(...);
future.whenComplete(...);

// ❌ 避免
SendResult result = enhancedKafkaTemplate.send(...).get();
```

### 3. 连接池配置

```yaml
spring:
  kafka:
    producer:
      properties:
        batch.size: 16384
        linger.ms: 1
        compression.type: snappy
```

### 4. 消费者并发

```yaml
spring:
  kafka:
    listener:
      concurrency: 3  # 根据实际情况调整
```

## 常见问题

### Q1: 消息发送失败怎么办？

A: 检查以下几点：
1. Kafka 连接配置是否正确
2. 主题是否存在
3. 网络连接是否正常
4. 查看日志中的详细错误信息

### Q2: 消费者不消费消息？

A: 检查以下几点：
1. 消费者组ID配置
2. 偏移量重置策略
3. 主题分区分配
4. 消费者日志

### Q3: 如何监控消息发送/消费？

A: 启用监控配置：

```yaml
kafka:
  monitoring:
    enabled: true
```

然后通过 Micrometer 查看指标：
- `kafka.message.send` - 发送计数
- `kafka.message.consume` - 消费计数
- `kafka.message.processing.time` - 处理时长

### Q4: 如何实现分布式追踪？

A: 启用追踪配置：

```yaml
kafka:
  trace:
    enabled: true
```

traceId 和 spanId 会自动传播，并在日志中显示。

## 更多示例

查看项目中的示例代码：
- `OrderService` - 消息发送示例
- `OrderConsumer` - 消息消费示例
- `UsageExamples` - 完整使用示例

## 技术支持

如有问题，请查看：
- README.md - 项目文档
- 代码注释 - 详细的 JavaDoc
- 示例代码 - 实际使用案例

