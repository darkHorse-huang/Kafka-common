# Kafka Common Library

一个功能完善的 Kafka 通用库，提供了消息生产、消费、监控、链路追踪等完整功能。

## 📋 目录

- [项目介绍](#项目介绍)
- [快速开始](#快速开始)
- [API 文档](#api-文档)
- [配置说明](#配置说明)
- [最佳实践](#最佳实践)
- [示例代码](#示例代码)
- [性能优化](#性能优化)
- [故障排查](#故障排查)
- [相关文档](#相关文档)

## 🚀 项目介绍

Kafka Common 是一个基于 Spring Boot 和 Spring Kafka 的通用库，提供了以下核心功能：

### 核心特性

- ✅ **增强的消息发送模板** - `EnhancedKafkaTemplate` 提供自动消息包装、头信息管理、监控集成
- ✅ **基础消费者抽象类** - `BaseKafkaConsumer` 实现模板方法模式，提供自动重试和死信队列处理
- ✅ **分布式链路追踪** - 自动传播 traceId 和 spanId，集成 MDC 支持日志关联
- ✅ **监控指标** - 集成 Micrometer，提供生产/消费消息计数、处理时长、队列大小等指标
- ✅ **自动配置** - Spring Boot 自动配置，开箱即用
- ✅ **注解驱动** - `@KafkaConsumer` 注解支持动态注册消费者
- ✅ **工具类** - 消息序列化/反序列化、重试模板等实用工具

### 技术栈

- Java 1.8+
- Spring Boot 2.7.0
- Spring Kafka 2.8.11
- Micrometer (可选)
- Jackson (JSON 序列化)
- Lombok

## 🏃 快速开始

### 1. 添加依赖

在 `pom.xml` 中添加依赖：

```xml
<dependency>
    <groupId>com.company</groupId>
    <artifactId>kafka-common</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. 配置 Kafka

在 `application.yml` 中配置 Kafka：

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

### 3. 使用 EnhancedKafkaTemplate 发送消息

```java
@Service
@RequiredArgsConstructor
public class OrderService {
    private final EnhancedKafkaTemplate enhancedKafkaTemplate;

    public void sendOrder(Order order) {
        Map<String, String> headers = new HashMap<>();
        headers.put("orderType", "PREMIUM");
        
        enhancedKafkaTemplate.send(
            "order-events",
            order,
            order.getOrderId(),
            headers
        ).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send order", ex);
            } else {
                log.info("Order sent: offset={}", result.getRecordMetadata().offset());
            }
        });
    }
}
```

### 4. 继承 BaseKafkaConsumer 消费消息

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

## 📚 API 文档

### EnhancedKafkaTemplate

增强的 Kafka 消息发送模板，提供以下方法：

#### send()

发送单条消息：

```java
CompletableFuture<SendResult<String, Object>> send(
    String topic,
    T data,
    String bizKey,
    Map<String, String> headers
)
```

**参数说明：**
- `topic`: Kafka 主题名称
- `data`: 业务数据对象
- `bizKey`: 业务键（用于分区路由）
- `headers`: 额外的消息头信息

**返回值：** `CompletableFuture<SendResult<String, Object>>`

#### sendTransactional()

事务性发送消息：

```java
void sendTransactional(
    String topic,
    T data,
    String bizKey,
    Runnable businessLogic
)
```

**使用场景：** 需要在事务中执行业务逻辑并发送消息

#### sendBatch()

批量发送消息：

```java
void sendBatch(
    String topic,
    List<T> dataList,
    String bizKeyPrefix
)
```

### BaseKafkaConsumer

基础消费者抽象类，子类需要实现：

#### doProcess()

实现业务逻辑处理：

```java
protected abstract void doProcess(T data, Map<String, String> headers) throws Exception
```

**异常处理：**
- 抛出 `RetryableException`：会触发自动重试
- 抛出其他异常：会发送到死信队列

#### 受保护方法

- `parseMessage()`: 解析消息（已实现）
- `getHeader()`: 获取消息头（已实现）
- `handleDlt()`: 死信队列处理（已实现）
- `handleRetry()`: 重试处理（已实现）

### KafkaMessageUtils

消息工具类，提供静态方法：

```java
// 从消息构建头信息
Map<String, String> buildHeaders(KafkaMessage<?> message)

// 从 ConsumerRecord 提取头信息
Map<String, String> extractHeaders(ConsumerRecord<String, Object> record)

// 序列化对象
String serialize(Object obj)

// 反序列化对象
<T> T deserialize(String json, Class<T> clazz)
```

### RetryTemplate

重试模板，支持指数退避：

```java
RetryTemplate<String> retryTemplate = RetryTemplate.<String>builder()
    .maxRetries(5)
    .initialDelayMs(500)
    .multiplier(2.0)
    .maxDelayMs(5000)
    .retryIf(e -> e instanceof RetryableException)
    .build();

String result = retryTemplate.execute(() -> {
    return someOperation();
});
```

## ⚙️ 配置说明

### 生产者配置

```yaml
kafka:
  producer:
    transaction-id-prefix: kafka-tx-  # 事务ID前缀
    retries: 3                          # 重试次数
    acks: all                           # 确认模式
    batch-size: 16384                   # 批次大小
    linger-ms: 1                        # 批次等待时间
    buffer-memory: 33554432            # 缓冲区内存
    compression-type: snappy            # 压缩类型
    enable-idempotence: true            # 启用幂等性
```

### 消费者配置

```yaml
kafka:
  consumer:
    group-id: kafka-common-group        # 消费者组ID
    auto-offset-reset: earliest         # 偏移量重置策略
    enable-auto-commit: false           # 是否自动提交
    max-poll-records: 500               # 最大拉取记录数
    concurrency: 3                      # 并发消费者线程数
    session-timeout-ms: 30000          # 会话超时时间
    heartbeat-interval-ms: 3000        # 心跳间隔
```

### 监控配置

```yaml
kafka:
  monitoring:
    enabled: true                        # 启用监控
    collection-interval: 60             # 指标收集间隔（秒）
    detailed-metrics: true              # 详细指标
    tags:
      application: ${spring.application.name}
      environment: ${spring.profiles.active}
```

### 链路追踪配置

```yaml
kafka:
  trace:
    enabled: true                       # 启用链路追踪
    trace-id-header: traceId           # Trace ID 头名称
    span-id-header: spanId              # Span ID 头名称
    propagation-enabled: true          # 启用传播
    sampling-rate: 1.0                 # 采样率（0.0-1.0）
```

### 重试配置

```yaml
kafka:
  retry:
    max-attempts: 3                     # 最大重试次数
    initial-delay-ms: 1000              # 初始延迟（毫秒）
    multiplier: 2.0                     # 退避倍数
    max-delay-ms: 10000                 # 最大延迟（毫秒）
```

### 死信队列配置

```yaml
kafka:
  dlt:
    enabled: true                       # 启用死信队列
    topic-suffix: -dlt                 # DLT 主题后缀
    retry-topic-suffix: -retry         # 重试主题后缀
    max-retry-attempts: 3              # 最大重试次数
```

## 💡 最佳实践

### 1. 消息发送

**✅ 推荐做法：**

```java
// 使用异步发送，不阻塞主线程
CompletableFuture<SendResult<String, Object>> future = 
    enhancedKafkaTemplate.send(topic, data, bizKey, headers);

future.whenComplete((result, ex) -> {
    if (ex != null) {
        // 处理发送失败
        handleSendFailure(ex);
    } else {
        // 记录成功日志
        log.info("Message sent: offset={}", result.getRecordMetadata().offset());
    }
});
```

**❌ 避免做法：**

```java
// 不要同步等待，会阻塞线程
SendResult result = enhancedKafkaTemplate.send(...).get();
```

### 2. 事务性发送

**✅ 推荐做法：**

```java
// 在事务中执行业务逻辑和发送消息
enhancedKafkaTemplate.sendTransactional(
    topic,
    data,
    bizKey,
    () -> {
        // 业务逻辑：保存到数据库
        orderRepository.save(order);
        // 消息发送会自动包含在事务中
    }
);
```

### 3. 消费者实现

**✅ 推荐做法：**

```java
@Override
protected void doProcess(Order data, Map<String, String> headers) throws Exception {
    // 1. 参数验证
    validateOrder(data);
    
    // 2. 业务处理
    processOrder(data);
    
    // 3. 对于临时性错误，抛出 RetryableException
    if (isTemporaryError()) {
        throw new RetryableException("Temporary error, will retry");
    }
}
```

**❌ 避免做法：**

```java
// 不要在 doProcess 中手动处理重试
// BaseKafkaConsumer 已经提供了自动重试机制
```

### 4. 异常处理

**异常类型：**

- `RetryableException`: 可重试异常，会自动重试
- `BusinessException`: 业务异常，直接发送到 DLT
- `MessageParseException`: 消息解析异常，发送到 DLT
- `MessageSendException`: 消息发送异常

**使用建议：**

```java
try {
    // 业务逻辑
} catch (TemporaryException e) {
    // 临时性错误，可以重试
    throw new RetryableException("Temporary error", e);
} catch (ValidationException e) {
    // 验证错误，不应该重试
    throw new BusinessException("Validation failed", e);
}
```

### 5. 监控和追踪

**启用监控：**

```yaml
kafka:
  monitoring:
    enabled: true
```

**启用追踪：**

```yaml
kafka:
  trace:
    enabled: true
```

**日志配置（包含 traceId）：**

```yaml
logging:
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level [%X{traceId:-}] %logger{36} - %msg%n"
```

### 6. 性能优化

**生产者优化：**

- 启用批量发送：`batch-size: 16384`
- 使用压缩：`compression-type: snappy`
- 启用幂等性：`enable-idempotence: true`

**消费者优化：**

- 调整并发数：`concurrency: 3`
- 批量拉取：`max-poll-records: 500`
- 手动提交：`enable-auto-commit: false`

## 📝 示例代码

### 完整示例

参考项目中的示例代码：

- `OrderService`: 展示如何使用 `EnhancedKafkaTemplate`
- `OrderConsumer`: 展示如何继承 `BaseKafkaConsumer`
- `UsageExamples`: 完整的使用示例集合
- `application-example.yml`: 完整的配置示例

### 使用 @KafkaConsumer 注解

```java
@Component
public class OrderService {
    
    @KafkaConsumer(
        topic = "order-events",
        groupId = "order-service",
        concurrency = 3,
        batch = false,
        ackMode = "MANUAL_IMMEDIATE"
    )
    public void consumeOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        // 处理消息
        processOrder(record);
        ack.acknowledge();
    }
}
```

### 使用工具类

```java
// 使用 KafkaMessageUtils
Map<String, String> headers = KafkaMessageUtils.extractHeaders(record);
Order order = KafkaMessageUtils.deserialize(record.value().toString(), Order.class);

// 使用 RetryTemplate
RetryTemplate<String> retryTemplate = RetryTemplate.<String>builder()
    .maxRetries(5)
    .initialDelayMs(500)
    .multiplier(2.0)
    .build();

String result = retryTemplate.execute(() -> performOperation());
```

## ⚡ 性能优化

### 1. 对象复用

库内部已经优化了对象复用：
- Timer.Builder 和 Counter.Builder 缓存
- TypeFactory 缓存
- ObjectMapper 单例

### 2. 批量操作

使用批量发送提高吞吐量：

```java
// 批量发送
enhancedKafkaTemplate.sendBatch("topic", dataList, "prefix");
```

### 3. 异步处理

使用异步发送，不阻塞主线程：

```java
// ✅ 推荐：异步发送
CompletableFuture<SendResult> future = enhancedKafkaTemplate.send(...);
future.whenComplete(...);

// ❌ 避免：同步等待
SendResult result = enhancedKafkaTemplate.send(...).get();
```

### 4. 连接池配置

优化 Kafka 连接池配置：

```yaml
spring:
  kafka:
    producer:
      properties:
        batch.size: 16384
        linger.ms: 1
        compression.type: snappy
```

## 🔧 故障排查

### 常见问题

1. **消息发送失败**
   - 检查 Kafka 连接配置
   - 检查主题是否存在
   - 查看日志中的异常信息
   - 检查网络连接

2. **消费者不消费消息**
   - 检查消费者组ID配置
   - 检查偏移量重置策略
   - 查看消费者日志
   - 检查主题分区分配

3. **死信队列消息过多**
   - 检查业务逻辑是否有问题
   - 调整重试次数和延迟
   - 查看 DLT 主题中的错误信息
   - 分析异常模式

4. **性能问题**
   - 检查批量大小配置
   - 调整并发消费者数量
   - 检查网络延迟
   - 监控 Kafka 集群状态

### 日志级别

建议的日志级别配置：

```yaml
logging:
  level:
    com.company.kafka: DEBUG  # 开发环境
    # com.company.kafka: INFO   # 生产环境
```

## 📚 相关文档

- [API_DOCUMENTATION.md](API_DOCUMENTATION.md) - 完整的 API 文档
- [USAGE_GUIDE.md](USAGE_GUIDE.md) - 详细的使用指南
- [示例代码](src/main/java/com/kafka/example/) - 完整的使用示例

## 📄 许可证

本项目采用 MIT 许可证。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📞 联系方式

如有问题，请联系项目维护者。

## 🔧 故障排查

### 常见问题

1. **消息发送失败**
   - 检查 Kafka 连接配置
   - 检查主题是否存在
   - 查看日志中的异常信息

2. **消费者不消费消息**
   - 检查消费者组ID配置
   - 检查偏移量重置策略
   - 查看消费者日志

3. **死信队列消息过多**
   - 检查业务逻辑是否有问题
   - 调整重试次数和延迟
   - 查看 DLT 主题中的错误信息

## 📄 许可证

本项目采用 MIT 许可证。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📞 联系方式

如有问题，请联系项目维护者。

