# Kafka Common API 文档

本文档提供 kafka-common 库的完整 API 文档。

## 目录

- [核心类](#核心类)
- [工具类](#工具类)
- [异常类](#异常类)
- [配置类](#配置类)
- [注解](#注解)

## 核心类

### EnhancedKafkaTemplate

增强的 Kafka 消息发送模板。

#### 方法

##### send()

```java
public <T> CompletableFuture<SendResult<String, Object>> send(
    String topic,
    T data,
    String bizKey,
    Map<String, String> headers)
```

**描述：** 异步发送消息到 Kafka 主题。

**参数：**
- `topic` (String, 必填): 主题名称
- `data` (T, 必填): 业务数据对象
- `bizKey` (String, 必填): 业务键，用于分区路由
- `headers` (Map<String, String>, 可选): 额外的消息头

**返回值：** `CompletableFuture<SendResult<String, Object>>`

**异常：**
- `IllegalArgumentException`: 如果 topic、data 或 bizKey 为 null 或空
- `MessageSendException`: 如果消息准备或发送失败

**示例：**
```java
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

##### sendTransactional()

```java
public <T> void sendTransactional(
    String topic,
    T data,
    String bizKey,
    Runnable businessLogic)
```

**描述：** 在事务中发送消息并执行业务逻辑。

**参数：**
- `topic` (String, 必填): 主题名称
- `data` (T, 必填): 业务数据对象
- `bizKey` (String, 必填): 业务键
- `businessLogic` (Runnable, 可选): 在事务中执行的业务逻辑

**异常：**
- `MessageSendException`: 如果事务失败

**示例：**
```java
enhancedKafkaTemplate.sendTransactional("order-events", order, orderId, () -> {
    orderRepository.save(order);
    inventoryService.reserve(order);
});
```

##### sendBatch()

```java
public <T> void sendBatch(
    String topic,
    List<T> dataList,
    String bizKeyPrefix)
```

**描述：** 批量发送消息。

**参数：**
- `topic` (String, 必填): 主题名称
- `dataList` (List<T>, 必填): 业务数据列表
- `bizKeyPrefix` (String, 必填): 业务键前缀

**示例：**
```java
List<Order> orders = getOrders();
enhancedKafkaTemplate.sendBatch("order-events", orders, "batch-order");
```

### BaseKafkaConsumer

基础 Kafka 消费者抽象类。

#### 抽象方法

##### doProcess()

```java
protected abstract void doProcess(T data, Map<String, String> headers) throws Exception
```

**描述：** 子类必须实现此方法来处理业务逻辑。

**参数：**
- `data` (T): 业务数据对象
- `headers` (Map<String, String>): 消息头信息

**异常：**
- `RetryableException`: 临时错误，会触发重试
- 其他异常: 永久错误，会发送到 DLT

**示例：**
```java
@Override
protected void doProcess(Order data, Map<String, String> headers) throws Exception {
    // 业务逻辑
    processOrder(data);
}
```

#### 受保护方法

##### parseMessage()

```java
protected KafkaMessage<T> parseMessage(ConsumerRecord<String, Object> record) 
    throws MessageParseException
```

**描述：** 解析消息（已实现）。

##### getHeader()

```java
protected String getHeader(ConsumerRecord<String, Object> record, String key)
```

**描述：** 获取消息头值。

##### extractHeaders()

```java
protected Map<String, String> extractHeaders(ConsumerRecord<String, Object> record)
```

**描述：** 提取所有消息头。

##### handleDlt()

```java
protected void handleDlt(ConsumerRecord<String, Object> record, 
                        Exception exception, 
                        String errorCode)
```

**描述：** 处理死信队列（已实现）。

## 工具类

### KafkaMessageUtils

消息工具类，提供静态方法。

#### buildHeaders()

```java
public static Map<String, String> buildHeaders(KafkaMessage<?> message)
```

**描述：** 从 KafkaMessage 构建头信息 Map。

#### extractHeaders()

```java
public static Map<String, String> extractHeaders(ConsumerRecord<String, Object> record)
```

**描述：** 从 ConsumerRecord 提取头信息。

#### serialize()

```java
public static String serialize(Object obj)
```

**描述：** 序列化对象为 JSON 字符串。

#### deserialize()

```java
public static <T> T deserialize(String json, Class<T> clazz)
```

**描述：** 反序列化 JSON 字符串为对象。

### RetryTemplate

重试模板类。

#### execute()

```java
public T execute(Supplier<T> operation) throws Exception
```

**描述：** 执行操作并自动重试。

**参数：**
- `operation` (Supplier<T>): 要执行的操作

**返回值：** 操作结果

**异常：** 如果所有重试都失败，抛出最后一个异常

## 异常类

### KafkaCommonException

基础异常类，所有 Kafka 相关异常的父类。

### MessageParseException

消息解析异常，继承自 `KafkaCommonException`。

### RetryableException

可重试异常，继承自 `KafkaCommonException`。

### BusinessException

业务异常，继承自 `KafkaCommonException`。

### MessageSendException

消息发送异常，继承自 `KafkaCommonException`。

## 配置类

### KafkaProducerConfig

Kafka 生产者配置类。

**配置 Bean：**
- `ProducerFactory<String, Object>`
- `KafkaTemplate<String, Object>`
- `KafkaTransactionManager<String, Object>`
- `ObjectMapper`

### KafkaConsumerConfig

Kafka 消费者配置类。

**配置 Bean：**
- `ConsumerFactory<String, Object>`
- `ConcurrentKafkaListenerContainerFactory<String, Object>`

### KafkaAutoConfiguration

自动配置类。

**配置 Bean：**
- `EnhancedKafkaTemplate`
- `KafkaMetrics` (条件：`kafka.monitoring.enabled=true`)
- `KafkaTraceInterceptor` (条件：`kafka.trace.enabled=true`)

## 注解

### @KafkaConsumer

用于标记 Kafka 消费者方法。

**属性：**
- `topic()` (String, 必填): 消费的主题
- `groupId()` (String, 可选): 消费者组ID
- `concurrency()` (int, 默认1): 并发数
- `batch()` (boolean, 默认false): 是否批量消费
- `batchSize()` (int, 默认10): 批量大小
- `ackMode()` (String, 默认"MANUAL_IMMEDIATE"): 确认模式

**示例：**
```java
@KafkaConsumer(
    topic = "order-events",
    groupId = "order-service",
    concurrency = 3
)
public void consumeOrder(ConsumerRecord<String, Object> record, Acknowledgment ack) {
    // 处理消息
}
```

## 完整示例

查看 `UsageExamples.java` 获取完整的使用示例。

