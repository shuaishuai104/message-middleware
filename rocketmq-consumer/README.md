# RocketMQ Consumer 项目

Spring Boot RocketMQ 消费者示例项目，演示 RocketMQ Consumer 的全部常用特性。

## 功能特性

| 特性 | 说明 | 对比 Kafka |
|------|------|------------|
| 并发消费 | 多线程并发处理消息 | Kafka concurrency |
| 顺序消费 | 同一队列消息顺序处理 | Kafka preserve partition order |
| 手动确认 | 处理完后手动确认 | Kafka MANUAL ack |
| 批量消费 | 一次处理多条消息 | Kafka batch listener |
| Tag 过滤 | 按 Tag 过滤消息 | Kafka header filter |
| SQL92 过滤 | 支持 SQL92 语法过滤 | - |
| 广播模式 | 一消息被所有消费者消费 | Kafka broadcast |
| 消费重试 | 失败消息自动重试 | Kafka retry |
| 消费组 | 集群消费，负载均衡 | Kafka Consumer Group |
| 拦截器 | 消费前后处理 | Kafka ConsumerInterceptor |

## 快速开始

### 1. 启动 RocketMQ 集群

```bash
# 在项目根目录执行
docker-compose -f docker-compose-rocketmq.yml up -d

# 等待服务启动（约30秒）
```

### 2. 修改配置

编辑 `src/main/resources/application.yml`，确保 `name-server` 与实际环境匹配：

```yaml
spring:
  rocketmq:
    name-server: localhost:9876
```

### 3. 运行项目

```bash
mvn spring-boot:run
```

服务启动在 `http://localhost:8080`

### 4. 先启动生产者发送消息

参考 [rocketmq-producer README](../rocketmq-producer/README.md) 发送测试消息

### 5. 查看消费状态

```bash
# 查看消费者组状态
curl http://localhost:8080/api/consumer/status

# 查看消费 lag
curl "http://localhost:8080/api/consumer/lag?topic=test-topic"

# 查看订阅关系
curl http://localhost:8080/api/consumer/subscriptions
```

## 消费模式

### 1. 并发消费（默认）

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group",
    consumeMode = ConsumeMode.CONCURRENTLY
)
public void consume(String message, MessageExt msgExt) {
    // 多线程并发消费
}
```

### 2. 顺序消费

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group-orderly",
    selectorExpression = "order-step",
    consumeMode = ConsumeMode.ORDERLY
)
public void consumeOrderly(String message, MessageExt msgExt) {
    // 同一队列消息顺序消费
}
```

### 3. 手动确认

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group-manual",
    autoAck = false
)
public void consumeManual(String message, MessageExt msgExt) {
    try {
        // 业务处理
        // 手动确认（框架自动处理）
    } catch (Exception e) {
        throw e; // 抛出异常触发重试
    }
}
```

### 4. Tag 过滤消费

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group-tag",
    selectorType = SelectorType.TAG,
    selectorExpression = "tag1 || tag2 || tag3"
)
public void consumeTagFilter(String message, MessageExt msgExt) {
    // 只消费 tag1、tag2、tag3 的消息
}
```

### 5. SQL92 过滤消费

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group-sql",
    selectorType = SelectorType.SQL92,
    selectorExpression = "trace-id IS NOT NULL AND priority = 'high'"
)
public void consumeSqlFilter(String message, MessageExt msgExt) {
    // 只消费带有 trace-id 且 priority=high 的消息
}
```

### 6. 广播模式消费

```java
@RocketMQMessageListener(
    topic = "test-topic",
    consumerGroup = "consumer-group-broadcast",
    messageModel = MessageModel.BROADCASTING
)
public void consumeBroadcast(String message, MessageExt msgExt) {
    // 所有消费者都会收到消息
}
```

## 核心配置说明

```yaml
spring:
  rocketmq:
    name-server: localhost:9876
    consumer:
      # 消费者组
      group: rocketmq-consumer-group

      # 消费线程数
      consume-thread-min: 10
      consume-thread-max: 20

      # 拉取批量大小
      pull-batch-size: 32

      # 最大重试次数
      max-retry-times: 3

      # 重试间隔（毫秒）
      retry-interval: 1000

      # 顺序消费失败挂起时间
      suspend-current-party-time: 3000
```

## 消费模式与消息模型

| 模式 | 说明 | 使用场景 |
|------|------|----------|
| `CONCURRENTLY` | 并发消费 | 大部分场景，吞吐量高 |
| `ORDERLY` | 顺序消费 | 订单处理等需要保序场景 |
| `CLUSTERING` | 集群消费（默认） | 一条消息只被一个消费者消费 |
| `BROADCASTING` | 广播消费 | 配置同步、缓存刷新等 |

## RocketMQ 与 Kafka 消费对比

| 概念 | RocketMQ | Kafka |
|------|----------|-------|
| 注解 | @RocketMQMessageListener | @KafkaListener |
| 确认模式 | autoAck = false | AckMode.MANUAL |
| 消费线程 | consumeThreadMin/Max | concurrency |
| 重试机制 | maxReconsumeTimes | DefaultErrorHandler |
| 消费进度 | 存储在 Broker | 存储在 __consumer_offsets |
| 过滤方式 | Tag/SQL92 | Header/Interceptor |
| 广播 | messageModel = BROADCASTING | partition_assignment_strategy |

## 项目结构

```
rocketmq-consumer/
├── src/main/java/com/lss/rocketmq_consumer/
│   ├── RocketMQConsumerApplication.java       # 启动类
│   ├── config/
│   │   └── RocketMQConsumerConfig.java       # Consumer配置
│   ├── service/
│   │   └── RocketMQConsumerService.java      # 消息消费服务
│   ├── controller/
│   │   └── ConsumerController.java           # 管理接口
│   └── interceptor/
│       └── CustomConsumerInterceptor.java     # 消费者拦截器
└── src/main/resources/
    └── application.yml                        # 配置文件
```

## 消费 Lag 监控

消费 Lag = 最新消息 offset - 已消费 offset

- Lag 为 0：消费无滞后
- Lag 持续增长：消费速度 < 生产速度
- Lag 过大：需要增加消费者或队列数

```bash
# 查看各队列 lag
curl "http://localhost:8080/api/consumer/lag?topic=test-topic"
```

## RocketMQ 管理控制台

启动后访问 http://localhost:8082 查看：
- Topic 订阅关系
- 消费者在线状态
- 消费进度和 Lag
- 消息堆积情况
