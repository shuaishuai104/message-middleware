# Kafka Consumer 项目

Spring Boot Kafka 消费者示例项目，演示 Kafka Consumer 的全部常用特性。

## 功能特性

| 特性 | 说明 | 配置参数 |
|------|------|----------|
| 自动提交 | 自动提交偏移量 | `enable-auto-commit=true` |
| 手动提交 | 处理完消息后手动提交 | `AckMode.MANUAL` |
| 批量消费 | 一次拉取多条消息 | `batchListener=true` |
| 并发消费 | 多线程同时消费 | `concurrency=3` |
| 消息过滤 | 根据条件过滤消息 | 拦截器实现 |
| 事务消费 | 读取已提交事务消息 | `isolation-level=read_committed` |
| 消费组管理 | Group Rebalance | `group-id` |
| 手动Offset | 完全控制消费位置 | `commitSync()` |
| 拦截器 | 消费前后处理 | `ConsumerInterceptor` |

## 快速开始

### 1. 启动 Kafka 集群

```bash
# 在项目根目录执行
docker-compose -f docker-compose-kafka.yml up -d

# 等待服务启动（约30秒）
```

### 2. 修改配置

编辑 `src/main/resources/application.yml`，确保 `bootstrap-servers` 与实际环境匹配：

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
```

### 3. 运行项目

```bash
mvn spring-boot:run
```

服务启动在 `http://localhost:8080`

### 4. 先启动生产者发送消息

参考 [kafka-producer README](../kafka-producer/README.md) 发送测试消息

### 5. 查看消费状态

```bash
# 查看消费者组状态
curl http://localhost:8080/api/consumer/status

# 查看消费 lag
curl "http://localhost:8080/api/consumer/lag?topic=test-topic"

# 重置消费位置
curl -X POST "http://localhost:8080/api/consumer/seek?topic=test-topic&partition=0&offset=0"
```

## 消费模式

### 1. 自动提交模式

```yaml
spring:
  kafka:
    enable-auto-commit: true
```

```java
@KafkaListener(topics = "test-topic")
public void consume(ConsumerRecord<String, String> record) {
    // 消息会自动提交偏移量
}
```

### 2. 手动提交模式（推荐）

```yaml
spring:
  kafka:
    enable-auto-commit: false
```

```java
@KafkaListener(topics = "test-topic", containerFactory = "kafkaListenerContainerFactory")
public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
    try {
        // 业务处理
        acknowledgment.acknowledge(); // 手动提交
    } catch (Exception e) {
        // 不提交，让消息稍后重新消费
        throw e;
    }
}
```

### 3. 批量消费模式

```yaml
spring:
  kafka:
    enable-auto-commit: false
```

```java
@KafkaListener(topics = "test-topic", containerFactory = "batchKafkaListenerContainerFactory")
public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
    for (ConsumerRecord<String, String> record : records) {
        // 业务处理
    }
    acknowledgment.acknowledge(); // 批量提交
}
```

## 核心配置说明

```yaml
spring:
  kafka:
    # 消费者组ID
    group-id: kafka-consumer-group

    # 无初始offset时的起始位置
    # earliest: 从最早消息开始
    # latest: 从最新消息开始
    auto-offset-reset: earliest

    # 是否自动提交
    enable-auto-commit: false

    # 心跳间隔（毫秒）
    heartbeat-interval-ms: 3000

    # 会话超时（毫秒）
    session-timeout-ms: 10000

    # 最大poll间隔（毫秒）
    max-poll-interval-ms: 300000

    # 每次poll的最大消息数
    max-poll-records: 500

    # 消费者线程数（应 <= 分区数）
    concurrency: 3

    # 事务隔离级别
    isolation-level: read_committed
```

## AckMode 详解

| 模式 | 说明 | 使用场景 |
|------|------|----------|
| `BATCH` | poll()的一批消息消费完提交 | 批量处理，吞吐量高 |
| `RECORD` | 每条消息消费后立即提交 | 实时性要求高 |
| `TIME` | 每隔指定时间提交 | 平衡可靠性和性能 |
| `COUNT` | 每消费指定数量提交 | 固定数量处理 |
| `MANUAL` | 调用 acknowledge() 手动提交 | 需要确认后提交 |
| `MANUAL_IMMEDIATE` | 调用 acknowledge() 立即提交 | 最精确的控制 |

## 项目结构

```
kafka-consumer/
├── src/main/java/com/lss/kafka_consumer/
│   ├── KafkaConsumerApplication.java       # 启动类
│   ├── config/
│   │   └── KafkaConsumerConfig.java        # Consumer配置
│   ├── service/
│   │   └── KafkaConsumerService.java       # 消息消费服务
│   ├── controller/
│   │   └── ConsumerController.java         # 管理接口
│   └── interceptor/
│       └── CustomConsumerInterceptor.java  # 消费者拦截器
└── src/main/resources/
    └── application.yml                    # 配置文件
```

## 消费Lag监控

消费 Lag = 最新消息offset - 已消费offset

- Lag 为 0：消费无滞后
- Lag 持续增长：消费速度 < 生产速度
- Lag 过大：需要增加消费者或分区

```bash
# 查看各分区 lag
curl "http://localhost:8080/api/consumer/lag?topic=test-topic"
```
