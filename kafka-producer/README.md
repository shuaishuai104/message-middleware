# Kafka Producer 项目

Spring Boot Kafka 生产者示例项目，演示 Kafka Producer 的全部常用特性。

## 功能特性

| 特性 | 说明 | 配置参数 |
|------|------|----------|
| 同步发送 | 阻塞等待 Broker 确认 | `acks=all` |
| 异步发送 | 回调通知，不阻塞 | `KafkaTemplate.send().whenComplete()` |
| 批量发送 | 依赖 `linger.ms` + `batch.size` | `batch-size`, `linger-ms` |
| 压缩发送 | 支持 gzip/snappy/lz4/zstd | `compression-type` |
| 幂等发送 | 保证 Exactly-Once 语义 | `enable-idempotence=true` |
| 事务发送 | 原子性写入多分区 | `transaction-id-prefix` |
| 分区路由 | 自定义分区策略 | `partitioner-class` |
| 消息头 | 传递追踪元数据 | `RecordHeader` |
| 拦截器 | 发送前后处理 | `ProducerInterceptor` |

## 快速开始

### 1. 启动 Kafka 集群

```bash
# 在项目根目录执行
docker-compose up -d

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

服务启动在 `http://localhost:8081`

### 4. 测试发送消息

```bash
# 同步发送
curl -X POST http://localhost:8081/api/kafka/sync \
  -H "Content-Type: application/json" \
  -d '{"key":"order-001","value":"测试消息内容"}'

# 异步发送
curl -X POST http://localhost:8081/api/kafka/async \
  -H "Content-Type: application/json" \
  -d '{"key":"order-002","value":"异步消息"}'

# 带消息头发送
curl -X POST http://localhost:8081/api/kafka/with-headers \
  -H "Content-Type: application/json" \
  -d '{"key":"order-003","value":"带头消息"}'

# 批量发送
curl -X POST http://localhost:8081/api/kafka/batch \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"key":"k1","value":"v1"},{"key":"k2","value":"v2"}]}'

# 事务发送
curl -X POST http://localhost:8081/api/kafka/transaction \
  -H "Content-Type: application/json" \
  -d '{"key1":"tx-1","value1":"事务消息1","key2":"tx-2","value2":"事务消息2"}'
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/kafka/sync` | 同步发送 |
| POST | `/api/kafka/sync/partition/{partition}` | 发送到指定分区 |
| POST | `/api/kafka/async` | 异步发送 |
| POST | `/api/kafka/with-headers` | 带消息头发送 |
| POST | `/api/kafka/batch` | 批量发送 |
| POST | `/api/kafka/transaction` | 事务发送 |
| POST | `/api/kafka/oneway` | Fire-and-Forget |

## 核心配置说明

```yaml
spring:
  kafka:
    # 确认机制：0/1/all
    acks: all

    # 重试次数
    retries: 3

    # 批处理大小（字节）
    batch-size: 16384

    # 批次等待时间（毫秒）
    linger-ms: 10

    # 压缩类型：none/gzip/snappy/lz4/zstd
    compression-type: snappy

    # 启用幂等性
    enable-idempotence: true

    # 事务ID前缀（启用事务时配置）
    # transaction-id-prefix: tx-
```

## 项目结构

```
kafka-producer/
├── src/main/java/com/lss/kafka_producer/
│   ├── KafkaProducerApplication.java      # 启动类
│   ├── config/
│   │   └── KafkaProducerConfig.java       # Producer配置
│   ├── service/
│   │   └── KafkaMessageService.java       # 消息发送服务
│   ├── controller/
│   │   └── MessageController.java         # REST接口
│   ├── partitioner/
│   │   └── CustomPartitioner.java         # 自定义分区器
│   └── interceptor/
│       └── CustomProducerInterceptor.java  # 生产者拦截器
└── src/main/resources/
    └── application.yml                    # 配置文件
```
