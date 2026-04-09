# RocketMQ Producer 项目

Spring Boot RocketMQ 生产者示例项目，演示 RocketMQ Producer 的全部常用特性。

## 功能特性

| 特性 | 说明 | 对比 Kafka |
|------|------|------------|
| 同步发送 | 阻塞等待 Broker 确认 | Kafka sync send |
| 异步发送 | 回调通知，不阻塞 | Kafka async send |
| 单向发送 | Fire-and-Forget | Kafka one-way send |
| 批量发送 | 一次发送多条消息 | Kafka batch send |
| 事务发送 | 原子性写入多队列 | Kafka transaction |
| 顺序消息 | 同一业务ID顺序处理 | Kafka partition ordering |
| Tags 过滤 | 消息打标签，消费者按标签过滤 | Kafka header filtering |
| SQL92 过滤 | 支持 SQL92 语法过滤消息 | - |
| 自定义路由 | MessageQueueSelector 路由到指定队列 | Kafka CustomPartitioner |
| 消息追踪 | 开启消息轨迹追踪 | Kafka enable.idempotence |

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

服务启动在 `http://localhost:8081`

### 4. 测试发送消息

```bash
# 同步发送
curl -X POST http://localhost:8081/api/rocketmq/sync \
  -H "Content-Type: application/json" \
  -d '{"key":"order-001","value":"测试消息内容"}'

# 异步发送
curl -X POST http://localhost:8081/api/rocketmq/async \
  -H "Content-Type: application/json" \
  -d '{"key":"order-002","value":"异步消息"}'

# 单向发送
curl -X POST http://localhost:8081/api/rocketmq/oneway \
  -H "Content-Type: application/json" \
  -d '{"value":"单向消息"}'

# 带 Tags 发送
curl -X POST http://localhost:8081/api/rocketmq/with-tags \
  -H "Content-Type: application/json" \
  -d '{"tags":"tag1","key":"k-001","value":"带标签消息"}'

# 批量发送
curl -X POST http://localhost:8081/api/rocketmq/batch \
  -H "Content-Type: application/json" \
  -d '{"messages":["消息1","消息2","消息3"]}'

# 事务发送
curl -X POST http://localhost:8081/api/rocketmq/transaction \
  -H "Content-Type: application/json" \
  -d '{"key1":"tx-1","value1":"事务消息1","key2":"tx-2","value2":"事务消息2"}'

# 顺序发送
curl -X POST http://localhost:8081/api/rocketmq/order \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ORDER-001","steps":["下单","付款","发货","收货"]}'
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/rocketmq/sync` | 同步发送 |
| POST | `/api/rocketmq/sync/queue/{queueId}` | 发送到指定队列 |
| POST | `/api/rocketmq/async` | 异步发送 |
| POST | `/api/rocketmq/oneway` | 单向发送 |
| POST | `/api/rocketmq/with-tags` | 带 Tags 发送 |
| POST | `/api/rocketmq/batch` | 批量发送 |
| POST | `/api/rocketmq/transaction` | 事务发送 |
| POST | `/api/rocketmq/order` | 顺序发送 |

## RocketMQ 与 Kafka 概念对比

| 概念 | RocketMQ | Kafka |
|------|----------|-------|
| 消息模型 | Producer + Topic + Queue | Producer + Topic + Partition |
| 消息过滤 | Tags ( \|\| ) + SQL92 | Headers + Interceptor |
| 发送模式 | sync/async/one-way | sync/async/transaction |
| 消费模式 | CONCURRENTLY / ORDERLY | 单条 / 批量 |
| 消息模型 | CLUSTERING / BROADCASTING | Consumer Group |
| 路由选择 | MessageQueueSelector | Partitioner |
| 事务 | 事务生产者 + TransactionListener | 事务ID |

## 核心配置说明

```yaml
spring:
  rocketmq:
    name-server: localhost:9876
    producer:
      # 生产者组
      group: rocketmq-producer-group

      # 发送超时时间（毫秒）
      send-message-timeout: 30000

      # 压缩消息体阈值（字节）
      compress-message-body-threshold: 4096

      # 最大消息大小（字节）
      max-message-size: 4194304

      # 同步发送失败重试次数
      retry-times-when-send-failed: 3

      # 异步发送失败重试次数
      retry-times-when-send-async-failed: 2

      # 开启消息追踪
      enable-msg-trace: true
```

## 项目结构

```
rocketmq-producer/
├── src/main/java/com/lss/rocketmq_producer/
│   ├── RocketMQProducerApplication.java      # 启动类
│   ├── config/
│   │   └── RocketMQProducerConfig.java      # Producer配置
│   ├── service/
│   │   └── RocketMQMessageService.java       # 消息发送服务
│   ├── controller/
│   │   └── MessageController.java            # REST接口
│   ├── partitioner/
│   │   └── CustomMessageQueueSelector.java   # 自定义队列选择器
│   └── interceptor/
│       └── CustomProducerInterceptor.java     # 生产者拦截器
└── src/main/resources/
    └── application.yml                       # 配置文件
```

## RocketMQ 管理控制台

启动后访问 http://localhost:8082 查看 RocketMQ 管理界面，可查看：
- Topic 列表和状态
- 消费者组信息
- 生产者连接状态
- 消息统计
