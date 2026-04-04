package com.lss.kafka_producer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Kafka 消息服务
 * 演示 Kafka Producer 的各种特性：
 * 1. 同步发送 - 等待确认
 * 2. 异步发送 - 回调通知
 * 3. 带分区key的发送 - 顺序保证
 * 4. 带头的发送 - 追踪信息
 * 5. 事务发送 - 原子性保证
 * 6. 批量发送 - 效率优先
 */
@Slf4j
@Service
public class KafkaMessageService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.topic.name:test-topic}")
    private String topicName;

    public KafkaMessageService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // ==================== 同步发送 ====================

    /**
     * 同步发送消息 - 阻塞等待发送结果
     * 特点：可靠性最高，延迟最大
     *
     * @param key    消息键，用于决定分区
     * @param value  消息值
     * @return 发送结果，包含分区和偏移量
     */
    public SendResult<String, String> sendSync(String key, String value) {
        try {
            // 指定key用于分区
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

            // 同步等待结果，最多等待30秒
            SendResult<String, String> result = future.get(30, TimeUnit.SECONDS);

            RecordMetadata metadata = result.getRecordMetadata();
            log.info("【同步发送成功】topic={}, partition={}, offset={}, key={}",
                    metadata.topic(), metadata.partition(), metadata.offset(), key);

            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("发送被中断", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException("发送失败", e);
        }
    }

    /**
     * 同步发送消息到指定分区
     *
     * @param partition 指定分区号
     * @param value     消息值
     * @return 发送结果
     */
    public SendResult<String, String> sendSyncToPartition(int partition, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, partition, null, value);
            SendResult<String, String> result = kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);

            RecordMetadata metadata = result.getRecordMetadata();
            log.info("【同步发送成功】topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());

            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("发送失败", e);
        }
    }

    // ==================== 异步发送 ====================

    /**
     * 异步发送消息 - 不阻塞，通过回调通知结果
     * 特点：吞吐量高，可靠性依赖回调处理
     *
     * @param key   消息键
     * @param value 消息值
     * @param callback 发送完成后的回调
     */
    public void sendAsync(String key, String value, SendCallback callback) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("【异步发送失败】key={}, error={}", key, ex.getMessage());
                        callback.onError(ex);
                    } else {
                        RecordMetadata metadata = result.getRecordMetadata();
                        log.info("【异步发送成功】topic={}, partition={}, offset={}, key={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), key);
                        callback.onSuccess(result);
                    }
                });
    }

    /**
     * 异步发送消息（无回调）
     *
     * @param key   消息键
     * @param value 消息值
     */
    public void sendAsync(String key, String value) {
        sendAsync(key, value, new SendCallback() {
            @Override
            public void onSuccess(Object result) {
                // 默认空实现
            }

            @Override
            public void onError(Throwable ex) {
                log.error("异步发送异常: {}", ex.getMessage());
            }
        });
    }

    // ==================== 带消息头的发送 ====================

    /**
     * 发送带消息头的消息 - 用于追踪和元数据传递
     *
     * @param key      消息键
     * @param value    消息值
     * @param traceId  追踪ID
     * @param spanId   跨度ID
     * @return 发送结果
     */
    public SendResult<String, String> sendWithHeaders(String key, String value,
                                                       String traceId, String spanId) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,
                null,  // partition (null使用默认分区器)
                key,
                value,
                java.util.List.of(
                        new RecordHeader("trace-id", traceId.getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("span-id", spanId.getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader("content-type", "application/json".getBytes(StandardCharsets.UTF_8))
                )
        );

        try {
            SendResult<String, String> result = kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
            log.info("【带头消息发送成功】topic={}, partition={}, offset={}, traceId={}",
                    metadata.topic(), metadata.partition(), metadata.offset(), traceId);
            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException("发送失败", e);
        }
    }

    // ==================== 批量发送 ====================

    /**
     * 批量发送消息 - 适用于高吞吐场景
     * 注意：批量发送依赖 linger.ms 和 batch.size 配置
     *
     * @param messages 消息列表
     * @return 最后一个发送结果
     */
    public SendResult<String, String> sendBatch(java.util.List<org.apache.kafka.clients.producer.ProducerRecord<String, String>> messages) {
        SendResult<String, String> lastResult = null;

        for (ProducerRecord<String, String> record : messages) {
            try {
                // 使用 CompletableFuture 但不立即等待，让批处理机制优化
                lastResult = kafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("批量发送中某条消息失败: key={}", record.key(), e);
                throw new RuntimeException("批量发送失败", e);
            }
        }

        // 刷新所有待发送消息
        kafkaTemplate.flush();

        if (lastResult != null) {
            RecordMetadata metadata = lastResult.getRecordMetadata();
            log.info("【批量发送完成】共 {} 条消息，最后一条: partition={}, offset={}",
                    messages.size(), metadata.partition(), metadata.offset());
        }

        return lastResult;
    }

    // ==================== 事务发送 ====================

    /**
     * 事务发送 - 保证原子性，要么全部成功要么全部失败
     * 注意：需要配置 transaction-id-prefix
     *
     * @param key          消息键
     * @param value        消息值
     * @param secondKey    第二条消息的键
     * @param secondValue  第二条消息的值
     * @return 发送结果
     */
    @Transactional(transactionManager = "transactionManager")
    public SendResult<String, String> sendTransaction(String key, String value,
                                                       String secondKey, String secondValue) {
        log.info("【开始事务】发送两条消息");

        try {
            // 第一条消息
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topicName, key, value);
            SendResult<String, String> result1 = kafkaTemplate.send(record1).get();

            // 第二条消息 - 如果这里失败，第一条也会回滚
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, secondKey, secondValue);
            SendResult<String, String> result2 = kafkaTemplate.send(record2).get();

            log.info("【事务提交成功】两条消息均发送成功");
            return result2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("事务发送被中断", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("事务发送失败", e);
        }
    }

    // ==================== One-Way 发送（仅发送，不等待） ====================

    /**
     * Fire-and-Forget 发送 - 只管发送，不等待任何确认
     * 特点：最低延迟，最高丢失风险
     * 适用：极不重要的日志或指标
     *
     * @param key   消息键
     * @param value 消息值
     */
    public void sendOneWay(String key, String value) {
        // 使用 sendAsync 且不等待结果
        kafkaTemplate.send(topicName, key, value);
        log.debug("【One-Way发送】已提交，key={}", key);
    }

    // ==================== 发送回调接口 ====================

    public interface SendCallback {
        void onSuccess(Object result);
        void onError(Throwable ex);
    }
}
