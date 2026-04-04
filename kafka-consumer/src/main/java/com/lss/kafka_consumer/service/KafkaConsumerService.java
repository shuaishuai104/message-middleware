package com.lss.kafka_consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Kafka 消费者服务
 * 演示 Kafka Consumer 的各种特性：
 * 1. 基础消息监听
 * 2. 手动提交偏移量
 * 3. 批量消费
 * 4. 带消息头的消息消费
 * 5. 异常处理与重试
 * 6. 消费进度记录
 */
@Slf4j
@Service
public class KafkaConsumerService {

    // ==================== 基础消息监听 ====================

    /**
     * 最基础的消费方式 - 自动提交偏移量
     * 注意：enable-auto-commit=true 时使用
     */
    @KafkaListener(topics = "test-topic", groupId = "${spring.kafka.group-id}")
    public void consumeBasic(ConsumerRecord<String, String> record) {
        log.info("【基础消费】topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }

    // ==================== 手动提交偏移量 ====================

    /**
     * 手动提交偏移量 - 更可靠的控制
     * 需要配置 ackMode = MANUAL
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-manual",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithManualAck(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.info("【手动提交】topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());

            // 业务处理...

            // 处理完成后手动提交偏移量
            acknowledgment.acknowledge();

            log.debug("【手动提交】偏移量已提交: partition={}, offset={}",
                    record.partition(), record.offset());
        } catch (Exception e) {
            log.error("【消费失败】处理消息异常", e);
            // 不提交，让消息稍后重新消费
            throw e;
        }
    }

    /**
     * 带分区信息的监听
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-partition",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithPartitionInfo(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        TopicPartition partitionInfo = new TopicPartition(record.topic(), record.partition());

        log.info("【分区消费】topic={}, partition={}, offset={}, key={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key());

        // 业务处理...

        acknowledgment.acknowledge();
    }

    // ==================== 批量消费 ====================

    /**
     * 批量消费 - 一次拉取多条消息
     * 需要配置 batch = true 和 ackMode = BATCH
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-batch",
            containerFactory = "batchKafkaListenerContainerFactory",
            batch = "true"
    )
    public void consumeBatch(List<ConsumerRecord<String, String>> records, Acknowledgment acknowledgment) {
        log.info("【批量消费】收到 {} 条消息", records.size());

        for (ConsumerRecord<String, String> record : records) {
            log.debug("【批量消费-单条】topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());

            // 业务处理...
        }

        // 批量提交所有偏移量
        acknowledgment.acknowledge();

        log.info("【批量消费】已提交 {} 条消息的偏移量", records.size());
    }

    // ==================== 带消息头的消费 ====================

    /**
     * 带消息头的消费 - 用于追踪
     * 从 ConsumerRecord 中提取消息头
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-headers",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithHeaders(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        // 从消息头中提取追踪信息
        String traceId = extractHeader(record, "trace-id");
        String spanId = extractHeader(record, "span-id");
        String contentType = extractHeader(record, "content-type");

        log.info("【带头消费】partition={}, offset={}, traceId={}, spanId={}, contentType={}, key={}, value={}",
                record.partition(), record.offset(), traceId, spanId, contentType, record.key(), record.value());

        // 可以通过traceId做链路追踪

        acknowledgment.acknowledge();
    }

    /**
     * 从 ConsumerRecord 中提取指定名称的消息头
     */
    private String extractHeader(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    // ==================== 手动管理偏移量 ====================

    /**
     * 手动管理偏移量 - 完全控制消费进度
     * 适用于需要严格控制消息处理顺序的场景
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-manual-offset",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithManualOffset(
            ConsumerRecord<String, String> record,
            Consumer<?, ?> consumer,
            Acknowledgment acknowledgment) {

        try {
            log.info("【手动Offset】topic={}, partition={}, offset={}, key={}, value={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    record.value());

            // 业务处理...

            // 手动提交特定分区的偏移量
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
            consumer.commitSync(Map.of(topicPartition, offsetAndMetadata));

            acknowledgment.acknowledge();

            log.debug("【手动Offset】已提交 partition={}, offset={}",
                    record.partition(), record.offset() + 1);
        } catch (Exception e) {
            log.error("【消费异常】", e);
            throw e;
        }
    }

    // ==================== 多主题监听 ====================

    /**
     * 多主题监听 - 使用正则匹配
     */
    @KafkaListener(
            topicPattern = "test-.*",
            groupId = "${spring.kafka.group-id}-multi"
    )
    public void consumeMultiTopic(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("【多主题消费】topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());

        acknowledgment.acknowledge();
    }

    // ==================== 过滤消息头消费 ====================

    /**
     * 过滤特定条件的消息
     */
    @KafkaListener(
            topics = "test-topic",
            groupId = "${spring.kafka.group-id}-filter",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeWithFilter(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment) {

        // 模拟过滤逻辑
        if (record.value() != null && record.value().contains("BLOCK")) {
            log.warn("【消息过滤】跳过被屏蔽的消息: key={}", record.key());
            acknowledgment.acknowledge(); // 跳过但不处理
            return;
        }

        log.info("【正常消费】topic={}, partition={}, offset={}, key={}, value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());

        // 业务处理...

        acknowledgment.acknowledge();
    }
}
