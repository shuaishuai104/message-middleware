package com.lss.kafka_consumer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 自定义消费者拦截器
 *
 * 使用场景：
 * 1. 消息过滤 - 根据条件过滤不需处理的消息
 * 2. 消息转换 - 对消息进行预处理
 * 3. 监控统计 - 记录消费数量、耗时等
 * 4. 链路追踪 - 提取trace-id等追踪信息
 */
@Slf4j
public class CustomConsumerInterceptor implements ConsumerInterceptor<String, String> {

    /**
     * 消费前拦截 - 可以修改消息或过滤
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long startTime = System.currentTimeMillis();
        int filteredCount = 0;

        for (ConsumerRecord<String, String> record : records) {
            // 消息过滤示例
            if (record.value() != null && record.value().contains("BLOCK")) {
                log.warn("【拦截器-过滤】消息被过滤: topic={}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());
                filteredCount++;
            }

            // 提取追踪头
            Header traceHeader = record.headers().lastHeader("trace-id");
            if (traceHeader != null) {
                String traceId = new String(traceHeader.value(), StandardCharsets.UTF_8);
                log.debug("【拦截器-追踪】traceId={}, offset={}", traceId, record.offset());
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log.info("【拦截器-消费前】共 {} 条消息, 过滤 {} 条, 耗时 {}ms",
                records.count(), filteredCount, elapsed);

        return records;
    }

    /**
     * 消费后拦截 - 提交偏移量前的最后处理机会
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.info("【拦截器-提交偏移量】提交的分区数: {}", offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            log.debug("【拦截器-提交偏移量】partition={}, offset={}, metadata={}",
                    entry.getKey().partition(),
                    entry.getValue().offset(),
                    entry.getValue().metadata());
        }
    }

    @Override
    public void close() {
        log.info("【拦截器关闭】");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("【拦截器配置】configs={}", configs);
    }
}
