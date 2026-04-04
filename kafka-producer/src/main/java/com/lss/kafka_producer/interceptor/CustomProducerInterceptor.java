package com.lss.kafka_producer.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/**
 * 自定义生产者拦截器
 *
 * 使用场景：
 * 1. 添加追踪头 - 在发送前添加 trace-id、span-id
 * 2. 修改消息 - 在发送前对消息进行加工处理
 * 3. 统计监控 - 记录发送成功/失败的数量和耗时
 * 4. 消息过滤 - 根据条件决定是否发送
 */
@Slf4j
public class CustomProducerInterceptor implements ProducerInterceptor<String, String> {

    private static final String TRACE_ID_HEADER = "x-trace-id";
    private static final String SEND_TIME_HEADER = "x-send-time";

    /**
     * 发送前拦截 - 可以修改消息、添加头、过滤消息等
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 1. 添加追踪头
        String traceId = UUID.randomUUID().toString();
        long sendTime = System.currentTimeMillis();

        ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                record.value(),
                java.util.List.of(
                        new RecordHeader(TRACE_ID_HEADER, traceId.getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(SEND_TIME_HEADER, String.valueOf(sendTime).getBytes(StandardCharsets.UTF_8))
                )
        );

        log.debug("【拦截器-发送前】topic={}, traceId={}, key={}",
                record.topic(), traceId, record.key());

        // 2. 消息过滤示例（注释掉）
        // if (record.value().contains("BLOCK")) {
        //     log.warn("消息被拦截器过滤: {}", record.value());
        //     return null; // 返回null则不发送
        // }

        return newRecord;
    }

    /**
     * 发送成功后拦截 - 可用于记录发送结果、清理资源等
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("【拦截器-发送失败】topic={}, partition={}, error={}",
                    metadata.topic(), metadata.partition(), exception.getMessage());
        } else {
            // 查找追踪头
            // 注意：这里无法直接访问原始record的头部，只能访问metadata
            log.info("【拦截器-发送成功】topic={}, partition={}, offset={}",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    @Override
    public void close() {
        // 清理资源
        log.info("【拦截器关闭】");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置中读取参数
        log.info("【拦截器配置】configs={}", configs);
    }
}
