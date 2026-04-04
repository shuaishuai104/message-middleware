package com.lss.kafka_consumer.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Consumer 管理控制器
 * 提供消费者组状态查询、手动提交偏移量等功能
 */
@Slf4j
@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.group-id}")
    private String groupId;

    private final ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory;

    public ConsumerController(ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory) {
        this.kafkaListenerContainerFactory = kafkaListenerContainerFactory;
    }

    /**
     * GET /api/consumer/status
     * 查询消费者组状态
     */
    @GetMapping("/status")
    public Map<String, Object> getConsumerStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("groupId", groupId);
        status.put("bootstrapServers", bootstrapServers);

        try (AdminClient adminClient = createAdminClient()) {
            // 获取消费者组 offsets
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get();

            status.put("partitionsCount", offsets.size());
            status.put("partitions", offsets.entrySet().stream()
                    .map(e -> Map.of(
                            "topic", e.getKey().topic(),
                            "partition", e.getKey().partition(),
                            "offset", e.getValue().offset(),
                            "metadata", e.getValue().metadata() != null ? e.getValue().metadata() : ""
                    ))
                    .toList());

            status.put("success", true);
        } catch (ExecutionException | InterruptedException e) {
            log.error("查询消费者状态失败", e);
            status.put("success", false);
            status.put("error", e.getMessage());
        }

        return status;
    }

    /**
     * POST /api/consumer/seek
     * 重置消费位置到指定offset
     */
    @PostMapping("/seek")
    public Map<String, Object> seekToOffset(
            @RequestParam String topic,
            @RequestParam int partition,
            @RequestParam long offset) {

        Map<String, Object> result = new HashMap<>();
        result.put("topic", topic);
        result.put("partition", partition);
        result.put("targetOffset", offset);

        try (AdminClient adminClient = createAdminClient()) {
            // 创建 AdminClient 修改 offset
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);

            adminClient.alterConsumerGroupOffsets(
                    groupId,
                    Map.of(topicPartition, offsetAndMetadata)
            ).all().get();

            result.put("success", true);
            result.put("message", "重置偏移量成功");
        } catch (ExecutionException | InterruptedException e) {
            log.error("重置偏移量失败", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * GET /api/consumer/lag
     * 查询消费 lag（滞后量）
     */
    @GetMapping("/lag")
    public Map<String, Object> getConsumerLag(
            @RequestParam(defaultValue = "test-topic") String topic) {

        Map<String, Object> result = new HashMap<>();
        result.put("topic", topic);
        result.put("groupId", groupId);

        try (AdminClient adminClient = createAdminClient()) {
            // 获取消费者组 offsets
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = offsetsResult.partitionsToOffsetAndMetadata().get();

            // 获取 topic 的分区信息
            Set<TopicPartition> topicPartitions = consumerOffsets.keySet().stream()
                    .filter(tp -> tp.topic().equals(topic))
                    .collect(java.util.stream.Collectors.toSet());

            // 获取 end offsets（最新消息的offset）
            Map<TopicPartition, Long> endOffsets = adminClient.listOffsets(
                    topicPartitions.stream()
                            .collect(java.util.stream.Collectors.toMap(
                                    tp -> tp,
                                    tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()
                            ))
            ).all().get().entrySet().stream()
                    .collect(java.util.stream.Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().offset()
                    ));

            // 计算 lag
            long totalLag = 0;
            for (TopicPartition tp : topicPartitions) {
                long consumerOffset = consumerOffsets.get(tp).offset();
                long endOffset = endOffsets.getOrDefault(tp, consumerOffset);
                long lag = endOffset - consumerOffset;
                totalLag += lag;
            }

            result.put("totalLag", totalLag);
            result.put("success", true);
        } catch (ExecutionException | InterruptedException e) {
            log.error("查询消费 lag 失败", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * 创建 AdminClient
     */
    private AdminClient createAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
        return AdminClient.create(props);
    }
}
