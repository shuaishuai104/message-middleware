package com.lss.kafka_producer.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * 自定义分区器示例
 * 演示如何根据消息内容实现自定义分区策略
 *
 * 使用场景：
 * 1. 灰度发布 - 将特定用户路由到特定分区
 * 2. 优先级队列 - 高优先级消息进入特定分区
 * 3. 地域分区 - 不同地区消息进入不同分区
 */
@Slf4j
public class CustomPartitioner implements Partitioner {

    /**
     * 根据消息的key实现自定义分区
     *
     * 分区策略：
     * - 如果key以 "priority-" 开头，发送到分区0（高优先级）
     * - 如果key以 "vip-" 开头，发送到分区1（VIP用户）
     * - 其他key使用 murmur2 哈希分区
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                        Object value, byte[] valueBytes,
                        Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

        if (key == null) {
            // 没有key时，使用轮询或粘性分区
            return 0;
        }

        String keyStr = key.toString();

        if (keyStr.startsWith("priority-")) {
            // 高优先级消息，发送到分区0
            log.debug("高优先级消息，key={}", keyStr);
            return 0;
        }

        if (keyStr.startsWith("vip-")) {
            // VIP用户消息，发送到分区1
            log.debug("VIP用户消息，key={}", keyStr);
            return 1;
        }

        // 其他消息使用哈希分区，保证相同key的消息到相同分区
        int numPartitions = partitions.size();
        return Math.abs(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
        // 清理资源
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置中读取参数
    }
}
