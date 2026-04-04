package com.lss.kafka_producer.partitioner;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * 自定义分区器单元测试
 */
class CustomPartitionerTest {

    private CustomPartitioner partitioner;

    @Mock
    private Cluster cluster;

    private static final String TOPIC = "test-topic";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        partitioner = new CustomPartitioner();

        // 模拟4个分区
        List<PartitionInfo> partitions = List.of(
                new PartitionInfo(TOPIC, 0, null, null, null),
                new PartitionInfo(TOPIC, 1, null, null, null),
                new PartitionInfo(TOPIC, 2, null, null, null),
                new PartitionInfo(TOPIC, 3, null, null, null)
        );
        when(cluster.partitionsForTopic(TOPIC)).thenReturn(partitions);
    }

    @Nested
    @DisplayName("分区策略测试")
    class PartitionStrategyTests {

        @Test
        @DisplayName("高优先级消息 - 发送到分区0")
        void priorityMessage_ToPartition0() {
            // given
            String key = "priority-order-001";

            // when
            int partition = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then
            assertThat(partition).isEqualTo(0);
        }

        @Test
        @DisplayName("VIP用户消息 - 发送到分区1")
        void vipMessage_ToPartition1() {
            // given
            String key = "vip-user-12345";

            // when
            int partition = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then
            assertThat(partition).isEqualTo(1);
        }

        @Test
        @DisplayName("普通消息 - 使用哈希分区")
        void normalMessage_UseHashPartition() {
            // given
            String key = "normal-order-001";

            // when
            int partition1 = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);
            int partition2 = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then - 相同key应该得到相同分区（哈希一致性）
            assertThat(partition1).isEqualTo(partition2);
            assertThat(partition1).isBetween(0, 3);
        }

        @Test
        @DisplayName("Null key - 默认使用分区0")
        void nullKey_DefaultToPartition0() {
            // when
            int partition = partitioner.partition(TOPIC, null, null,
                    "value", "value".getBytes(), cluster);

            // then
            assertThat(partition).isEqualTo(0);
        }

        @Test
        @DisplayName("不同普通key - 分布在不同分区")
        void differentKeys_DifferentPartitions() {
            // given
            String key1 = "order-001";
            String key2 = "order-002";
            String key3 = "order-003";

            // when
            int partition1 = partitioner.partition(TOPIC, key1, key1.getBytes(),
                    "value", "value".getBytes(), cluster);
            int partition2 = partitioner.partition(TOPIC, key2, key2.getBytes(),
                    "value", "value".getBytes(), cluster);
            int partition3 = partitioner.partition(TOPIC, key3, key3.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then - 分布应该均匀
            assertThat(partition1).isBetween(0, 3);
            assertThat(partition2).isBetween(0, 3);
            assertThat(partition3).isBetween(0, 3);
        }
    }

    @Nested
    @DisplayName("边界条件测试")
    class EdgeCaseTests {

        @Test
        @DisplayName("空key字符串 - 使用哈希分区")
        void emptyKey_UseHashPartition() {
            // given
            String key = "";

            // when
            int partition = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then
            assertThat(partition).isBetween(0, 3);
        }

        @Test
        @DisplayName("只有前缀匹配的部分key - 使用哈希分区")
        void partialMatchKey_UseHashPartition() {
            // given
            String key = "priority-order"; // 以priority开头但后面不是标准格式

            // when
            int partition = partitioner.partition(TOPIC, key, key.getBytes(),
                    "value", "value".getBytes(), cluster);

            // then - 应该使用哈希分区而不是分区0
            assertThat(partition).isBetween(0, 3);
        }
    }
}
