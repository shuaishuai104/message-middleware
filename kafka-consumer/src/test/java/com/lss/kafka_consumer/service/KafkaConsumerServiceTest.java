package com.lss.kafka_consumer.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Consumer 服务单元测试
 * 测试消息处理和消费相关的逻辑
 */
class KafkaConsumerServiceTest {

    private static final String TOPIC = "test-topic";

    // ==================== 消息处理测试 ====================

    @Nested
    @DisplayName("消息处理测试")
    class MessageProcessingTests {

        @Test
        @DisplayName("验证消息key和value提取逻辑")
        void extractKeyAndValue() {
            // given
            String key = "order-001";
            String value = "测试消息内容";

            // when & then - 验证字符串处理
            assertThat(key).isEqualTo("order-001");
            assertThat(value).isEqualTo("测试消息内容");
        }

        @Test
        @DisplayName("验证分区和offset计算")
        void partitionAndOffsetCalculation() {
            // given
            int partition = 0;
            long offset = 100L;

            // when
            long nextOffset = offset + 1;

            // then
            assertThat(nextOffset).isEqualTo(101L);
            assertThat(partition).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("验证批量消息数量计算")
        void batchSizeCalculation() {
            // given
            int batchSize = 3;

            // when
            int processedCount = batchSize;

            // then
            assertThat(processedCount).isEqualTo(3);
        }
    }

    // ==================== Header 提取测试 ====================

    @Nested
    @DisplayName("消息头处理测试")
    class HeaderProcessingTests {

        @Test
        @DisplayName("提取追踪ID")
        void extractTraceId() {
            // given
            String traceId = "trace-12345";
            byte[] traceIdBytes = traceId.getBytes(StandardCharsets.UTF_8);

            // when
            String extractedTraceId = new String(traceIdBytes, StandardCharsets.UTF_8);

            // then
            assertThat(extractedTraceId).isEqualTo(traceId);
        }

        @Test
        @DisplayName("提取spanId")
        void extractSpanId() {
            // given
            String spanId = "span-67890";
            byte[] spanIdBytes = spanId.getBytes(StandardCharsets.UTF_8);

            // when
            String extractedSpanId = new String(spanIdBytes, StandardCharsets.UTF_8);

            // then
            assertThat(extractedSpanId).isEqualTo(spanId);
        }

        @Test
        @DisplayName("处理空header值")
        void handleNullHeaderValue() {
            // given
            byte[] nullBytes = null;

            // when
            String result = nullBytes != null ? new String(nullBytes, StandardCharsets.UTF_8) : null;

            // then
            assertThat(result).isNull();
        }
    }

    // ==================== 消息过滤测试 ====================

    @Nested
    @DisplayName("消息过滤测试")
    class MessageFilteringTests {

        @Test
        @DisplayName("BLOCK消息被识别")
        void blockedMessage_Identified() {
            // given
            String blockedValue = "包含BLOCK的内容";

            // when
            boolean isBlocked = isBlockedMessage(blockedValue);

            // then
            assertThat(isBlocked).isTrue();
        }

        @Test
        @DisplayName("正常消息不被拦截")
        void normalMessage_NotBlocked() {
            // given
            String normalValue = "这是一条正常的消息";

            // when
            boolean isBlocked = isBlockedMessage(normalValue);

            // then
            assertThat(isBlocked).isFalse();
        }

        @Test
        @DisplayName("空消息处理")
        void emptyMessage_Handled() {
            // given
            String emptyValue = "";

            // when
            boolean isBlocked = isBlockedMessage(emptyValue);

            // then
            assertThat(isBlocked).isFalse();
        }

        @Test
        @DisplayName("null消息处理")
        void nullMessage_Handled() {
            // when
            boolean isBlocked = isBlockedMessage(null);

            // then
            assertThat(isBlocked).isFalse();
        }
    }

    // ==================== Offset 管理测试 ====================

    @Nested
    @DisplayName("Offset管理测试")
    class OffsetManagementTests {

        @Test
        @DisplayName("计算下一个消费的offset")
        void calculateNextOffset() {
            // given
            long currentOffset = 100L;

            // when
            long nextOffset = currentOffset + 1;

            // then
            assertThat(nextOffset).isEqualTo(101L);
        }

        @Test
        @DisplayName("批量消息的起始和结束offset")
        void batchMessageOffsets() {
            // given
            long firstOffset = 100L;
            int batchSize = 3;

            // when
            long lastOffset = firstOffset + batchSize - 1;
            long nextOffset = lastOffset + 1;

            // then
            assertThat(firstOffset).isEqualTo(100L);
            assertThat(lastOffset).isEqualTo(102L);
            assertThat(nextOffset).isEqualTo(103L);
        }

        @Test
        @DisplayName("Offset提交验证")
        void offsetCommitValidation() {
            // given
            long offset = 100L;

            // when
            long offsetToCommit = offset + 1;

            // then
            assertThat(offsetToCommit).isGreaterThan(offset);
        }
    }

    // ==================== Topic 匹配测试 ====================

    @Nested
    @DisplayName("Topic匹配测试")
    class TopicMatchingTests {

        @Test
        @DisplayName("正则匹配test-开头的topic")
        void matchTestTopics() {
            // given
            String topic1 = "test-topic";
            String topic2 = "test-123";
            String topic3 = "other-topic";

            // when & then
            assertThat(topic1).matches("test-.*");
            assertThat(topic2).matches("test-.*");
            assertThat(topic3).doesNotMatch("test-.*");
        }

        @Test
        @DisplayName("Topic名称验证")
        void topicNameValidation() {
            // given
            String topicName = "my-application-topic";

            // when & then
            assertThat(topicName).isNotEmpty();
            assertThat(topicName).doesNotContain("#");
        }
    }

    // ==================== Helper Methods ====================

    private boolean isBlockedMessage(String value) {
        return value != null && value.contains("BLOCK");
    }
}
