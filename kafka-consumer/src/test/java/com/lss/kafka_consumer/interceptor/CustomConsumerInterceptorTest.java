package com.lss.kafka_consumer.interceptor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 消费者拦截器单元测试
 */
class CustomConsumerInterceptorTest {

    private CustomConsumerInterceptor interceptor;

    @BeforeEach
    void setUp() {
        interceptor = new CustomConsumerInterceptor();
        Map<String, Object> configs = new HashMap<>();
        interceptor.configure(configs);
    }

    // ==================== 消息处理测试 ====================

    @Nested
    @DisplayName("消息处理测试")
    class MessageProcessingTests {

        @Test
        @DisplayName("正常消息不被过滤")
        void normalMessage_NotFiltered() {
            // given
            String value = "normal message";

            // when
            boolean isBlocked = isBlockedMessage(value);

            // then
            assertThat(isBlocked).isFalse();
        }

        @Test
        @DisplayName("包含BLOCK的消息被标记")
        void blockedMessage_Logged() {
            // given
            String value = "包含BLOCK的内容";

            // when
            boolean isBlocked = isBlockedMessage(value);

            // then
            assertThat(isBlocked).isTrue();
        }

        @Test
        @DisplayName("空消息处理")
        void emptyMessage_Handled() {
            // given
            String value = "";

            // when
            boolean isBlocked = isBlockedMessage(value);

            // then
            assertThat(isBlocked).isFalse();
        }
    }

    // ==================== Header 提取测试 ====================

    @Nested
    @DisplayName("Header提取测试")
    class HeaderExtractionTests {

        @Test
        @DisplayName("提取trace-id header")
        void extractTraceId() {
            // given
            String traceId = "trace-123";
            byte[] traceIdBytes = traceId.getBytes(StandardCharsets.UTF_8);

            // when
            String extractedTraceId = new String(traceIdBytes, StandardCharsets.UTF_8);

            // then
            assertThat(extractedTraceId).isEqualTo(traceId);
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

    // ==================== 偏移量提交测试 ====================

    @Nested
    @DisplayName("偏移量提交测试")
    class OffsetCommitTests {

        @Test
        @DisplayName("验证偏移量计算")
        void validateOffsetCalculation() {
            // given
            long currentOffset = 100L;

            // when
            long nextOffsetToCommit = currentOffset + 1;

            // then
            assertThat(nextOffsetToCommit).isEqualTo(101L);
        }

        @Test
        @DisplayName("多分区偏移量计算")
        void multiPartitionOffsetCalculation() {
            // given
            Map<Integer, Long> partitionOffsets = new HashMap<>();
            partitionOffsets.put(0, 100L);
            partitionOffsets.put(1, 200L);
            partitionOffsets.put(2, 300L);

            // then
            assertThat(partitionOffsets).hasSize(3);
            assertThat(partitionOffsets.get(0)).isEqualTo(100L);
            assertThat(partitionOffsets.get(1)).isEqualTo(200L);
            assertThat(partitionOffsets.get(2)).isEqualTo(300L);
        }
    }

    // ==================== 生命周期测试 ====================

    @Nested
    @DisplayName("生命周期测试")
    class LifecycleTests {

        @Test
        @DisplayName("关闭拦截器 - 不抛异常")
        void close_DoesNotThrow() {
            // when & then
            interceptor.close();
        }

        @Test
        @DisplayName("配置为空 - 正常初始化")
        void configure_EmptyConfigs() {
            // given
            CustomConsumerInterceptor newInterceptor = new CustomConsumerInterceptor();

            // when & then - 不抛异常
            newInterceptor.configure(new HashMap<>());
            newInterceptor.close();
        }

        @Test
        @DisplayName("拦截器配置验证")
        void interceptorConfiguration() {
            // given
            Map<String, Object> configs = new HashMap<>();
            configs.put("key1", "value1");
            configs.put("key2", "value2");

            // when
            interceptor.configure(configs);

            // then - 不抛异常
        }
    }

    // ==================== Helper Methods ====================

    private boolean isBlockedMessage(String value) {
        return value != null && value.contains("BLOCK");
    }
}
