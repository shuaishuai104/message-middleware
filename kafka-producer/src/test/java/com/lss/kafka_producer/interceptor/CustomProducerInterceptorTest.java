package com.lss.kafka_producer.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 生产者拦截器单元测试
 */
class CustomProducerInterceptorTest {

    private CustomProducerInterceptor interceptor;

    @BeforeEach
    void setUp() {
        interceptor = new CustomProducerInterceptor();
        Map<String, Object> configs = new HashMap<>();
        interceptor.configure(configs);
    }

    @Nested
    @DisplayName("发送前拦截测试")
    class OnSendTests {

        @Test
        @DisplayName("添加追踪头到消息")
        void onSend_AddsTraceHeaders() {
            // given
            String key = "test-key";
            String value = "test-value";
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", key, value);

            // when
            ProducerRecord<String, String> intercepted = interceptor.onSend(record);

            // then
            assertThat(intercepted).isNotNull();
            assertThat(intercepted.headers()).isNotEmpty();

            // 验证 trace-id 头存在
            var traceHeader = intercepted.headers().lastHeader("x-trace-id");
            assertThat(traceHeader).isNotNull();
            assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8))
                    .isNotEmpty();

            // 验证 send-time 头存在
            var sendTimeHeader = intercepted.headers().lastHeader("x-send-time");
            assertThat(sendTimeHeader).isNotNull();
            assertThat(new String(sendTimeHeader.value(), StandardCharsets.UTF_8))
                    .matches("\\d+"); // 应该是数字时间戳
        }

        @Test
        @DisplayName("保留原始消息内容")
        void onSend_PreservesOriginalContent() {
            // given
            String key = "original-key";
            String value = "original-value";
            ProducerRecord<String, String> record = new ProducerRecord<>("original-topic", key, value);

            // when
            ProducerRecord<String, String> intercepted = interceptor.onSend(record);

            // then
            assertThat(intercepted.key()).isEqualTo(key);
            assertThat(intercepted.value()).isEqualTo(value);
            assertThat(intercepted.topic()).isEqualTo("original-topic");
        }

        @Test
        @DisplayName("发送到指定分区 - 保留分区信息")
        void onSend_PreservesPartition() {
            // given
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 2, "key", "value");

            // when
            ProducerRecord<String, String> intercepted = interceptor.onSend(record);

            // then
            assertThat(intercepted.partition()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("发送成功确认测试")
    class OnAcknowledgementTests {

        @Test
        @DisplayName("发送成功 - 不抛异常")
        void onAcknowledgement_Success() {
            // given
            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("test-topic", 0),
                    100L, 0, 0, 0, 0
            );

            // when & then - 不抛异常
            interceptor.onAcknowledgement(metadata, null);
        }

        @Test
        @DisplayName("发送失败 - 记录错误日志")
        void onAcknowledgement_Failure() {
            // given
            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition("test-topic", 0),
                    100L, 0, 0, 0, 0
            );
            Exception exception = new RuntimeException("Send failed");

            // when & then - 不抛异常
            interceptor.onAcknowledgement(metadata, exception);
        }
    }

    @Nested
    @DisplayName("生命周期测试")
    class LifecycleTests {

        @Test
        @DisplayName("关闭拦截器 - 不抛异常")
        void close_DoesNotThrow() {
            // when & then
            interceptor.close(); // 不应该抛异常
        }

        @Test
        @DisplayName("配置为空 - 正常初始化")
        void configure_EmptyConfigs() {
            // given
            CustomProducerInterceptor newInterceptor = new CustomProducerInterceptor();

            // when & then - 不抛异常
            newInterceptor.configure(new HashMap<>());
            newInterceptor.close();
        }
    }
}
