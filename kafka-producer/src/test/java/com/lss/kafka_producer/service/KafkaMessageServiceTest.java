package com.lss.kafka_producer.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Kafka 消息服务单元测试
 * 测试 Kafka Producer 的各种发送场景
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaMessageServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaMessageService kafkaMessageService;

    private static final String TOPIC_NAME = "test-topic";

    @BeforeEach
    void setUp() {
        kafkaMessageService = new KafkaMessageService(kafkaTemplate);
        ReflectionTestUtils.setField(kafkaMessageService, "topicName", TOPIC_NAME);
    }

    private RecordMetadata createRecordMetadata(int partition, long offset) {
        return new RecordMetadata(
                new TopicPartition(TOPIC_NAME, partition),
                offset,
                0,
                0,
                0,
                0
        );
    }

    // ==================== 同步发送测试 ====================

    @Nested
    @DisplayName("同步发送测试")
    class SyncSendTests {

        @Test
        @DisplayName("同步发送成功 - 返回正确的分区和偏移量")
        void syncSend_Success() throws Exception {
            // given
            String key = "order-001";
            String value = "测试消息内容";
            int partition = 0;
            long offset = 100L;

            RecordMetadata metadata = createRecordMetadata(partition, offset);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when
            SendResult<String, String> result = kafkaMessageService.sendSync(key, value);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().partition()).isEqualTo(partition);
            assertThat(result.getRecordMetadata().offset()).isEqualTo(offset);

            ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate).send(recordCaptor.capture());

            ProducerRecord<String, String> capturedRecord = recordCaptor.getValue();
            assertThat(capturedRecord.topic()).isEqualTo(TOPIC_NAME);
            assertThat(capturedRecord.key()).isEqualTo(key);
            assertThat(capturedRecord.value()).isEqualTo(value);
        }

        @Test
        @DisplayName("同步发送失败 - 抛出异常")
        void syncSend_Failure() throws Exception {
            // given
            String key = "order-002";
            String value = "失败消息";

            CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("Kafka发送失败"));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when & then
            assertThatThrownBy(() -> kafkaMessageService.sendSync(key, value))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("发送失败");
        }

        @Test
        @DisplayName("同步发送超时 - 抛出TimeoutException")
        void syncSend_Timeout() {
            // given
            String key = "order-003";
            String value = "超时消息";

            CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when & then
            assertThatThrownBy(() -> kafkaMessageService.sendSync(key, value))
                    .isInstanceOf(RuntimeException.class);
        }

        @Test
        @DisplayName("同步发送到指定分区成功")
        void sendSyncToPartition_Success() throws Exception {
            // given
            int partition = 2;
            String value = "指定分区消息";
            long offset = 50L;

            RecordMetadata metadata = createRecordMetadata(partition, offset);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when
            SendResult<String, String> result = kafkaMessageService.sendSyncToPartition(partition, value);

            // then
            assertThat(result.getRecordMetadata().partition()).isEqualTo(partition);

            ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate).send(recordCaptor.capture());

            ProducerRecord<String, String> capturedRecord = recordCaptor.getValue();
            assertThat(capturedRecord.partition()).isEqualTo(partition);
        }
    }

    // ==================== 异步发送测试 ====================

    @Nested
    @DisplayName("异步发送测试")
    class AsyncSendTests {

        @Test
        @DisplayName("异步发送成功 - 回调正常执行")
        void asyncSend_Success() {
            // given
            String key = "async-key";
            String value = "异步消息";
            int partition = 0;
            long offset = 200L;

            RecordMetadata metadata = createRecordMetadata(partition, offset);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            KafkaMessageService.SendCallback callback = mock(KafkaMessageService.SendCallback.class);

            // when
            kafkaMessageService.sendAsync(key, value, callback);

            // then
            verify(callback).onSuccess(any(SendResult.class));
            verify(callback, never()).onError(any(Throwable.class));
        }

        @Test
        @DisplayName("异步发送失败 - 回调错误处理")
        void asyncSend_Failure() {
            // given
            String key = "async-fail-key";
            String value = "失败异步消息";

            CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
            future.completeExceptionally(new RuntimeException("网络异常"));
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            KafkaMessageService.SendCallback callback = mock(KafkaMessageService.SendCallback.class);

            // when
            kafkaMessageService.sendAsync(key, value, callback);

            // then - 异步回调需要等待
            verify(callback, timeout(1000)).onError(any(Throwable.class));
            verify(callback, never()).onSuccess(any());
        }

        @Test
        @DisplayName("异步发送无回调 - 不抛出异常")
        void asyncSend_NoCallback() {
            // given
            String key = "no-callback-key";
            String value = "无回调消息";

            RecordMetadata metadata = createRecordMetadata(0, 300L);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when & then - 不抛出异常
            kafkaMessageService.sendAsync(key, value);
        }
    }

    // ==================== 带消息头的发送测试 ====================

    @Nested
    @DisplayName("带消息头发送测试")
    class HeadersSendTests {

        @Test
        @DisplayName("发送带追踪头的消息成功")
        void sendWithHeaders_Success() throws Exception {
            // given
            String key = "header-key";
            String value = "带头消息";
            String traceId = "trace-12345";
            String spanId = "span-67890";
            int partition = 1;
            long offset = 400L;

            RecordMetadata metadata = createRecordMetadata(partition, offset);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when
            SendResult<String, String> result = kafkaMessageService.sendWithHeaders(key, value, traceId, spanId);

            // then
            assertThat(result).isNotNull();

            ArgumentCaptor<ProducerRecord<String, String>> recordCaptor =
                    ArgumentCaptor.forClass(ProducerRecord.class);
            verify(kafkaTemplate).send(recordCaptor.capture());

            ProducerRecord<String, String> capturedRecord = recordCaptor.getValue();

            // 验证trace-id头
            Header traceHeader = capturedRecord.headers().lastHeader("trace-id");
            assertThat(traceHeader).isNotNull();
            assertThat(new String(traceHeader.value(), StandardCharsets.UTF_8)).isEqualTo(traceId);

            // 验证span-id头
            Header spanHeader = capturedRecord.headers().lastHeader("span-id");
            assertThat(spanHeader).isNotNull();
            assertThat(new String(spanHeader.value(), StandardCharsets.UTF_8)).isEqualTo(spanId);

            // 验证content-type头
            Header contentTypeHeader = capturedRecord.headers().lastHeader("content-type");
            assertThat(contentTypeHeader).isNotNull();
        }
    }

    // ==================== 批量发送测试 ====================

    @Nested
    @DisplayName("批量发送测试")
    class BatchSendTests {

        @Test
        @DisplayName("批量发送多条消息成功")
        void sendBatch_Success() throws Exception {
            // given
            List<ProducerRecord<String, String>> messages = List.of(
                    new ProducerRecord<>(TOPIC_NAME, "key1", "value1"),
                    new ProducerRecord<>(TOPIC_NAME, "key2", "value2"),
                    new ProducerRecord<>(TOPIC_NAME, "key3", "value3")
            );

            int lastPartition = 0;
            long lastOffset = 500L;

            RecordMetadata metadata = createRecordMetadata(lastPartition, lastOffset);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(sendResult);
            when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(future);

            // when
            SendResult<String, String> result = kafkaMessageService.sendBatch(messages);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().offset()).isEqualTo(lastOffset);

            verify(kafkaTemplate, times(3)).send(any(ProducerRecord.class));
            verify(kafkaTemplate).flush();
        }

        @Test
        @DisplayName("批量发送失败 - 抛出异常")
        void sendBatch_Failure() throws Exception {
            // given
            List<ProducerRecord<String, String>> messages = List.of(
                    new ProducerRecord<>(TOPIC_NAME, "key1", "value1"),
                    new ProducerRecord<>(TOPIC_NAME, "key2", "value2")
            );

            RecordMetadata metadata = createRecordMetadata(0, 100L);
            SendResult<String, String> sendResult = new SendResult<>(null, metadata);

            CompletableFuture<SendResult<String, String>> successFuture = CompletableFuture.completedFuture(sendResult);
            CompletableFuture<SendResult<String, String>> failFuture = new CompletableFuture<>();
            failFuture.completeExceptionally(new RuntimeException("批量发送失败"));

            when(kafkaTemplate.send(any(ProducerRecord.class)))
                    .thenReturn(successFuture)
                    .thenReturn(failFuture);

            // when & then
            assertThatThrownBy(() -> kafkaMessageService.sendBatch(messages))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("批量发送失败");
        }
    }

}
