package com.lss.kafka_producer.controller;

import com.lss.kafka_producer.service.KafkaMessageService;
import lombok.Data;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Kafka 消息发送控制器
 * 提供REST API发送各种类型的Kafka消息
 */
@RestController
@RequestMapping("/api/kafka")
public class MessageController {

    private final KafkaMessageService kafkaMessageService;

    public MessageController(KafkaMessageService kafkaMessageService) {
        this.kafkaMessageService = kafkaMessageService;
    }

    // ==================== 同步发送 ====================

    /**
     * POST /api/kafka/sync
     * 同步发送消息 - 阻塞等待Broker确认
     */
    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> sendSync(@RequestBody MessageRequest request) {
        SendResult<String, String> result = kafkaMessageService.sendSync(request.getKey(), request.getValue());

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "sync",
                "partition", result.getRecordMetadata().partition(),
                "offset", result.getRecordMetadata().offset(),
                "timestamp", result.getRecordMetadata().timestamp()
        ));
    }

    /**
     * POST /api/kafka/sync/partition/{partition}
     * 同步发送消息到指定分区
     */
    @PostMapping("/sync/partition/{partition}")
    public ResponseEntity<Map<String, Object>> sendSyncToPartition(
            @PathVariable int partition,
            @RequestBody MessageRequest request) {
        SendResult<String, String> result = kafkaMessageService.sendSyncToPartition(partition, request.getValue());

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "sync-to-partition",
                "partition", result.getRecordMetadata().partition(),
                "offset", result.getRecordMetadata().offset()
        ));
    }

    // ==================== 异步发送 ====================

    /**
     * POST /api/kafka/async
     * 异步发送消息 - 立即返回，通过回调处理结果
     */
    @PostMapping("/async")
    public ResponseEntity<Map<String, Object>> sendAsync(@RequestBody MessageRequest request) {
        kafkaMessageService.sendAsync(request.getKey(), request.getValue());

        Map<String, Object> result = new java.util.HashMap<>();
        result.put("success", true);
        result.put("message", "异步消息已提交");
        result.put("key", request.getKey());

        return ResponseEntity.ok(result);
    }

    // ==================== 带消息头的发送 ====================

    /**
     * POST /api/kafka/with-headers
     * 发送带追踪头的消息
     */
    @PostMapping("/with-headers")
    public ResponseEntity<Map<String, Object>> sendWithHeaders(@RequestBody MessageRequest request) {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString().substring(0, 8);

        SendResult<String, String> result = kafkaMessageService.sendWithHeaders(
                request.getKey(),
                request.getValue(),
                traceId,
                spanId
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "with-headers",
                "traceId", traceId,
                "spanId", spanId,
                "partition", result.getRecordMetadata().partition(),
                "offset", result.getRecordMetadata().offset()
        ));
    }

    // ==================== 批量发送 ====================

    /**
     * POST /api/kafka/batch
     * 批量发送消息 - 适用于高吞吐场景
     */
    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatch(@RequestBody BatchMessageRequest request) {
        List<org.apache.kafka.clients.producer.ProducerRecord<String, String>> records = request.getMessages().stream()
                .map(msg -> new org.apache.kafka.clients.producer.ProducerRecord<>(
                        request.getTopic() != null ? request.getTopic() : "test-topic",
                        msg.getKey(),
                        msg.getValue()
                ))
                .collect(Collectors.toList());

        SendResult<String, String> result = kafkaMessageService.sendBatch(records);

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "batch",
                "count", request.getMessages().size(),
                "lastPartition", result.getRecordMetadata().partition(),
                "lastOffset", result.getRecordMetadata().offset()
        ));
    }

    // ==================== 事务发送 ====================

    /**
     * POST /api/kafka/transaction
     * 事务发送 - 两条消息作为一个事务
     */
    @PostMapping("/transaction")
    public ResponseEntity<Map<String, Object>> sendTransaction(@RequestBody TransactionRequest request) {
        SendResult<String, String> result = kafkaMessageService.sendTransaction(
                request.getKey1(),
                request.getValue1(),
                request.getKey2(),
                request.getValue2()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "transaction",
                "message", "事务提交成功，两条消息均已发送"
        ));
    }

    // ==================== One-Way 发送 ====================

    /**
     * POST /api/kafka/oneway
     * Fire-and-Forget 发送 - 最低延迟，最高丢失风险
     */
    @PostMapping("/oneway")
    public ResponseEntity<Map<String, Object>> sendOneWay(@RequestBody MessageRequest request) {
        kafkaMessageService.sendOneWay(request.getKey(), request.getValue());

        Map<String, Object> result = new java.util.HashMap<>();
        result.put("success", true);
        result.put("type", "one-way");
        result.put("message", "消息已提交发送（不保证送达）");

        return ResponseEntity.ok(result);
    }

    // ==================== 请求和响应类 ====================

    @Data
    public static class MessageRequest {
        private String key;
        private String value;
    }

    @Data
    public static class BatchMessageRequest {
        private String topic;
        private List<MessageRequest> messages;
    }

    @Data
    public static class TransactionRequest {
        private String key1;
        private String value1;
        private String key2;
        private String value2;
    }
}
