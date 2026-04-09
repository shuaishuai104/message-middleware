package com.lss.rocketmq_producer.controller;

import com.lss.rocketmq_producer.service.RocketMQMessageService;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/rocketmq")
public class MessageController {

    private final RocketMQMessageService rocketMQMessageService;

    public MessageController(RocketMQMessageService rocketMQMessageService) {
        this.rocketMQMessageService = rocketMQMessageService;
    }

    @PostMapping("/sync")
    public ResponseEntity<Map<String, Object>> sendSync(@RequestBody MessageRequest request) {
        SendResult result = rocketMQMessageService.sendSync(
                request.getTag() != null ? request.getTag() : "*",
                request.getKey() != null ? request.getKey() : UUID.randomUUID().toString(),
                request.getValue()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "sync",
                "msgId", result.getMsgId(),
                "queueOffset", result.getQueueOffset()
        ));
    }

    @PostMapping("/sync/queue/{queueId}")
    public ResponseEntity<Map<String, Object>> sendSyncToQueue(
            @PathVariable int queueId,
            @RequestBody MessageRequest request) {
        SendResult result = rocketMQMessageService.sendSyncToQueue(queueId, request.getValue());

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "sync-to-queue",
                "queueId", queueId,
                "msgId", result.getMsgId()
        ));
    }

    @PostMapping("/async")
    public ResponseEntity<Map<String, Object>> sendAsync(@RequestBody MessageRequest request) {
        rocketMQMessageService.sendAsync(
                request.getTag() != null ? request.getTag() : "*",
                request.getKey() != null ? request.getKey() : UUID.randomUUID().toString(),
                request.getValue()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "异步消息已提交",
                "key", request.getKey()
        ));
    }

    @PostMapping("/with-tags")
    public ResponseEntity<Map<String, Object>> sendWithTags(@RequestBody TagsMessageRequest request) {
        SendResult result = rocketMQMessageService.sendWithTags(
                request.getTags(),
                request.getKey() != null ? request.getKey() : UUID.randomUUID().toString(),
                request.getValue()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "with-tags",
                "tags", request.getTags(),
                "msgId", result.getMsgId()
        ));
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatch(@RequestBody BatchMessageRequest request) {
        SendResult result = rocketMQMessageService.sendBatch(request.getMessages());

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "batch",
                "count", request.getMessages().size(),
                "msgId", result.getMsgId()
        ));
    }

    @PostMapping("/oneway")
    public ResponseEntity<Map<String, Object>> sendOneWay(@RequestBody MessageRequest request) {
        rocketMQMessageService.sendOneWay(
                request.getTag() != null ? request.getTag() : "*",
                request.getValue()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "one-way",
                "message", "消息已提交发送（不保证送达）"
        ));
    }

    @PostMapping("/order")
    public ResponseEntity<Map<String, Object>> sendOrder(@RequestBody OrderMessageRequest request) {
        rocketMQMessageService.sendOrderly(
                request.getOrderId(),
                request.getSteps()
        );

        return ResponseEntity.ok(Map.of(
                "success", true,
                "type", "orderly",
                "orderId", request.getOrderId(),
                "stepsCount", request.getSteps().size()
        ));
    }

    public static class MessageRequest {
        private String tag;
        private String key;
        private String value;

        public String getTag() { return tag; }
        public void setTag(String tag) { this.tag = tag; }
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    public static class TagsMessageRequest {
        private String tags;
        private String key;
        private String value;

        public String getTags() { return tags; }
        public void setTags(String tags) { this.tags = tags; }
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    public static class BatchMessageRequest {
        private List<String> messages;

        public List<String> getMessages() { return messages; }
        public void setMessages(List<String> messages) { this.messages = messages; }
    }

    public static class OrderMessageRequest {
        private String orderId;
        private List<String> steps;

        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
        public List<String> getSteps() { return steps; }
        public void setSteps(List<String> steps) { this.steps = steps; }
    }
}
