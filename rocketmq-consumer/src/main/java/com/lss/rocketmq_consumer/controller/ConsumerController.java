package com.lss.rocketmq_consumer.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    @Value("${spring.rocketmq.consumer.group:rocketmq-consumer-group}")
    private String consumerGroup;

    @Value("${spring.rocketmq.name-server:localhost:9876}")
    private String nameServer;

    @GetMapping("/status")
    public Map<String, Object> getConsumerStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("groupId", consumerGroup);
        status.put("nameServer", nameServer);
        status.put("messageModel", "CLUSTERING");
        status.put("consumeMode", "CONCURRENTLY");
        status.put("success", true);
        return status;
    }

    @GetMapping("/lag")
    public Map<String, Object> getConsumerLag() {
        Map<String, Object> result = new HashMap<>();
        result.put("success", true);
        result.put("message", "Lag查询需要通过RocketMQ管理工具或控制台查看");
        result.put("tip", "请访问 http://localhost:8082 查看RocketMQ Console");
        return result;
    }

    @GetMapping("/subscriptions")
    public Map<String, Object> getSubscriptions() {
        Map<String, Object> result = new HashMap<>();
        result.put("subscriptions", java.util.Collections.singletonList("test-topic"));
        result.put("success", true);
        return result;
    }
}
