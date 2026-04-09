package com.lss.rocketmq_consumer.controller;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/consumer")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);

    @Autowired
    private DefaultMQPushConsumer defaultMQPushConsumer;

    @GetMapping("/status")
    public Map<String, Object> getConsumerStatus() {
        Map<String, Object> status = new HashMap<>();

        try {
            String group = defaultMQPushConsumer.getConsumerGroup();
            status.put("groupId", group);
            status.put("nameServer", defaultMQPushConsumer.getNamesrvAddr());
            status.put("messageModel", defaultMQPushConsumer.getMessageModel());
            status.put("instanceName", defaultMQPushConsumer.getInstanceName());

            Map<String, String> subscription = defaultMQPushConsumer.getSubscription();
            status.put("subscription", subscription);
            status.put("success", true);
        } catch (Exception e) {
            log.error("Query consumer status failed", e);
            status.put("success", false);
            status.put("error", e.getMessage());
        }

        return status;
    }

    @GetMapping("/lag")
    public Map<String, Object> getConsumerLag() {
        Map<String, Object> result = new HashMap<>();

        try {
            result.put("success", true);
            result.put("message", "Lag查询需要通过RocketMQ管理工具或控制台查看");
        } catch (Exception e) {
            log.error("Query consumer lag failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    @GetMapping("/subscriptions")
    public Map<String, Object> getSubscriptions() {
        Map<String, Object> result = new HashMap<>();

        try {
            Map<String, String> subscription = defaultMQPushConsumer.getSubscription();
            result.put("subscriptions", subscription);
            result.put("success", true);
        } catch (Exception e) {
            log.error("Query subscriptions failed", e);
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }
}
