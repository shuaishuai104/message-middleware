package com.lss.rocketmq_producer.service;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Service
public class RocketMQMessageService {

    private static final Logger log = LoggerFactory.getLogger(RocketMQMessageService.class);

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    private static final String TOPIC = "test-topic";

    /**
     * 同步发送消息 - 使用 "topic:tag" 格式指定目标
     */
    public SendResult sendSync(String tag, String key, String body) {
        String destination = TOPIC + ":" + (tag != null ? tag : "*");

        try {
            SendResult result = rocketMQTemplate.syncSend(destination, body, 3000);

            log.info("[Sync send OK] msgId={}, offset={}",
                    result.getMsgId(), result.getQueueOffset());

            return result;
        } catch (Exception e) {
            log.error("[Sync send FAIL] topic={}, tag={}, error={}", TOPIC, tag, e.getMessage());
            throw new RuntimeException("消息发送失败", e);
        }
    }

    /**
     * 发送消息到指定队列 - 使用 MessageQueueSelector
     */
    public SendResult sendSyncToQueue(int queueId, String body) {
        Message message = new Message(TOPIC, "*", "", body.getBytes(StandardCharsets.UTF_8));

        try {
            SendResult result = rocketMQTemplate.getProducer().send(message,
                    (mqs, msg, arg) -> mqs.get((Integer) arg), queueId, 3000);
            log.info("[Sync send to queue OK] queueId={}, msgId={}", queueId, result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("指定队列发送失败", e);
        }
    }

    /**
     * 异步发送消息
     */
    public void sendAsync(String tag, String key, String body) {
        String destination = TOPIC + ":" + (tag != null ? tag : "*");

        rocketMQTemplate.asyncSend(destination, body, new org.apache.rocketmq.client.producer.SendCallback() {
            @Override
            public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                log.info("[Async send OK] msgId={}, offset={}",
                        result.getMsgId(), result.getQueueOffset());
            }

            @Override
            public void onException(Throwable e) {
                log.error("[Async send FAIL] topic={}, error={}", TOPIC, e.getMessage());
            }
        });

        log.debug("[Async send submitted] tag={}, key={}", tag, key);
    }

    /**
     * 单向发送消息（不等待响应）
     */
    public void sendOneWay(String tag, String body) {
        String destination = TOPIC + ":" + (tag != null ? tag : "*");
        rocketMQTemplate.sendOneWay(destination, body);
        log.debug("[Oneway send submitted] tag={}", tag);
    }

    /**
     * 批量发送消息
     */
    public SendResult sendBatch(List<String> messages) {
        try {
            SendResult result = rocketMQTemplate.syncSend(TOPIC, messages, 3000);
            log.info("[Batch send OK] count={}, msgId={}", messages.size(), result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("批量发送失败", e);
        }
    }

    /**
     * 带 Tags 发送消息 - tags 可以是 "tag1 || tag2 || tag3" 格式
     */
    public SendResult sendWithTags(String tags, String key, String body) {
        String destination = TOPIC + ":" + tags;
        Message message = new Message();
        message.setTopic(TOPIC);
        message.setTags(tags);
        message.setKeys(key != null ? key : UUID.randomUUID().toString());
        message.setBody(body.getBytes(StandardCharsets.UTF_8));
        message.putUserProperty("trace-id", UUID.randomUUID().toString());
        message.putUserProperty("span-id", UUID.randomUUID().toString().substring(0, 8));

        try {
            SendResult result = rocketMQTemplate.syncSend(destination, message, 3000);
            log.info("[Send with tags OK] tags={}, msgId={}", tags, result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("带Tags发送失败", e);
        }
    }

    /**
     * 顺序消息发送 - 同一 orderId 的消息按顺序发送
     */
    public void sendOrderly(String orderId, List<String> steps) {
        MessageQueueSelector selector = (mqs, msg, arg) -> {
            int hash = arg.toString().hashCode();
            return mqs.get(Math.abs(hash % mqs.size()));
        };

        for (int i = 0; i < steps.size(); i++) {
            String step = steps.get(i);
            Message message = new Message(TOPIC, "order-step", orderId,
                    String.format("step-%d: %s", i, step).getBytes(StandardCharsets.UTF_8));

            try {
                SendResult result = rocketMQTemplate.getProducer().send(message, selector, orderId, 3000);
                log.info("[Orderly send OK] orderId={}, step={}, msgId={}",
                        orderId, i, result.getMsgId());
            } catch (Exception e) {
                throw new RuntimeException("顺序消息发送失败", e);
            }
        }
    }
}
