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

    public SendResult sendSync(String tag, String key, String body) {
        Message message = new Message(TOPIC, tag, key, body.getBytes(StandardCharsets.UTF_8));

        try {
            SendResult result = rocketMQTemplate.syncSend(TOPIC, message, 3000);

            log.info("[Sync send OK] msgId={}, offset={}",
                    result.getMsgId(), result.getQueueOffset());

            return result;
        } catch (Exception e) {
            log.error("[Sync send FAIL] topic={}, tag={}, error={}", TOPIC, tag, e.getMessage());
            throw new RuntimeException("消息发送失败", e);
        }
    }

    public SendResult sendSyncToQueue(int queueId, String body) {
        Message message = new Message(TOPIC, "*", "", body.getBytes(StandardCharsets.UTF_8));

        try {
            SendResult result = rocketMQTemplate.syncSend(TOPIC + ":" + queueId, message, 3000);
            log.info("[Sync send to queue OK] queueId={}, msgId={}", queueId, result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("指定队列发送失败", e);
        }
    }

    public void sendAsync(String tag, String key, String body) {
        Message message = new Message(TOPIC, tag, key, body.getBytes(StandardCharsets.UTF_8));

        rocketMQTemplate.asyncSend(TOPIC, message, new org.apache.rocketmq.client.producer.SendCallback() {
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

    public void sendOneWay(String tag, String body) {
        Message message = new Message(TOPIC, tag, "", body.getBytes(StandardCharsets.UTF_8));
        rocketMQTemplate.sendOneWay(TOPIC, message);
        log.debug("[Oneway send submitted] tag={}", tag);
    }

    public SendResult sendBatch(List<String> messages) {
        try {
            SendResult result = rocketMQTemplate.syncSend(TOPIC, messages, 3000);
            log.info("[Batch send OK] count={}, msgId={}", messages.size(), result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("批量发送失败", e);
        }
    }

    public SendResult sendWithTags(String tags, String key, String body) {
        Message message = new Message(TOPIC, tags, key, body.getBytes(StandardCharsets.UTF_8));
        message.putUserProperty("trace-id", UUID.randomUUID().toString());
        message.putUserProperty("span-id", UUID.randomUUID().toString().substring(0, 8));

        try {
            SendResult result = rocketMQTemplate.syncSend(TOPIC, message, 3000);
            log.info("[Send with tags OK] tags={}, msgId={}", tags, result.getMsgId());
            return result;
        } catch (Exception e) {
            throw new RuntimeException("带Tags发送失败", e);
        }
    }

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
