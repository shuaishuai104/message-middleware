package com.lss.rocketmq_consumer.service;

import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RocketMQMessageListener(
        topic = "test-topic",
        consumerGroup = "rocketmq-consumer-group-orderly",
        selectorExpression = "order-step"
)
public class OrderlyConsumer implements RocketMQListener<String> {

    private static final Logger log = LoggerFactory.getLogger(OrderlyConsumer.class);

    @Override
    public void onMessage(String message) {
        log.info("[Orderly consume] message={}", message);
    }
}
