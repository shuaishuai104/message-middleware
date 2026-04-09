package com.lss.rocketmq_consumer.config;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RocketMQConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(RocketMQConsumerConfig.class);

    @Value("${spring.rocketmq.name-server:localhost:9876}")
    private String nameServer;

    @Value("${spring.rocketmq.consumer.group:rocketmq-consumer-group}")
    private String consumerGroup;

    @Value("${spring.rocketmq.consumer.consume-thread-min:10}")
    private int consumeThreadMin;

    @Value("${spring.rocketmq.consumer.consume-thread-max:20}")
    private int consumeThreadMax;

    @Value("${spring.rocketmq.consumer.max-retry-times:3}")
    private int maxRetryTimes;

    @Bean
    public DefaultMQPushConsumer defaultMQPushConsumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeThreadMin(consumeThreadMin);
        consumer.setConsumeThreadMax(consumeThreadMax);
        consumer.setMaxReconsumeTimes(maxRetryTimes);

        log.info("Consumer configured, group={}, nameServer={}", consumerGroup, nameServer);

        return consumer;
    }
}
