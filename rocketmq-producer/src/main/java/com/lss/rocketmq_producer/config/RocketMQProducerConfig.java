package com.lss.rocketmq_producer.config;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ Producer 配置类
 * 通过代码方式设置 Producer 属性
 */
@Configuration
public class RocketMQProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(RocketMQProducerConfig.class);

    @Value("${rocketmq.name-server:localhost:9876}")
    private String nameServer;

    @Value("${spring.rocketmq.producer.group:rocketmq-producer-group}")
    private String producerGroup;

    @Value("${spring.rocketmq.producer.send-message-timeout:30000}")
    private int sendMessageTimeout;

    @Value("${spring.rocketmq.producer.max-message-size:4194304}")
    private int maxMessageSize;

    @Value("${spring.rocketmq.producer.compress-message-body-threshold:4096}")
    private int compressMessageBodyThreshold;

    @Value("${spring.rocketmq.producer.retry-times-when-send-failed:3}")
    private int retryTimesWhenSendFailed;

    @Value("${spring.rocketmq.producer.retry-times-when-send-async-failed:2}")
    private int retryTimesWhenSendAsyncFailed;

    @Bean
    public DefaultMQProducer defaultMQProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServer);
        producer.setSendMsgTimeout(sendMessageTimeout);
        producer.setMaxMessageSize(maxMessageSize);
        producer.setCompressMsgBodyOverHowmuch(compressMessageBodyThreshold);
        producer.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);
        producer.setRetryTimesWhenSendAsyncFailed(retryTimesWhenSendAsyncFailed);
        producer.setVipChannelEnabled(false);

        log.info("DefaultMQProducer created: nameServer={}, group={}", nameServer, producerGroup);
        return producer;
    }

    @Bean
    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer defaultMQProducer) {
        RocketMQTemplate template = new RocketMQTemplate();
        template.setProducer(defaultMQProducer);
        log.info("RocketMQTemplate bean created with config: nameServer={}, group={}, timeout={}",
                nameServer, producerGroup, sendMessageTimeout);
        return template;
    }
}
