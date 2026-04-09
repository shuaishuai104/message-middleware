package com.lss.rocketmq_consumer.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ Consumer 配置类
 *
 * 当前使用 Spring Boot 自动配置
 * 所有 Consumer 属性通过 application.yml 配置
 *
 * 如需自定义 Consumer 属性，请参考以下配置项：
 * - spring.rocketmq.name-server: NameServer 地址
 * - spring.rocketmq.consumer.group: 消费者组
 * - spring.rocketmq.consumer.consume-thread-min: 最小消费线程数
 * - spring.rocketmq.consumer.consume-thread-max: 最大消费线程数
 * - spring.rocketmq.consumer.max-retry-times: 最大重试次数
 * - spring.rocketmq.consumer.pull-batch-size: 拉取批量大小
 *
 * 消费者模式通过 @RocketMQMessageListener 注解配置：
 * - consumeMode: CONCURRENTLY (并发) / ORDERLY (顺序)
 * - messageModel: CLUSTERING (集群) / BROADCASTING (广播)
 */
@Configuration
public class RocketMQConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(RocketMQConsumerConfig.class);

    public RocketMQConsumerConfig() {
        log.info("RocketMQ Consumer Configuration initialized");
    }
}
