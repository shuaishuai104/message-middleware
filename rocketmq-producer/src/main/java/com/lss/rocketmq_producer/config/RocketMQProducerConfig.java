package com.lss.rocketmq_producer.config;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * RocketMQ Producer 配置类
 * 使用 Spring Boot 自动配置
 */
@Configuration
public class RocketMQProducerConfig {

    private static final Logger log = LoggerFactory.getLogger(RocketMQProducerConfig.class);

    @Bean
    public RocketMQTemplate rocketMQTemplate() {
        log.info("RocketMQTemplate bean created");
        return new RocketMQTemplate();
    }
}
