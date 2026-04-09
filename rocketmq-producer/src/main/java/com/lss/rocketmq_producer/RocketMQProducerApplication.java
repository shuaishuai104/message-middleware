package com.lss.rocketmq_producer;

import org.apache.rocketmq.spring.autoconfigure.RocketMQAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(exclude = {RocketMQAutoConfiguration.class})
public class RocketMQProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RocketMQProducerApplication.class, args);
    }
}
