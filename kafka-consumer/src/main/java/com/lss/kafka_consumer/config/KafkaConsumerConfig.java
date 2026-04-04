package com.lss.kafka_consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer 配置类
 * 包含常用消费者配置、手动提交配置、并发配置等
 */
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.group-id}")
    private String groupId;

    @Value("${spring.kafka.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.enable-auto-commit:false}")
    private boolean enableAutoCommit;

    @Value("${spring.kafka.heartbeat-interval-ms:3000}")
    private int heartbeatIntervalMs;

    @Value("${spring.kafka.session-timeout-ms:10000}")
    private int sessionTimeoutMs;

    @Value("${spring.kafka.max-poll-interval-ms:300000}")
    private int maxPollIntervalMs;

    @Value("${spring.kafka.max-poll-records:500}")
    private int maxPollRecords;

    @Value("${spring.kafka.fetch-max-wait:500}")
    private int fetchMaxWait;

    @Value("${spring.kafka.concurrency:3}")
    private int concurrency;

    @Value("${spring.kafka.isolation-level:read_committed}")
    private String isolationLevel;

    @Value("${spring.kafka.client-id}")
    private String clientId;

    /**
     * 基础 Consumer 配置
     */
    private Map<String, Object> baseConsumerConfigs() {
        Map<String, Object> configs = new HashMap<>();

        // 基础配置
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        // Offset 管理
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);

        // 心跳与会话
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        // 拉取配置
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);

        // 反序列化
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // 事务隔离级别
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        return configs;
    }

    /**
     * Consumer 工厂
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerConfigs());
    }

    /**
     * Kafka Listener Container 工厂
     * 配置并发消费、手动提交等
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // 设置并发数（每个实例的线程数）
        // 注意：此值应 <= Topic的分区数，否则会有空闲线程
        factory.setConcurrency(concurrency);

        // 设置为批量消费模式
        // factory.setBatchListener(true);

        // 设置提交模式
        // AckMode 有以下几种：
        // - RECORD: 每条消息消费后立即提交
        // - BATCH: poll()返回的一批消息消费完后提交（默认）
        // - TIME: 每隔指定时间提交一次（需配合 ackTime 配置）
        // - COUNT: 每消费指定数量提交一次（需配合 ackCount 配置）
        // - COUNT_TIME: 时间和数量任一满足即提交
        // - MANUAL: 手动调用 acknowledge() 提交
        // - MANUAL_IMMEDIATE: 手动立即提交（调用 acknowledge() 时立即提交）
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 设置错误处理器
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(1000L, 3)  // 重试3次，间隔1秒
        ));

        return factory;
    }

    /**
     * 批量消费的 Container Factory
     * 注意：Spring Kafka 3.x 使用 @KafkaListener(batch = true) 来启用批量消费
     */
    @Bean("batchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(concurrency);

        // 批量消费时的提交模式
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        return factory;
    }

    /**
     * 使用 JSON 反序列化的 Consumer 工厂（用于复杂对象）
     */
    @Bean
    public ConsumerFactory<String, Object> jsonConsumerFactory() {
        Map<String, Object> configs = baseConsumerConfigs();

        JsonDeserializer<Object> deserializer = new JsonDeserializer<>(Object.class);
        deserializer.addTrustedPackages("*");
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), deserializer);
    }
}
