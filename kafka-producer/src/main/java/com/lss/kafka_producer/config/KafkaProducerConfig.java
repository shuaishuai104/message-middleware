package com.lss.kafka_producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Producer 配置类
 * 包含常用生产者和事务生产者配置
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.acks:all}")
    private String acks;

    @Value("${spring.kafka.retries:3}")
    private int retries;

    @Value("${spring.kafka.batch-size:16384}")
    private int batchSize;

    @Value("${spring.kafka.linger-ms:10}")
    private int lingerMs;

    @Value("${spring.kafka.buffer-memory:33554432}")
    private long bufferMemory;

    @Value("${spring.kafka.compression-type:snappy}")
    private String compressionType;

    @Value("${spring.kafka.enable-idempotence:true}")
    private boolean enableIdempotence;

    @Value("${spring.kafka.max-in-flight-requests-per-connection:5}")
    private int maxInFlightRequests;

    @Value("${spring.kafka.delivery-timeout-ms:120000}")
    private int deliveryTimeoutMs;

    @Value("${spring.kafka.request-timeout-ms:30000}")
    private int requestTimeoutMs;

    @Value("${spring.kafka.transaction-timeout-ms:60000}")
    private int transactionTimeoutMs;

    @Value("${spring.kafka.transaction-id-prefix:}")
    private String transactionIdPrefix;

    /**
     * 基础 Producer 配置
     */
    private Map<String, Object> baseProducerConfigs() {
        Map<String, Object> configs = new HashMap<>();

        // 基础配置
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 确认机制
        configs.put(ProducerConfig.ACKS_CONFIG, acks);

        // 重试配置
        configs.put(ProducerConfig.RETRIES_CONFIG, retries);
        configs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

        // 批处理配置
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);

        // 压缩
        configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        // 幂等性
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);

        // 请求配置
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlightRequests);
        configs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        configs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        // 事务超时
        configs.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);

        return configs;
    }

    /**
     * 普通 Producer 工厂
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProducerConfigs());
    }

    /**
     * KafkaTemplate - 用于普通消息发送
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * 事务 Producer 工厂
     * 只有配置了 transaction-id-prefix 时才创建
     */
    @Bean
    @ConditionalOnProperty(name = "spring.kafka.transaction-id-prefix")
    public ProducerFactory<String, String> transactionProducerFactory() {
        Map<String, Object> configs = baseProducerConfigs();
        // 事务模式下，enable-idempotence 必须为 true
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    /**
     * 事务管理器 - 用于Kafka事务
     * 只有配置了 transaction-id-prefix 时才创建
     */
    @Bean
    @ConditionalOnProperty(name = "spring.kafka.transaction-id-prefix")
    public KafkaTransactionManager<String, String> transactionManager() {
        return new KafkaTransactionManager<>(transactionProducerFactory());
    }
}
