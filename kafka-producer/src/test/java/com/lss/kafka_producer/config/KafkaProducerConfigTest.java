package com.lss.kafka_producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Producer 配置测试
 */
@SpringBootTest(classes = KafkaProducerConfig.class)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.acks=all",
        "spring.kafka.retries=5",
        "spring.kafka.batch-size=32768",
        "spring.kafka.linger-ms=20",
        "spring.kafka.buffer-memory=67108864",
        "spring.kafka.compression-type=gzip",
        "spring.kafka.enable-idempotence=true",
        "spring.kafka.max-in-flight-requests-per-connection=5",
        "spring.kafka.delivery-timeout-ms=180000",
        "spring.kafka.request-timeout-ms=45000",
        "spring.kafka.transaction-timeout-ms=90000"
})
class KafkaProducerConfigTest {

    @Autowired
    private ProducerFactory<String, String> producerFactory;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.acks}")
    private String acks;

    @Value("${spring.kafka.retries}")
    private int retries;

    @Test
    @DisplayName("ProducerFactory 配置正确注入")
    void producerFactory_InjectedCorrectly() {
        assertThat(producerFactory).isNotNull();
        assertThat(producerFactory).isInstanceOf(DefaultKafkaProducerFactory.class);
    }

    @Nested
    @DisplayName("配置值验证测试")
    class ConfigValueTests {

        @Test
        @DisplayName("Bootstrap servers 配置正确")
        void bootstrapServers_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        }

        @Test
        @DisplayName("Acks 配置正确 - all")
        void acks_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        }

        @Test
        @DisplayName("Retries 配置正确")
        void retries_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.RETRIES_CONFIG)).isEqualTo(5);
        }

        @Test
        @DisplayName("Batch size 配置正确 - 32KB")
        void batchSize_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.BATCH_SIZE_CONFIG)).isEqualTo(32768);
        }

        @Test
        @DisplayName("Linger ms 配置正确 - 20ms")
        void lingerMs_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.LINGER_MS_CONFIG)).isEqualTo(20);
        }

        @Test
        @DisplayName("Buffer memory 配置正确 - 64MB")
        void bufferMemory_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG)).isEqualTo(67108864L);
        }

        @Test
        @DisplayName("压缩类型配置正确 - gzip")
        void compressionType_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)).isEqualTo("gzip");
        }

        @Test
        @DisplayName("幂等性配置正确 - 开启")
        void idempotence_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
        }

        @Test
        @DisplayName("Max in-flight requests 配置正确")
        void maxInFlightRequests_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)).isEqualTo(5);
        }

        @Test
        @DisplayName("Delivery timeout 配置正确 - 3分钟")
        void deliveryTimeout_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).isEqualTo(180000);
        }

        @Test
        @DisplayName("Request timeout 配置正确 - 45秒")
        void requestTimeout_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).isEqualTo(45000);
        }

        @Test
        @DisplayName("序列化配置正确 - StringSerializer")
        void serializer_Configured() {
            Map<String, Object> configs = producerFactory.getConfigurationProperties();
            assertThat(configs.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                    .isEqualTo(org.apache.kafka.common.serialization.StringSerializer.class);
            assertThat(configs.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                    .isEqualTo(org.apache.kafka.common.serialization.StringSerializer.class);
        }
    }
}
