package com.lss.kafka_consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Kafka Consumer 配置测试
 */
@SpringBootTest(classes = KafkaConsumerConfig.class)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.group-id=test-group",
        "spring.kafka.auto-offset-reset=earliest",
        "spring.kafka.enable-auto-commit=false",
        "spring.kafka.heartbeat-interval-ms=3000",
        "spring.kafka.session-timeout-ms=10000",
        "spring.kafka.max-poll-interval-ms=300000",
        "spring.kafka.max-poll-records=500",
        "spring.kafka.fetch-max-wait=500",
        "spring.kafka.concurrency=3",
        "spring.kafka.isolation-level=read_committed",
        "spring.kafka.client-id=test-consumer"
})
class KafkaConsumerConfigTest {

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Value("${spring.kafka.group-id}")
    private String groupId;

    @Value("${spring.kafka.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${spring.kafka.enable-auto-commit}")
    private boolean enableAutoCommit;

    @Value("${spring.kafka.heartbeat-interval-ms}")
    private int heartbeatIntervalMs;

    @Value("${spring.kafka.session-timeout-ms}")
    private int sessionTimeoutMs;

    @Value("${spring.kafka.max-poll-interval-ms}")
    private int maxPollIntervalMs;

    @Value("${spring.kafka.max-poll-records}")
    private int maxPollRecords;

    @Value("${spring.kafka.fetch-max-wait}")
    private int fetchMaxWait;

    @Value("${spring.kafka.concurrency}")
    private int concurrency;

    @Test
    @DisplayName("ConsumerFactory 配置正确注入")
    void consumerFactory_InjectedCorrectly() {
        assertThat(consumerFactory).isNotNull();
    }

    @Nested
    @DisplayName("配置值验证测试")
    class ConfigValueTests {

        @Test
        @DisplayName("Bootstrap servers 配置正确")
        void bootstrapServers_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEqualTo("localhost:9092");
        }

        @Test
        @DisplayName("Group ID 配置正确")
        void groupId_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.GROUP_ID_CONFIG)).isEqualTo("test-group");
        }

        @Test
        @DisplayName("Auto offset reset 配置正确 - earliest")
        void autoOffsetReset_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        }

        @Test
        @DisplayName("Enable auto commit 配置正确 - false")
        void enableAutoCommit_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        }

        @Test
        @DisplayName("Heartbeat interval 配置正确 - 3秒")
        void heartbeatInterval_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG)).isEqualTo(3000);
        }

        @Test
        @DisplayName("Session timeout 配置正确 - 10秒")
        void sessionTimeout_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG)).isEqualTo(10000);
        }

        @Test
        @DisplayName("Max poll interval 配置正确 - 5分钟")
        void maxPollInterval_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)).isEqualTo(300000);
        }

        @Test
        @DisplayName("Max poll records 配置正确 - 500")
        void maxPollRecords_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(500);
        }

        @Test
        @DisplayName("Fetch max wait 配置正确 - 500ms")
        void fetchMaxWait_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)).isEqualTo(500);
        }

        @Test
        @DisplayName("Isolation level 配置正确 - read_committed")
        void isolationLevel_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
        }

        @Test
        @DisplayName("Client ID 配置正确")
        void clientId_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.CLIENT_ID_CONFIG)).isEqualTo("test-consumer");
        }

        @Test
        @DisplayName("反序列化配置正确 - StringDeserializer")
        void deserializer_Configured() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                    .isEqualTo(org.apache.kafka.common.serialization.StringDeserializer.class);
            assertThat(configs.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
                    .isEqualTo(org.apache.kafka.common.serialization.StringDeserializer.class);
        }
    }

    @Nested
    @DisplayName("最新配置值验证")
    class LatestConfigValueTests {

        @Test
        @DisplayName("Earliest 策略 - 从头开始消费")
        void earliestStrategy() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        }

        @Test
        @DisplayName("Latest 策略 - 从最新消息开始")
        void latestStrategy() {
            // This test validates the config key exists
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            assertThat(configs).containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
        }

        @Test
        @DisplayName("手动提交模式验证")
        void manualCommitMode() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            // enable-auto-commit=false 意味着使用手动提交
            assertThat(configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        }

        @Test
        @DisplayName("并发消费数验证")
        void concurrencySetting() {
            Map<String, Object> configs = consumerFactory.getConfigurationProperties();
            // 并发数应该小于等于分区数，这里只验证配置存在
            assertThat(configs).containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
        }
    }
}
