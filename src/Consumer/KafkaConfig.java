package Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaConfig {

    // В контейнере передаётся KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String DEFAULT_TOPIC = "user-transactions";
    private static final String DEFAULT_GROUP_ID = "spark-transaction-consumer";

    public static Properties createConsumerConfig(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        return props;
    }

    public static Properties createDefaultConfig() {
        return createConsumerConfig(DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_GROUP_ID);
    }

    public static String getBootstrapServersFromEnv() {
        String env = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return env != null ? env : DEFAULT_BOOTSTRAP_SERVERS;
    }

    public static String getTopicFromEnv() {
        String env = System.getenv("KAFKA_TOPIC");
        return env != null ? env : DEFAULT_TOPIC;
    }

    public static String getGroupIdFromEnv() {
        String env = System.getenv("KAFKA_GROUP_ID");
        return env != null ? env : DEFAULT_GROUP_ID;
    }

    public static String getConfigInfo() {
        return String.format("Kafka Consumer Config: bootstrap=%s, topic=%s, groupId=%s",
                getBootstrapServersFromEnv(), getTopicFromEnv(), getGroupIdFromEnv());
    }
}
