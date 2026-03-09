package Consumer;

import Data.Data;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerApp.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        log.info("Запуск Kafka Consumer приложения");

        String bootstrapServers = KafkaConfig.getBootstrapServersFromEnv();
        String topic = KafkaConfig.getTopicFromEnv();
        String groupId = KafkaConfig.getGroupIdFromEnv();

        log.info(KafkaConfig.getConfigInfo());

        Properties props = KafkaConfig.createConsumerConfig(bootstrapServers, groupId);
        loadPropertiesFromFile(props, "config/consumer.properties");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            log.info("Подписка на топик: {}", topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Data data = mapper.readValue(record.value(), Data.class);
                        log.info("Получено: partition={}, offset={}, key={}, data={}",
                                record.partition(), record.offset(), record.key(), data);
                        processTransaction(data);
                    } catch (Exception e) {
                        log.error("Ошибка обработки сообщения (partition={}, offset={}): {}",
                                record.partition(), record.offset(), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Критическая ошибка consumer: {}", e.getMessage(), e);
        }
    }

    private static void processTransaction(Data data) {
        log.debug("Обработка транзакции: user_id={}, type={}, sum={}",
                data.getUser_id(), data.getType(), data.getSum());
    }

    private static void loadPropertiesFromFile(Properties props, String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            try (InputStream input = new FileInputStream(file)) {
                props.load(input);
                log.info("Загружены настройки из файла: {}", filePath);
            } catch (IOException e) {
                log.warn("Не удалось загрузить настройки из файла: {}", e.getMessage());
            }
        }
    }
}
