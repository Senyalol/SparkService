//package producerKafka;
//
//import org.apache.kafka.clients.admin.AdminClient;
//import org.apache.kafka.clients.admin.CreateTopicsResult;
//import org.apache.kafka.clients.admin.NewTopic;
//
//import java.util.Collections;
//import java.util.Properties;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//public class ProducerConfig {
//
//    private static final String SEGMENT_TOPIC = "spark-segments";
//    private static final String ANOMALY_TOPIC = "spark-anomaly";
//
//    private static final int SEGMENTS_PARTITIONS = 2;
//    private static final int ANOMALY_PARTITIONS = 2;
//
//    public static Properties createProducerConfig(String bootstrapServers){
//        Properties props = new Properties();
//
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
//
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all"); //Подтверждение отправки
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3); //Кол-во отправок сообщения при ошибке
//
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, 5); // Задержка отправки
//        props.put(org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG, 16384); //Размер  группы  сообщений в буфере
//
//        return props;
//    }
//
//    public static void createTopic(String bootstrap ,String topicName, int partitions, short replication){
//
//        Properties props = createProducerConfig(bootstrap);
//
//        try(AdminClient adminClient = AdminClient.create(props)) {
//
//            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replication);
//
//            CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
//
//            result.all().get(10, TimeUnit.SECONDS);
//        }
//        catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            //Логи
//        }
//        catch (ExecutionException e) {
//            if (e.getCause().getMessage().contains("already exists")) {
//                // Логги
//            } else {
//                //Логги
//            }
//        }
//        catch (TimeoutException e) {
//            //Логги
//        }
//
//    }
//
//    public static void createSegmentTopicWithPartition(String bootstrap, String topicName){
//        createTopic(bootstrap, topicName, SEGMENTS_PARTITIONS, (short) 1);
//    }
//
//    public static void createAnomalyTopicWithPartition(String bootstrap, String topicName){
//        createTopic(bootstrap, topicName, ANOMALY_PARTITIONS, (short) 1);
//    }
//
//}