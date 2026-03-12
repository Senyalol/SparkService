package spark;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Spark Structured Streaming: чтение транзакций из Kafka,
 * детекция аномалий (Credit >= 3 * avg_check_5min) и RFM-сегментация.
 */
public class SparkStreamingApp {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamingApp.class);
    private static final long WINDOW_ANOMALY_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long WINDOW_RFM_MS = TimeUnit.HOURS.toMillis(24);
    private static final long NEWCOMER_HOURS = 1;
    private static final double SLEEPING_R_MINUTES = 30;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "user-transactions");
        String alertsTopic = System.getenv().getOrDefault("KAFKA_ALERTS_TOPIC", "alerts");
        String segmentsTopic = System.getenv().getOrDefault("KAFKA_SEGMENTS_TOPIC", "user-segments");

        // Определяем master URL из переменной окружения или используем local[*] по умолчанию
        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");

        SparkSession spark = SparkSession.builder()
                .appName("SparkTransaction")
                .master(master)
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType transactionSchema = new StructType(new StructField[]{
                new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("firstname", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastname", DataTypes.StringType, true, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("sum", DataTypes.DoubleType, false, Metadata.empty())
        });

        System.out.println(">>> Spark Streaming Application Started");
        System.out.println(">>> Kafka Bootstrap Servers: " + bootstrapServers);
        System.out.println(">>> Input Topic: " + inputTopic);
        System.out.println(">>> Alerts Topic: " + alertsTopic);
        System.out.println(">>> Segments Topic: " + segmentsTopic);

        // --- Источник: Kafka user-transactions ---
        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "latest")
                .load();

        // Парсинг JSON из Kafka
        Dataset<Row> parsedStream = rawStream
                .selectExpr("CAST(value AS STRING) as json", "CAST(timestamp AS LONG) as event_time")
                .select(
                        functions.from_json(functions.col("json"), transactionSchema).as("data"),
                        functions.col("event_time")
                )
                .select("data.*", "event_time")
                .filter(functions.col("user_id").isNotNull());

        // Разделяем поток для двух разных обработчиков
        // Поток для аномалий
        Dataset<Row> transactionsForAnomalies = parsedStream.select("user_id", "type", "sum", "event_time");

        // Поток для RFM
        Dataset<Row> transactionsForRFM = parsedStream.select("user_id", "type", "sum", "event_time");

        // --- Аномалии: state = "ts1:amt1,ts2:amt2,..." за последние 5 мин ---
        KeyValueGroupedDataset<Integer, Row> byUserForAnomalies = transactionsForAnomalies
                .groupByKey(
                        (MapFunction<Row, Integer>) row -> row.getInt(0),
                        Encoders.INT()
                );

        Dataset<AlertEvent> alerts = byUserForAnomalies
                .flatMapGroupsWithState(
                        new AnomalyDetectionFunction(),
                        OutputMode.Append(),
                        Encoders.STRING(),
                        Encoders.bean(AlertEvent.class),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );

        // --- RFM: state = "last_ts|first_ts|ts1:amt1:type1,ts2:amt2:type2,..." за 24ч ---
        KeyValueGroupedDataset<Integer, Row> byUserForRFM = transactionsForRFM
                .groupByKey(
                        (MapFunction<Row, Integer>) row -> row.getInt(0),
                        Encoders.INT()
                );

        Dataset<SegmentEvent> segments = byUserForRFM
                .flatMapGroupsWithState(
                        new RFMSegmentationFunction(),
                        OutputMode.Append(),
                        Encoders.STRING(),
                        Encoders.bean(SegmentEvent.class),
                        GroupStateTimeout.ProcessingTimeTimeout()
                );

        // --- Запись в Kafka (key=user_id, value=JSON) ---
        // Для alerts
        Dataset<Row> alertsForKafka = alerts.select(
                functions.col("user_id").cast(DataTypes.StringType).alias("key"),
                functions.to_json(functions.struct(functions.col("*"))).alias("value")
        );

        StreamingQuery queryAlerts = alertsForKafka.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("topic", alertsTopic)
                .option("checkpointLocation", "/tmp/spark-alerts-checkpoint")
                .outputMode(OutputMode.Append())
                .start();

        log.info("Alerts streaming started to topic: {}", alertsTopic);

        // Console sink для alerts (для отладки)
        StreamingQuery queryAlertsConsole = alerts.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .option("truncate", "false")
                .start();

        log.info("Alerts console sink started for debugging");

        // Для segments
        Dataset<Row> segmentsForKafka = segments.select(
                functions.col("user_id").cast(DataTypes.StringType).alias("key"),
                functions.to_json(functions.struct(functions.col("*"))).alias("value")
        );

        StreamingQuery querySegments = segmentsForKafka.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("topic", segmentsTopic)
                .option("checkpointLocation", "/tmp/spark-segments-checkpoint")
                .outputMode(OutputMode.Append())
                .start();

        log.info("Segments streaming started to topic: {}", segmentsTopic);

        // Console sink для segments (для отладки)
        StreamingQuery querySegmentsConsole = segments.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .option("truncate", "false")
                .start();

        log.info("Segments console sink started for debugging");

        log.info("Streaming applications started");
        System.out.println(">>> All streams started. Waiting for data...");

        // Ждем завершения (в реальном приложении нужно awaitTermination для обоих)
        spark.streams().awaitAnyTermination();
    }

    /**
     * Функция для детекции аномалий
     */
    static class AnomalyDetectionFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, AlertEvent> {

        @Override
        public Iterator<AlertEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
            List<AlertEvent> out = new ArrayList<>();

            if (state.hasTimedOut()) {
                state.remove();
                return out.iterator();
            }

            state.setTimeoutDuration(TimeUnit.MINUTES.toMillis(5));

            String stateStr = state.exists() ? state.get() : "";
            long now = System.currentTimeMillis();

            // Парсим существующее состояние
            List<TransactionEntry> entries = parseStateEntries(stateStr, now, WINDOW_ANOMALY_MS);

            while (rows.hasNext()) {
                Row r = rows.next();
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);

                // Вычисляем среднее по существующим записям
                double avg = calculateAverage(entries);

                // Правило аномалии: Credit и сумма >= 3 * avg_check_5min
                if ("Credit".equalsIgnoreCase(type) && avg > 0 && sum >= 3 * avg) {
                    AlertEvent alert = new AlertEvent();
                    alert.setUser_id(userId);
                    alert.setEvent_time(eventTime);
                    alert.setType(type);
                    alert.setSum(sum);
                    alert.setAvg_check_5min(avg);
                    alert.setMessage("Credit >= 3 * avg_check_5min");
                    out.add(alert);

                    // Логируем аномалию в консоль
                    System.out.println("!!! ANOMALY DETECTED !!!");
                    System.out.println("User: " + userId + ", Amount: " + sum + ", Avg: " + avg);
                    System.out.println("Alert JSON: " + objectMapper.writeValueAsString(alert));
                }

                // Добавляем новую транзакцию
                entries.add(new TransactionEntry(eventTime, sum, type));
            }

            // Обновляем состояние
            state.update(serializeEntries(entries));

            return out.iterator();
        }

        private List<TransactionEntry> parseStateEntries(String stateStr, long now, long windowMs) {
            List<TransactionEntry> entries = new ArrayList<>();
            if (stateStr == null || stateStr.isEmpty()) {
                return entries;
            }

            for (String e : stateStr.split(",")) {
                if (e.isEmpty()) continue;
                String[] parts = e.split(":");
                if (parts.length >= 2) {
                    try {
                        long ts = Long.parseLong(parts[0]);
                        if (now - ts <= windowMs) {
                            double sum = Double.parseDouble(parts[1]);
                            String type = parts.length >= 3 ? parts[2] : "";
                            entries.add(new TransactionEntry(ts, sum, type));
                        }
                    } catch (NumberFormatException ex) {
                        // Игнорируем некорректные записи
                    }
                }
            }
            return entries;
        }

        private double calculateAverage(List<TransactionEntry> entries) {
            if (entries.isEmpty()) return 0;
            double total = 0;
            for (TransactionEntry e : entries) {
                total += e.sum;
            }
            return total / entries.size();
        }

        private String serializeEntries(List<TransactionEntry> entries) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < entries.size(); i++) {
                if (i > 0) sb.append(",");
                TransactionEntry e = entries.get(i);
                sb.append(e.timestamp).append(":").append(e.sum).append(":").append(e.type);
            }
            return sb.toString();
        }
    }

    /**
     * Функция для RFM-сегментации
     */
    static class RFMSegmentationFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, SegmentEvent> {

        @Override
        public Iterator<SegmentEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
            List<SegmentEvent> out = new ArrayList<>();

            if (state.hasTimedOut()) {
                state.remove();
                return out.iterator();
            }

            state.setTimeoutDuration(TimeUnit.HOURS.toMillis(24));

            String stateStr = state.exists() ? state.get() : "";
            long now = System.currentTimeMillis();

            // Парсим существующее состояние
            RFMState rfmState = parseRFMState(stateStr, now);

            while (rows.hasNext()) {
                Row r = rows.next();
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);

                // Обновляем RFM состояние
                rfmState = updateRFMState(rfmState, eventTime, sum, type, now);

                // Вычисляем сегмент
                String segment = calculateSegment(rfmState, now);

                // Создаем событие сегмента
                SegmentEvent ev = new SegmentEvent();
                ev.setUser_id(userId);
                ev.setSegment(segment);
                ev.setR_minutes(rfmState.rMinutes);
                ev.setF(rfmState.f);
                ev.setM(rfmState.m);
                ev.setUpdated_at(now);
                out.add(ev);

                // Логируем сегмент в консоль
                System.out.println(">>> RFM Segment for user " + userId + ": " + segment +
                        " (f=" + rfmState.f + ", m=" + rfmState.m + ", r=" + rfmState.rMinutes + ")");
                System.out.println(">>> Segment JSON: " + objectMapper.writeValueAsString(ev));
            }

            // Обновляем состояние
            state.update(serializeRFMState(rfmState));

            return out.iterator();
        }

        private RFMState parseRFMState(String stateStr, long now) {
            RFMState state = new RFMState();
            state.lastTs = 0;
            state.firstTs = 0;
            state.entries = new ArrayList<>();

            if (stateStr == null || stateStr.isEmpty()) {
                return state;
            }

            String[] parts = stateStr.split("\\|", 3);
            if (parts.length >= 2) {
                try {
                    state.lastTs = Long.parseLong(parts[0]);
                    state.firstTs = Long.parseLong(parts[1]);

                    if (parts.length == 3 && !parts[2].isEmpty()) {
                        for (String e : parts[2].split(",")) {
                            if (e.isEmpty()) continue;
                            String[] p = e.split(":");
                            if (p.length >= 3) {
                                long ts = Long.parseLong(p[0]);
                                if (now - ts <= WINDOW_RFM_MS) {
                                    double sum = Double.parseDouble(p[1]);
                                    String type = p[2];
                                    state.entries.add(new TransactionEntry(ts, sum, type));
                                }
                            }
                        }
                    }
                } catch (NumberFormatException ex) {
                    // Игнорируем
                }
            }
            return state;
        }

        private RFMState updateRFMState(RFMState state, long eventTime, double sum, String type, long now) {
            if (state.firstTs == 0) {
                state.firstTs = eventTime;
            }
            state.lastTs = eventTime;

            // Добавляем новую транзакцию
            state.entries.add(new TransactionEntry(eventTime, sum, type));

            // Удаляем старые транзакции
            state.entries.removeIf(entry -> now - entry.timestamp > WINDOW_RFM_MS);

            // Вычисляем метрики
            state.f = state.entries.size();
            state.m = 0;
            for (TransactionEntry e : state.entries) {
                if ("Deposit".equalsIgnoreCase(e.type)) {
                    state.m += e.sum;
                }
            }
            state.rMinutes = (now - state.lastTs) / 60000.0;

            return state;
        }

        private String calculateSegment(RFMState state, long now) {
            double firstHoursAgo = (now - state.firstTs) / 3600000.0;

            if (firstHoursAgo < NEWCOMER_HOURS) {
                return "Новичок";
            } else if (state.m > 10000 && state.f > 5) {
                return "VIP";
            } else if (state.m > 1000 && state.f > 1) {
                return "Активный";
            } else if (state.rMinutes > SLEEPING_R_MINUTES) {
                return "Спящий";
            } else {
                return "Стандартный";
            }
        }

        private String serializeRFMState(RFMState state) {
            StringBuilder sb = new StringBuilder();
            sb.append(state.lastTs).append("|").append(state.firstTs).append("|");

            for (int i = 0; i < state.entries.size(); i++) {
                if (i > 0) sb.append(",");
                TransactionEntry e = state.entries.get(i);
                sb.append(e.timestamp).append(":").append(e.sum).append(":").append(e.type);
            }
            return sb.toString();
        }
    }

    /**
     * Вспомогательный класс для хранения транзакции
     */
    static class TransactionEntry {
        long timestamp;
        double sum;
        String type;

        TransactionEntry(long timestamp, double sum, String type) {
            this.timestamp = timestamp;
            this.sum = sum;
            this.type = type;
        }
    }

    /**
     * Вспомогательный класс для RFM состояния
     */
    static class RFMState {
        long lastTs;
        long firstTs;
        List<TransactionEntry> entries;
        long f;
        double m;
        double rMinutes;
    }
}

//package spark;
//
//import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.streaming.GroupState;
//import org.apache.spark.sql.streaming.GroupStateTimeout;
//import org.apache.spark.sql.streaming.OutputMode;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.StreamingQueryException;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.Metadata;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
///**
// * Spark Structured Streaming: чтение транзакций из Kafka,
// * детекция аномалий (Credit >= 3 * avg_check_5min) и RFM-сегментация.
// */
//public class SparkStreamingApp {
//
//    private static final Logger log = LoggerFactory.getLogger(SparkStreamingApp.class);
//    private static final long WINDOW_ANOMALY_MS = TimeUnit.MINUTES.toMillis(5);
//    private static final long WINDOW_RFM_MS = TimeUnit.HOURS.toMillis(24);
//    private static final long NEWCOMER_HOURS = 1;
//    private static final double SLEEPING_R_MINUTES = 30;
//
//    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
//        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
//        String inputTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "user-transactions");
//        String alertsTopic = System.getenv().getOrDefault("KAFKA_ALERTS_TOPIC", "alerts");
//        String segmentsTopic = System.getenv().getOrDefault("KAFKA_SEGMENTS_TOPIC", "user-segments");
//
//        // Определяем master URL из переменной окружения или используем local[*] по умолчанию
//        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");
//
//        SparkSession spark = SparkSession.builder()
//                .appName("SparkTransaction")
//                .master(master)
//                .config("spark.sql.shuffle.partitions", "2")
//                .getOrCreate();
//
//        spark.sparkContext().setLogLevel("WARN");
//
//        StructType transactionSchema = new StructType(new StructField[]{
//                new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("firstname", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("lastname", DataTypes.StringType, true, Metadata.empty()),
//                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("sum", DataTypes.DoubleType, false, Metadata.empty())
//        });
//
//        // --- Источник: Kafka user-transactions ---
//        Dataset<Row> rawStream = spark.readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", bootstrapServers)
//                .option("subscribe", inputTopic)
//                .option("startingOffsets", "latest")
//                .load();
//
//        // Парсинг JSON из Kafka
//        Dataset<Row> parsedStream = rawStream
//                .selectExpr("CAST(value AS STRING) as json", "CAST(timestamp AS LONG) as event_time")
//                .select(
//                        functions.from_json(functions.col("json"), transactionSchema).as("data"),
//                        functions.col("event_time")
//                )
//                .select("data.*", "event_time")
//                .filter(functions.col("user_id").isNotNull());
//
//        // Разделяем поток для двух разных обработчиков
//        //parsedStream.cache(); // Кэшируем, чтобы не читать дважды
//
//        // Поток для аномалий
//        Dataset<Row> transactionsForAnomalies = parsedStream.select("user_id", "type", "sum", "event_time");
//
//        // Поток для RFM
//        Dataset<Row> transactionsForRFM = parsedStream.select("user_id", "type", "sum", "event_time");
//
//        // --- Аномалии: state = "ts1:amt1,ts2:amt2,..." за последние 5 мин ---
//        KeyValueGroupedDataset<Integer, Row> byUserForAnomalies = transactionsForAnomalies
//                .groupByKey(
//                        (MapFunction<Row, Integer>) row -> row.getInt(0),
//                        Encoders.INT()
//                );
//
//        Dataset<AlertEvent> alerts = byUserForAnomalies
//                .flatMapGroupsWithState(
//                        new AnomalyDetectionFunction(),
//                        OutputMode.Append(),
//                        Encoders.STRING(),
//                        Encoders.bean(AlertEvent.class),
//                        GroupStateTimeout.ProcessingTimeTimeout()
//                );
//
//        // --- RFM: state = "last_ts|first_ts|ts1:amt1:type1,ts2:amt2:type2,..." за 24ч ---
//        KeyValueGroupedDataset<Integer, Row> byUserForRFM = transactionsForRFM
//                .groupByKey(
//                        (MapFunction<Row, Integer>) row -> row.getInt(0),
//                        Encoders.INT()
//                );
//
//        Dataset<SegmentEvent> segments = byUserForRFM
//                .flatMapGroupsWithState(
//                        new RFMSegmentationFunction(),
//                        OutputMode.Append(),
//                        Encoders.STRING(),
//                        Encoders.bean(SegmentEvent.class),
//                        GroupStateTimeout.ProcessingTimeTimeout()
//                );
//
//        // --- Запись в Kafka (key=user_id, value=JSON) ---
//        Dataset<Row> alertsForKafka = alerts.select(
//                functions.col("user_id").cast(DataTypes.StringType).alias("key"),
//                functions.to_json(functions.struct(functions.col("*"))).alias("value")
//        );
//
//            StreamingQuery queryAlerts = alertsForKafka.writeStream()
//                    .format("kafka")
//                    .option("kafka.bootstrap.servers", bootstrapServers)
//                    .option("topic", alertsTopic)
//                    .option("checkpointLocation", "/tmp/spark-alerts-checkpoint")
//                    .outputMode(OutputMode.Append())
//                    .start();
//
//            log.info("Alerts streaming started to topic: {}", alertsTopic);
//
//
//        Dataset<Row> segmentsForKafka = segments.select(
//                functions.col("user_id").cast(DataTypes.StringType).alias("key"),
//                functions.to_json(functions.struct(functions.col("*"))).alias("value")
//        );
//
//            StreamingQuery querySegments = segmentsForKafka.writeStream()
//                    .format("kafka")
//                    .option("kafka.bootstrap.servers", bootstrapServers)
//                    .option("topic", segmentsTopic)
//                    .option("checkpointLocation", "/tmp/spark-segments-checkpoint")
//                    .outputMode(OutputMode.Append())
//                    .start();
//
//            log.info("Segments streaming started to topic: {}", segmentsTopic);
//
//
//        log.info("Streaming applications started");
//
//        // Ждем завершения (в реальном приложении нужно awaitTermination для обоих)
//        spark.streams().awaitAnyTermination();
//    }
//
//    /**
//     * Функция для детекции аномалий
//     */
//    static class AnomalyDetectionFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, AlertEvent> {
//
//        @Override
//        public Iterator<AlertEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
//            List<AlertEvent> out = new ArrayList<>();
//
//            if (state.hasTimedOut()) {
//                state.remove();
//                return out.iterator();
//            }
//
//            state.setTimeoutDuration(TimeUnit.MINUTES.toMillis(5));
//
//            String stateStr = state.exists() ? state.get() : "";
//            long now = System.currentTimeMillis();
//
//            // Парсим существующее состояние
//            List<TransactionEntry> entries = parseStateEntries(stateStr, now, WINDOW_ANOMALY_MS);
//
//            while (rows.hasNext()) {
//                Row r = rows.next();
//                long eventTime = r.getLong(3);
//                String type = r.getString(1);
//                double sum = r.getDouble(2);
//
//                // Вычисляем среднее по существующим записям
//                double avg = calculateAverage(entries);
//
//                // Правило аномалии: Credit и сумма >= 3 * avg_check_5min
//                if ("Credit".equalsIgnoreCase(type) && avg > 0 && sum >= 3 * avg) {
//                    AlertEvent alert = new AlertEvent();
//                    alert.setUser_id(userId);
//                    alert.setEvent_time(eventTime);
//                    alert.setType(type);
//                    alert.setSum(sum);
//                    alert.setAvg_check_5min(avg);
//                    alert.setMessage("Credit >= 3 * avg_check_5min");
//                    out.add(alert);
//                }
//
//                // Добавляем новую транзакцию
//                entries.add(new TransactionEntry(eventTime, sum, type));
//            }
//
//            // Обновляем состояние
//            state.update(serializeEntries(entries));
//
//            return out.iterator();
//        }
//
//        private List<TransactionEntry> parseStateEntries(String stateStr, long now, long windowMs) {
//            List<TransactionEntry> entries = new ArrayList<>();
//            if (stateStr == null || stateStr.isEmpty()) {
//                return entries;
//            }
//
//            for (String e : stateStr.split(",")) {
//                if (e.isEmpty()) continue;
//                String[] parts = e.split(":");
//                if (parts.length >= 2) {
//                    try {
//                        long ts = Long.parseLong(parts[0]);
//                        if (now - ts <= windowMs) {
//                            double sum = Double.parseDouble(parts[1]);
//                            String type = parts.length >= 3 ? parts[2] : "";
//                            entries.add(new TransactionEntry(ts, sum, type));
//                        }
//                    } catch (NumberFormatException ex) {
//                        // Игнорируем некорректные записи
//                    }
//                }
//            }
//            return entries;
//        }
//
//        private double calculateAverage(List<TransactionEntry> entries) {
//            if (entries.isEmpty()) return 0;
//            double total = 0;
//            for (TransactionEntry e : entries) {
//                total += e.sum;
//            }
//            return total / entries.size();
//        }
//
//        private String serializeEntries(List<TransactionEntry> entries) {
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < entries.size(); i++) {
//                if (i > 0) sb.append(",");
//                TransactionEntry e = entries.get(i);
//                sb.append(e.timestamp).append(":").append(e.sum).append(":").append(e.type);
//            }
//            return sb.toString();
//        }
//    }
//
//    /**
//     * Функция для RFM-сегментации
//     */
//    static class RFMSegmentationFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, SegmentEvent> {
//
//        @Override
//        public Iterator<SegmentEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
//            List<SegmentEvent> out = new ArrayList<>();
//
//            if (state.hasTimedOut()) {
//                state.remove();
//                return out.iterator();
//            }
//
//            state.setTimeoutDuration(TimeUnit.HOURS.toMillis(24));
//
//            String stateStr = state.exists() ? state.get() : "";
//            long now = System.currentTimeMillis();
//
//            // Парсим существующее состояние
//            RFMState rfmState = parseRFMState(stateStr, now);
//
//            while (rows.hasNext()) {
//                Row r = rows.next();
//                long eventTime = r.getLong(3);
//                String type = r.getString(1);
//                double sum = r.getDouble(2);
//
//                // Обновляем RFM состояние
//                rfmState = updateRFMState(rfmState, eventTime, sum, type, now);
//
//                // Вычисляем сегмент
//                String segment = calculateSegment(rfmState, now);
//
//                // Создаем событие сегмента
//                SegmentEvent ev = new SegmentEvent();
//                ev.setUser_id(userId);
//                ev.setSegment(segment);
//                ev.setR_minutes(rfmState.rMinutes);
//                ev.setF(rfmState.f);
//                ev.setM(rfmState.m);
//                ev.setUpdated_at(now);
//                out.add(ev);
//            }
//
//            // Обновляем состояние
//            state.update(serializeRFMState(rfmState));
//
//            return out.iterator();
//        }
//
//        private RFMState parseRFMState(String stateStr, long now) {
//            RFMState state = new RFMState();
//            state.lastTs = 0;
//            state.firstTs = 0;
//            state.entries = new ArrayList<>();
//
//            if (stateStr == null || stateStr.isEmpty()) {
//                return state;
//            }
//
//            String[] parts = stateStr.split("\\|", 3);
//            if (parts.length >= 2) {
//                try {
//                    state.lastTs = Long.parseLong(parts[0]);
//                    state.firstTs = Long.parseLong(parts[1]);
//
//                    if (parts.length == 3 && !parts[2].isEmpty()) {
//                        for (String e : parts[2].split(",")) {
//                            if (e.isEmpty()) continue;
//                            String[] p = e.split(":");
//                            if (p.length >= 3) {
//                                long ts = Long.parseLong(p[0]);
//                                if (now - ts <= WINDOW_RFM_MS) {
//                                    double sum = Double.parseDouble(p[1]);
//                                    String type = p[2];
//                                    state.entries.add(new TransactionEntry(ts, sum, type));
//                                }
//                            }
//                        }
//                    }
//                } catch (NumberFormatException ex) {
//                    // Игнорируем
//                }
//            }
//            return state;
//        }
//
//        private RFMState updateRFMState(RFMState state, long eventTime, double sum, String type, long now) {
//            if (state.firstTs == 0) {
//                state.firstTs = eventTime;
//            }
//            state.lastTs = eventTime;
//
//            // Добавляем новую транзакцию
//            state.entries.add(new TransactionEntry(eventTime, sum, type));
//
//            // Удаляем старые транзакции
//            state.entries.removeIf(entry -> now - entry.timestamp > WINDOW_RFM_MS);
//
//            // Вычисляем метрики
//            state.f = state.entries.size();
//            state.m = 0;
//            for (TransactionEntry e : state.entries) {
//                if ("Deposit".equalsIgnoreCase(e.type)) {
//                    state.m += e.sum;
//                }
//            }
//            state.rMinutes = (now - state.lastTs) / 60000.0;
//
//            return state;
//        }
//
//        private String calculateSegment(RFMState state, long now) {
//            double firstHoursAgo = (now - state.firstTs) / 3600000.0;
//
//            if (firstHoursAgo < NEWCOMER_HOURS) {
//                return "Новичок";
//            } else if (state.m > 10000 && state.f > 5) {
//                return "VIP";
//            } else if (state.m > 1000 && state.f > 1) {
//                return "Активный";
//            } else if (state.rMinutes > SLEEPING_R_MINUTES) {
//                return "Спящий";
//            } else {
//                return "Стандартный";
//            }
//        }
//
//        private String serializeRFMState(RFMState state) {
//            StringBuilder sb = new StringBuilder();
//            sb.append(state.lastTs).append("|").append(state.firstTs).append("|");
//
//            for (int i = 0; i < state.entries.size(); i++) {
//                if (i > 0) sb.append(",");
//                TransactionEntry e = state.entries.get(i);
//                sb.append(e.timestamp).append(":").append(e.sum).append(":").append(e.type);
//            }
//            return sb.toString();
//        }
//    }
//
//    /**
//     * Вспомогательный класс для хранения транзакции
//     */
//    static class TransactionEntry {
//        long timestamp;
//        double sum;
//        String type;
//
//        TransactionEntry(long timestamp, double sum, String type) {
//            this.timestamp = timestamp;
//            this.sum = sum;
//            this.type = type;
//        }
//    }
//
//    /**
//     * Вспомогательный класс для RFM состояния
//     */
//    static class RFMState {
//        long lastTs;
//        long firstTs;
//        List<TransactionEntry> entries;
//        long f;
//        double m;
//        double rMinutes;
//    }
//}