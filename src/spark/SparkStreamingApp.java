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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
    private static final double NEWCOMER_HOURS = 5.0 / 60.0; // 5 минут
    //private static final double NEWCOMER_HOURS = 0.5 / 60.0; // 30 секунд
    private static final double SLEEPING_R_MINUTES = 30;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String inputTopic = System.getenv().getOrDefault("KAFKA_TOPIC", "user-transactions");
        String alertsTopic = System.getenv().getOrDefault("KAFKA_ALERTS_TOPIC", "alerts");
        String segmentsTopic = System.getenv().getOrDefault("KAFKA_SEGMENTS_TOPIC", "user-segments");

        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");

        SparkSession spark = SparkSession.builder()
                .appName("SparkTransaction")
                .master(master)
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.streaming.metricsEnabled", "true")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        spark.sparkContext().conf().registerKryoClasses(new Class[]{
                AlertEvent.class,
                SegmentEvent.class,
                Row.class
        });

        StructType transactionSchema = new StructType(new StructField[]{
                new StructField("user_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("firstname", DataTypes.StringType, true, Metadata.empty()),
                new StructField("lastname", DataTypes.StringType, true, Metadata.empty()),
                new StructField("type", DataTypes.StringType, false, Metadata.empty()),
                new StructField("sum", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("event_time", DataTypes.LongType, false, Metadata.empty())
        });

        log.info("=== Spark Streaming Application Started ===");
        log.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        log.info("Input Topic: {}", inputTopic);
        log.info("Alerts Topic: {}", alertsTopic);
        log.info("Segments Topic: {}", segmentsTopic);
        log.info("Master URL: {}", master);
        log.info("Anomaly Window: {} minutes", TimeUnit.MILLISECONDS.toMinutes(WINDOW_ANOMALY_MS));
        log.info("RFM Window: {} hours", TimeUnit.MILLISECONDS.toHours(WINDOW_RFM_MS));
        log.info("===========================================");

        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", inputTopic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load();


        Dataset<Row> parsedStream = rawStream
                .selectExpr("CAST(value AS STRING) as json")
                .select(
                        functions.from_json(functions.col("json"), transactionSchema).as("data")
                )
                .select("data.*")
                .filter(functions.col("user_id").isNotNull());

        Dataset<Row> transactionsForAnomalies = parsedStream.select("user_id", "type", "sum", "event_time");
        Dataset<Row> transactionsForRFM = parsedStream.select("user_id", "type", "sum", "event_time");

        // --- Аномалии ---
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

        // --- RFM ---
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


        Dataset<Row> alertsForKafka = alerts
                .select(
                        functions.col("user_id").cast(DataTypes.StringType).alias("key"),
                        functions.to_json(functions.struct(functions.col("*"))).alias("value")
                );

        StreamingQuery queryAlerts = alertsForKafka.writeStream()
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                    if (!batch.isEmpty()) {
                        List<Row> rows = batch.collectAsList();
                        for (Row row : rows) {
                            try {
                                String key = row.getString(0);
                                String value = row.getString(1);
                                log.warn("!!! ANOMALY DETECTED - Key: {}, Value: {}", key, value);
                            } catch (Exception e) {
                                log.error("Error logging alert", e);
                            }
                        }
                    }

                    // Запись в Kafka
                    batch.write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", bootstrapServers)
                            .option("topic", alertsTopic)
                            .save();
                })
                .option("checkpointLocation", "/tmp/spark-alerts-checkpoint")
                .queryName("alerts-to-kafka")
                .start();

        log.info("Alerts streaming started to topic: {}", alertsTopic);

        StreamingQuery queryAlertsConsole = alerts.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .option("truncate", "false")
                .queryName("alerts-console")
                .start();

        log.info("Alerts console sink started for debugging");

        // Для segments
        Dataset<Row> segmentsForKafka = segments
                .select(
                        functions.col("user_id").cast(DataTypes.StringType).alias("key"),
                        functions.to_json(functions.struct(functions.col("*"))).alias("value")
                );

        StreamingQuery querySegments = segmentsForKafka.writeStream()
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                    if (!batch.isEmpty()) {
                        List<Row> rows = batch.collectAsList();
                        for (Row row : rows) {
                            try {
                                String key = row.getString(0);
                                String value = row.getString(1);
                                log.info("📊 RFM Segment - Key: {}, Value: {}", key, value);
                            } catch (Exception e) {
                                log.error("Error logging segment", e);
                            }
                        }
                    }

                    batch.write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", bootstrapServers)
                            .option("topic", segmentsTopic)
                            .save();
                })
                .option("checkpointLocation", "/tmp/spark-segments-checkpoint")
                .queryName("segments-to-kafka")
                .start();

        log.info("Segments streaming started to topic: {}", segmentsTopic);

        StreamingQuery querySegmentsConsole = segments.writeStream()
                .format("console")
                .outputMode(OutputMode.Append())
                .option("truncate", "false")
                .queryName("segments-console")
                .start();

        log.info("Segments console sink started for debugging");

        log.info("All streams started.");

        spark.streams().awaitAnyTermination();
    }

    /**
     * Функция для детекции аномалий
     */
    static class AnomalyDetectionFunction implements FlatMapGroupsWithStateFunction<Integer, Row, String, AlertEvent> {

        private static final Logger log = LoggerFactory.getLogger(AnomalyDetectionFunction.class);

        @Override
        public Iterator<AlertEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
            List<AlertEvent> out = new ArrayList<>();

            if (state.hasTimedOut()) {
                log.debug("State timed out for user: {}", userId);
                state.remove();
                return out.iterator();
            }

            state.setTimeoutDuration(TimeUnit.MINUTES.toMillis(5));

            String stateStr = state.exists() ? state.get() : "";
            long now = System.currentTimeMillis();

            List<TransactionEntry> entries = parseStateEntries(stateStr, now, WINDOW_ANOMALY_MS);

            int transactionCount = 0;
            while (rows.hasNext()) {
                Row r = rows.next();
                transactionCount++;
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);

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
                }

                entries.add(new TransactionEntry(eventTime, sum, type));
            }

            if (transactionCount > 0 && log.isDebugEnabled()) {
                log.debug("Processed {} transactions for user {}, current window size: {}",
                        transactionCount, userId, entries.size());
            }

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
                        log.error("Failed to parse state entry: {}", e, ex);
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

        private static final Logger log = LoggerFactory.getLogger(RFMSegmentationFunction.class);

        private transient redis.clients.jedis.Jedis jedis;
        private transient ObjectMapper mapper;

        private final ObjectMapper createMapper() {
            ObjectMapper m = new ObjectMapper();
            m.registerModule(new JavaTimeModule());
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            return m;
        }

        @Override
        public Iterator<SegmentEvent> call(Integer userId, Iterator<Row> rows, GroupState<String> state) throws Exception {
            List<SegmentEvent> out = new ArrayList<>();

            if (state.hasTimedOut()) {
                log.debug("State timed out for user: {}", userId);
                state.remove();
                return out.iterator();
            }

            state.setTimeoutDuration(TimeUnit.HOURS.toMillis(24));

            // Инициализация Redis клиента (lazy, внутри executor'а)
            if (jedis == null) {
                String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
                int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
                jedis = new redis.clients.jedis.Jedis(redisHost, redisPort);
                log.info("Connected to Redis at {}:{}", redisHost, redisPort);
            }

            // Инициализация ObjectMapper (lazy)
            if (mapper == null) {
                mapper = createMapper();
            }

            String redisKey = "rfm:" + userId;
            String savedStateJson = jedis.get(redisKey);

            RFMState rfmState;
            if (savedStateJson != null && !savedStateJson.isEmpty()) {
                rfmState = mapper.readValue(savedStateJson, RFMState.class);
                log.debug("Loaded user {} from Redis: f={}, m={}", userId, rfmState.f, rfmState.m);
            } else if (state.exists()) {
                String stateStr = state.get();
                rfmState = parseRFMState(stateStr);
                log.debug("Loaded user {} from Spark state", userId);
            } else {
                rfmState = new RFMState();
                log.debug("New user: {}", userId);
            }

            while (rows.hasNext()) {
                Row r = rows.next();
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);

                rfmState = updateRFMState(rfmState, eventTime, sum, type);
                String segment = calculateSegment(rfmState, eventTime, userId);

                SegmentEvent ev = new SegmentEvent();
                ev.setUser_id(userId);
                ev.setSegment(segment);
                ev.setR_minutes(rfmState.rMinutes);
                ev.setF(rfmState.f);
                ev.setM(rfmState.m);
                ev.setUpdated_at(eventTime);
                out.add(ev);
            }

            if (!out.isEmpty()) {
                String newStateJson = mapper.writeValueAsString(rfmState);
                jedis.set(redisKey, newStateJson);
                jedis.expire(redisKey, 604800);
                log.debug("Saved user {} to Redis: f={}, m={}", userId, rfmState.f, rfmState.m);
            }

            state.update(serializeRFMState(rfmState));

            return out.iterator();
        }

        private RFMState parseRFMState(String stateStr) {
            RFMState state = new RFMState();
            state.setLastTs(0);
            state.setFirstTs(0);
            state.setEntries(new ArrayList<>());

            if (stateStr == null || stateStr.isEmpty()) {
                return state;
            }

            String[] parts = stateStr.split("\\|", 3);
            if (parts.length >= 2) {
                try {
                    state.setLastTs(Long.parseLong(parts[0]));
                    state.setFirstTs(Long.parseLong(parts[1]));

                    if (parts.length == 3 && !parts[2].isEmpty()) {
                        for (String e : parts[2].split(",")) {
                            if (e.isEmpty()) continue;
                            String[] p = e.split(":");
                            if (p.length >= 3) {
                                long ts = Long.parseLong(p[0]);
                                double sum = Double.parseDouble(p[1]);
                                String type = p[2];
                                state.getEntries().add(new TransactionEntry(ts, sum, type));
                            }
                        }
                    }
                } catch (NumberFormatException ex) {
                    log.error("Failed to parse RFM state: {}", stateStr, ex);
                }
            }
            return state;
        }

        private RFMState updateRFMState(RFMState state, long eventTime, double sum, String type) {
            if (state.firstTs == 0) {
                state.firstTs = eventTime;
            }

            state.lastTs = eventTime;

            state.entries.add(new TransactionEntry(eventTime, sum, type));

            state.entries.removeIf(entry -> eventTime - entry.timestamp > WINDOW_RFM_MS);

            state.f = state.entries.size();
            state.m = 0;
            for (TransactionEntry e : state.entries) {
                if ("Deposit".equalsIgnoreCase(e.type)) {
                    state.m += e.sum;
                }
            }
            state.rMinutes = (eventTime - state.lastTs) / 60000.0;

            return state;
        }

        private String calculateSegment(RFMState state, long currentTime, int userId) {
            double firstHoursAgo = (currentTime - state.firstTs) / 3600000.0;

            System.out.println("DEBUG: user=" + userId +
                    ", firstTs=" + state.firstTs +
                    ", currentTime=" + currentTime +
                    ", diffSec=" + (currentTime - state.firstTs) / 1000 +
                    ", firstHoursAgo=" + firstHoursAgo +
                    ", threshold=" + NEWCOMER_HOURS);

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
            sb.append(state.getLastTs()).append("|").append(state.getFirstTs()).append("|");

            for (int i = 0; i < state.getEntries().size(); i++) {
                if (i > 0) sb.append(",");
                TransactionEntry e = state.getEntries().get(i);
                sb.append(e.getTimestamp()).append(":").append(e.getSum()).append(":").append(e.getType());
            }
            return sb.toString();
        }
    }
    /**
     * Вспомогательный класс для хранения транзакции
     */
    /**
     * Вспомогательный класс для хранения транзакции
     */
    static class TransactionEntry {
        private long timestamp;
        private double sum;
        private String type;

        public TransactionEntry() {}

        public TransactionEntry(long timestamp, double sum, String type) {
            this.timestamp = timestamp;
            this.sum = sum;
            this.type = type;
        }

        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

        public double getSum() { return sum; }
        public void setSum(double sum) { this.sum = sum; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    /**
     * Вспомогательный класс для RFM состояния
     */
    /**
     * Вспомогательный класс для RFM состояния
     */
    static class RFMState {
        private long lastTs;
        private long firstTs;
        private List<TransactionEntry> entries = new ArrayList<>();
        private long f;
        private double m;
        private double rMinutes;

        public RFMState() {}

        public long getLastTs() { return lastTs; }
        public void setLastTs(long lastTs) { this.lastTs = lastTs; }

        public long getFirstTs() { return firstTs; }
        public void setFirstTs(long firstTs) { this.firstTs = firstTs; }

        public List<TransactionEntry> getEntries() { return entries; }
        public void setEntries(List<TransactionEntry> entries) { this.entries = entries; }

        public long getF() { return f; }
        public void setF(long f) { this.f = f; }

        public double getM() { return m; }
        public void setM(double m) { this.m = m; }

        public double getRMinutes() { return rMinutes; }
        public void setRMinutes(double rMinutes) { this.rMinutes = rMinutes; }
    }
}