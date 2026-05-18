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
import com.fasterxml.jackson.annotation.JsonProperty;
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
    private static final long WINDOW_RFM_MS = TimeUnit.MINUTES.toMillis(5);
    private static final double NEWCOMER_HOURS = 5.0 / 60.0; // 5 минут
    //private static final double NEWCOMER_HOURS = 0.5 / 60.0; // 30 секунд
    private static final double SLEEPING_R_MINUTES = 30;
    private static final String ANOMALY_STATE_SEP = "||";

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
        log.info("RFM Window: {} minutes", TimeUnit.MILLISECONDS.toMinutes(WINDOW_RFM_MS));
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

        // Для segments: явные имена полей (Spark иногда даёт другой регистр для r_minutes)
        String rCol = resolveColumnName(segments.schema(), "r_minutes");
        Dataset<Row> segmentsForKafka = segments.select(
                functions.col("user_id").cast(DataTypes.StringType).alias("key"),
                functions.to_json(functions.struct(
                        functions.col("user_id").alias("user_id"),
                        functions.col("segment").alias("segment"),
                        functions.col(rCol).cast(DataTypes.DoubleType).alias("r_minutes"),
                        functions.col("f").alias("f"),
                        functions.col("m").cast(DataTypes.DoubleType).alias("m"),
                        functions.col("updated_at").alias("updated_at")
                )).alias("value")
        );

        StreamingQuery querySegments = segmentsForKafka.writeStream()
                .foreachBatch((Dataset<Row> batch, Long batchId) -> {
                    if (!batch.isEmpty()) {
                        List<Row> rows = batch.collectAsList();
                        for (Row row : rows) {
                            try {
                                String key = row.getString(0);
                                String value = row.getString(1);
                                log.info("RFM Segment - Key: {}, Value: {}", key, value);
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

    private static String resolveColumnName(StructType schema, String target) {
        for (StructField field : schema.fields()) {
            if (field.name().equalsIgnoreCase(target)) {
                return field.name();
            }
        }
        return target;
    }

    static double clampM(double m) {
        return Math.max(0, m);
    }

    static void purgeWindow(List<TransactionEntry> entries, long curr, long windowMs) {
        entries.removeIf(entry -> curr - entry.timestamp > windowMs);
    }

    /** Баланс в окне RFM: Deposit +, Credit −; M не ниже 0. */
    static double segmentBalanceFromEntries(List<TransactionEntry> entries) {
        double balance = 0;
        for (TransactionEntry e : entries) {
            String t = e.getType();
            if (t == null) {
                continue;
            }
            if ("Deposit".equalsIgnoreCase(t)) {
                balance += e.getSum();
            } else if ("Credit".equalsIgnoreCase(t)) {
                balance -= e.getSum();
            }
        }
        return clampM(balance);
    }

    static boolean isNegativeMCredit(double currentM, String type, double sum) {
        return "Credit".equalsIgnoreCase(type) && sum > currentM;
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

            List<TransactionEntry> avgEntries = new ArrayList<>();
            List<TransactionEntry> mEntries = new ArrayList<>();
            loadAnomalyState(stateStr, now, avgEntries, mEntries);

            int transactionCount = 0;
            while (rows.hasNext()) {
                Row r = rows.next();
                transactionCount++;
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);
                long curr = eventTime > 0 ? eventTime : now;

                purgeWindow(mEntries, curr, WINDOW_RFM_MS);
                double currentM = segmentBalanceFromEntries(mEntries);

                if (isNegativeMCredit(currentM, type, sum)) {
                    AlertEvent alert = new AlertEvent();
                    alert.setUser_id(userId);
                    alert.setEvent_time(eventTime);
                    alert.setType(type);
                    alert.setSum(sum);
                    alert.setAvg_check_5min(currentM);
                    alert.setMessage(AnomalyType.NEGATIVE_M.name());
                    out.add(alert);
                    continue;
                }

                double avg = calculateAverage(avgEntries);

                if ("Credit".equalsIgnoreCase(type) && avg > 0 && sum >= 3 * avg) {
                    AlertEvent alert = new AlertEvent();
                    alert.setUser_id(userId);
                    alert.setEvent_time(eventTime);
                    alert.setType(type);
                    alert.setSum(sum);
                    alert.setAvg_check_5min(avg);
                    alert.setMessage(AnomalyType.BIGGER_THEN_AVG_CHECK.name());
                    out.add(alert);
                }

                long entryTs = eventTime > 0 ? eventTime : now;
                mEntries.add(new TransactionEntry(entryTs, sum, type));
                avgEntries.add(new TransactionEntry(entryTs, sum, type));
            }

            if (transactionCount > 0 && log.isDebugEnabled()) {
                log.debug("Processed {} transactions for user {}, avg window={}, m window={}",
                        transactionCount, userId, avgEntries.size(), mEntries.size());
            }

            state.update(serializeAnomalyState(avgEntries, mEntries));

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

        private void loadAnomalyState(String stateStr, long now,
                                      List<TransactionEntry> avgEntries, List<TransactionEntry> mEntries) {
            if (stateStr == null || stateStr.isEmpty()) {
                return;
            }
            int sep = stateStr.indexOf(ANOMALY_STATE_SEP);
            if (sep >= 0) {
                avgEntries.addAll(parseStateEntries(stateStr.substring(0, sep), now, WINDOW_ANOMALY_MS));
                mEntries.addAll(parseEntriesCsv(stateStr.substring(sep + ANOMALY_STATE_SEP.length())));
            } else {
                avgEntries.addAll(parseStateEntries(stateStr, now, WINDOW_ANOMALY_MS));
            }
        }

        private List<TransactionEntry> parseEntriesCsv(String csv) {
            List<TransactionEntry> entries = new ArrayList<>();
            if (csv == null || csv.isEmpty()) {
                return entries;
            }
            for (String e : csv.split(",")) {
                if (e.isEmpty()) {
                    continue;
                }
                String[] parts = e.split(":");
                if (parts.length >= 3) {
                    try {
                        long ts = Long.parseLong(parts[0]);
                        double sum = Double.parseDouble(parts[1]);
                        String type = parts[2];
                        entries.add(new TransactionEntry(ts, sum, type));
                    } catch (NumberFormatException ex) {
                        log.error("Failed to parse M state entry: {}", e, ex);
                    }
                }
            }
            return entries;
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

        private String serializeAnomalyState(List<TransactionEntry> avgEntries, List<TransactionEntry> mEntries) {
            return serializeEntries(avgEntries) + ANOMALY_STATE_SEP + serializeEntries(mEntries);
        }
    }

    static class RfmUpdateResult {
        final RFMState state;
        final boolean applied;

        RfmUpdateResult(RFMState state, boolean applied) {
            this.state = state;
            this.applied = applied;
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

            RFMState rfmState = resolveInitialRfmState(userId, savedStateJson, state);

            while (rows.hasNext()) {
                Row r = rows.next();
                long eventTime = r.getLong(3);
                String type = r.getString(1);
                double sum = r.getDouble(2);

                RfmUpdateResult update = updateRFMState(rfmState, eventTime, sum, type);
                if (!update.applied) {
                    log.debug("Rejected Credit for user {}: sum={}, current M={}", userId, sum, rfmState.m);
                    continue;
                }
                rfmState = update.state;
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

        /**
         * Состояние из Redis и из Spark checkpoint; на новом executor lastTs не теряется.
         */
        private RFMState resolveInitialRfmState(Integer userId, String savedStateJson, GroupState<String> state)
                throws Exception {
            RFMState fromRedis = null;
            if (savedStateJson != null && !savedStateJson.isEmpty()) {
                fromRedis = mapper.readValue(savedStateJson, RFMState.class);
                ensureEntriesNotNull(fromRedis);
                reconcileLastTsFromEntries(fromRedis);
                log.debug("Loaded user {} from Redis: lastTs={}, f={}, m={}",
                        userId, fromRedis.getLastTs(), fromRedis.getF(), fromRedis.getM());
            }

            RFMState fromSpark = null;
            if (state.exists()) {
                fromSpark = parseRFMState(state.get());
                ensureEntriesNotNull(fromSpark);
                reconcileLastTsFromEntries(fromSpark);
                log.debug("Loaded user {} from Spark state: lastTs={}", userId, fromSpark.getLastTs());
            }

            RFMState rfmState;
            if (fromRedis != null && fromSpark != null) {
                rfmState = fromRedis;
                rfmState.setLastTs(Math.max(fromRedis.getLastTs(), fromSpark.getLastTs()));
                rfmState.setLastWallMs(Math.max(fromRedis.getLastWallMs(), fromSpark.getLastWallMs()));
                rfmState.setFirstTs(mergeFirstTs(fromRedis.getFirstTs(), fromSpark.getFirstTs()));
                if (fromSpark.getEntries().size() > fromRedis.getEntries().size()) {
                    rfmState.setEntries(new ArrayList<>(fromSpark.getEntries()));
                }
                recomputeAggregates(rfmState);
            } else if (fromRedis != null) {
                rfmState = fromRedis;
            } else if (fromSpark != null) {
                rfmState = fromSpark;
            } else {
                rfmState = new RFMState();
                log.debug("New user: {}", userId);
            }

            reconcileLastTsFromEntries(rfmState);
            return rfmState;
        }

        private static void ensureEntriesNotNull(RFMState s) {
            if (s.getEntries() == null) {
                s.setEntries(new ArrayList<>());
            }
        }

        /** Восстановить lastTs из истории, если после JSON он не прочитался (0). */
        private static void reconcileLastTsFromEntries(RFMState s) {
            if (s.getLastTs() != 0 || s.getEntries() == null || s.getEntries().isEmpty()) {
                return;
            }
            long maxTs = 0;
            for (TransactionEntry e : s.getEntries()) {
                if (e.getTimestamp() > maxTs) {
                    maxTs = e.getTimestamp();
                }
            }
            if (maxTs > 0) {
                s.setLastTs(maxTs);
            }
        }

        private static long mergeFirstTs(long a, long b) {
            if (a == 0) {
                return b;
            }
            if (b == 0) {
                return a;
            }
            return Math.min(a, b);
        }

        private static void recomputeAggregates(RFMState s) {
            s.setF(s.getEntries().size());
            s.setM(segmentBalanceFromEntries(s.getEntries()));
        }

        private RFMState parseRFMState(String stateStr) {
            RFMState state = new RFMState();
            state.setLastTs(0);
            state.setFirstTs(0);
            state.setLastWallMs(0);
            state.setEntries(new ArrayList<>());

            if (stateStr == null || stateStr.isEmpty()) {
                return state;
            }

            String[] parts = stateStr.split("\\|", 4);
            if (parts.length < 3) {
                return state;
            }
            try {
                state.setLastTs(Long.parseLong(parts[0]));
                state.setFirstTs(Long.parseLong(parts[1]));
                if (parts.length == 4) {
                    state.setLastWallMs(Long.parseLong(parts[2]));
                    parseRFMEntries(parts[3], state);
                } else {
                    parseRFMEntries(parts[2], state);
                }
            } catch (NumberFormatException ex) {
                log.error("Failed to parse RFM state: {}", stateStr, ex);
            }
            return state;
        }

        private void parseRFMEntries(String csv, RFMState state) {
            if (csv == null || csv.isEmpty()) {
                return;
            }
            for (String e : csv.split(",")) {
                if (e.isEmpty()) {
                    continue;
                }
                String[] p = e.split(":");
                if (p.length >= 3) {
                    try {
                        long ts = Long.parseLong(p[0]);
                        double sum = Double.parseDouble(p[1]);
                        String type = p[2];
                        state.getEntries().add(new TransactionEntry(ts, sum, type));
                    } catch (NumberFormatException ex) {
                        log.error("Failed to parse RFM entry: {}", e, ex);
                    }
                }
            }
        }

        private RfmUpdateResult updateRFMState(RFMState state, long eventTime, double sum, String type) {
            long wallNow = System.currentTimeMillis();
            long curr = eventTime > 0 ? eventTime : wallNow;

            purgeWindow(state.entries, curr, WINDOW_RFM_MS);
            double currentM = segmentBalanceFromEntries(state.entries);
            state.m = currentM;
            state.f = state.entries.size();

            if (isNegativeMCredit(currentM, type, sum)) {
                return new RfmUpdateResult(state, false);
            }

            if (state.firstTs == 0) {
                state.firstTs = curr;
            }

            long prevLast = state.lastTs;
            long prevWall = state.lastWallMs;

            if (prevLast == 0 && prevWall == 0) {
                state.rMinutes = 0;
            } else {
                long deltaEvent = 0;
                if (prevLast > 0 && curr >= prevLast) {
                    deltaEvent = curr - prevLast;
                }
                if (deltaEvent > 0) {
                    state.rMinutes = deltaEvent / 60000.0;
                } else {
                    long wallDelta = prevWall > 0 ? (wallNow - prevWall) : 0;
                    state.rMinutes = wallDelta > 0 ? wallDelta / 60000.0 : 0;
                }
            }

            state.lastTs = curr;
            state.lastWallMs = wallNow;

            state.entries.add(new TransactionEntry(curr, sum, type));
            purgeWindow(state.entries, curr, WINDOW_RFM_MS);

            state.f = state.entries.size();
            state.m = segmentBalanceFromEntries(state.entries);

            return new RfmUpdateResult(state, true);
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
            sb.append(state.getLastTs()).append("|")
                    .append(state.getFirstTs()).append("|")
                    .append(state.getLastWallMs()).append("|");

            for (int i = 0; i < state.getEntries().size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
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
        @JsonProperty("timestamp")
        private long timestamp;
        @JsonProperty("sum")
        private double sum;
        @JsonProperty("type")
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
        @JsonProperty("lastTs")
        private long lastTs;
        @JsonProperty("firstTs")
        private long firstTs;
        /** Время последней обработки на JVM; если event_time не меняется — R считаем по нему. */
        @JsonProperty("lastWallMs")
        private long lastWallMs;
        @JsonProperty("entries")
        private List<TransactionEntry> entries = new ArrayList<>();
        @JsonProperty("f")
        private long f;
        @JsonProperty("m")
        private double m;
        @JsonProperty("rMinutes")
        private double rMinutes;

        public RFMState() {}

        public long getLastTs() { return lastTs; }
        public void setLastTs(long lastTs) { this.lastTs = lastTs; }

        public long getFirstTs() { return firstTs; }
        public void setFirstTs(long firstTs) { this.firstTs = firstTs; }

        public long getLastWallMs() { return lastWallMs; }
        public void setLastWallMs(long lastWallMs) { this.lastWallMs = lastWallMs; }

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