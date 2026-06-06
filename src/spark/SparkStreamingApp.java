package spark;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.In;
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


public class SparkStreamingApp {

    private static final Logger log = LoggerFactory.getLogger(SparkStreamingApp.class);
    private static final long WINDOW_ANOMALY_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("WINDOW_ANOMALY_MS")));
    private static final long WINDOW_RFM_MS = TimeUnit.MINUTES.toMillis(Long.parseLong(System.getenv("WINDOW_RFM_MS")));

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

}