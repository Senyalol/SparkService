# Этап сборки
FROM maven:3.9.9-eclipse-temurin-11 AS builder

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Этап выполнения
FROM apache/spark:3.5.3

USER root

WORKDIR /app

# Копируем fat-JAR (именно тот, который 73 МБ)
COPY --from=builder /app/target/SparkTransaction-1.0-SNAPSHOT.jar /app/app.jar

# Проверяем, что файл скопировался
RUN ls -la /app/app.jar || (echo "JAR file not found!" && exit 1)

# Копируем скрипт запуска
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENV KAFKA_TOPIC=user-transactions
ENV KAFKA_ALERTS_TOPIC=alerts
ENV KAFKA_SEGMENTS_TOPIC=user-segments
ENV SPARK_MASTER=local[*]

ENTRYPOINT ["/app/run.sh"]

## Этап сборки
#FROM maven:3.9.9-eclipse-temurin-11 AS build
#
#WORKDIR /app
#
#COPY pom.xml .
#COPY src ./src
#
#RUN mvn clean package -DskipTests
#
## Этап выполнения - используем Spark образ с тегом 3.5
#FROM apache/spark:3.5.3
#
#USER root
#
#WORKDIR /app
#
#COPY --from=build /app/target/*-all.jar app.jar
#COPY run.sh /app/run.sh
#RUN chmod +x /app/run.sh
#
#ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
#ENV KAFKA_TOPIC=user-transactions
#ENV KAFKA_ALERTS_TOPIC=alerts
#ENV KAFKA_SEGMENTS_TOPIC=user-segments
#ENV SPARK_MASTER=local[*]
#
#ENTRYPOINT ["/app/run.sh"]