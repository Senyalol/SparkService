#!/bin/bash
/opt/spark/bin/spark-submit \
  --master ${SPARK_MASTER} \
  --class Main \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /app/app.jar

# #!/bin/bash
# /opt/spark/bin/spark-submit \
#   --master ${SPARK_MASTER} \
#   --class Main \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
#   /app/app.jar