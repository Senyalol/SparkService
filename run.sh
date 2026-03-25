#!/bin/bash
echo "🧹 Cleaning up old Spark temporary files..."
rm -rf /tmp/blockmgr-* 2>/dev/null
rm -rf /tmp/spark-* 2>/dev/null
rm -rf /tmp/temporary-* 2>/dev/null
echo "Cleanup completed"

echo "Starting Spark Streaming Application..."
/opt/spark/bin/spark-submit \
  --master ${SPARK_MASTER} \
  --class Main \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /app/app.jar