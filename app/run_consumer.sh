#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

echo "Starting PySpark Consumer Application..."
echo "KAFKA_BROKERS environment variable: $KAFKA_BROKERS" # Debug: cek env var

# Tentukan path ke spark-submit. 
# Untuk bitnami/spark, biasanya /opt/bitnami/spark/bin/spark-submit
# Untuk apache/spark, biasanya /opt/spark/bin/spark-submit
# SPARK_HOME mungkin sudah diset di image.

SPARK_SUBMIT_CMD=""
if [ -n "$SPARK_HOME" ]; then
  SPARK_SUBMIT_CMD="$SPARK_HOME/bin/spark-submit"
elif [ -f "/opt/bitnami/spark/bin/spark-submit" ]; then
  SPARK_SUBMIT_CMD="/opt/bitnami/spark/bin/spark-submit"
elif [ -f "/opt/spark/bin/spark-submit" ]; then
  SPARK_SUBMIT_CMD="/opt/spark/bin/spark-submit"
else
  echo "SPARK_HOME not set and common spark-submit paths not found. Please check your Spark image."
  exit 1
fi

echo "Using spark-submit: $SPARK_SUBMIT_CMD"

# Ganti versi paket Kafka (org.apache.spark:spark-sql-kafka-0-10_2.12:X.Y.Z)
# agar sesuai dengan versi Spark yang Anda gunakan di Dockerfile.spark_consumer.
# Contoh: Jika Dockerfile.spark_consumer menggunakan bitnami/spark:3.3, maka paketnya adalah versi 3.3.0
# Contoh: Jika Dockerfile.spark_consumer menggunakan bitnami/spark:3.5, maka paketnya adalah versi 3.5.0
# Pastikan juga versi Scala (_2.12) cocok dengan image Spark.

KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0" # Default, sesuaikan jika perlu

# Anda bisa menyesuaikan KAFKA_PACKAGE berdasarkan versi spark dari image jika diperlukan
# Misalnya, jika Anda menggunakan image bitnami/spark:3.5.0
# KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" 

$SPARK_SUBMIT_CMD \
  --master local[*] \
  --packages $KAFKA_PACKAGE \
  /app/consumer_pyspark.py

echo "PySpark Consumer Application has finished or was stopped."