from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when, lit, greatest, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, FloatType

KAFKA_BROKER = "localhost:9092"
TOPIC_SUHU = "sensor-suhu-gudang"
TOPIC_KELEMBABAN = "sensor-kelembaban-gudang"

# Skema untuk data suhu dari Kafka
schema_suhu = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("suhu", FloatType(), True),
    StructField("timestamp", DoubleType(), True)  # Unix epoch timestamp
])

# Skema untuk data kelembaban dari Kafka
schema_kelembaban = StructType([
    StructField("gudang_id", StringType(), True),
    StructField("kelembaban", FloatType(), True),
    StructField("timestamp", DoubleType(), True)  # Unix epoch timestamp
])

def process_combined_data(df, epoch_id):
    """
    Memproses DataFrame gabungan dari suhu dan kelembaban untuk setiap micro-batch.
    Menerapkan logika peringatan dan mencetak output.
    """
    if df.count() > 0:
        print(f"\n--- Batch Gabungan {epoch_id} ---")
        
        # Terapkan logika peringatan berdasarkan data yang ada (suhu dan kelembaban bisa null dari join)
        processed_df = df.withColumn(
            "status_suhu_val",  # Kolom sementara untuk logika
            when(col("suhu").isNotNull() & (col("suhu") > 80), "Suhu tinggi")
            .otherwise(when(col("suhu").isNotNull(), "Suhu normal").otherwise(None)) # Handle suhu null
        ).withColumn(
            "status_kelembaban_val", # Kolom sementara untuk logika
            when(col("kelembaban").isNotNull() & (col("kelembaban") > 70), "Kelembaban tinggi")
            .otherwise(when(col("kelembaban").isNotNull(), "Kelembaban normal").otherwise(None)) # Handle kelembaban null
        ).withColumn(
            "peringatan_kritis_flag", # Flag boolean untuk kondisi kritis
             when(col("suhu").isNotNull() & col("kelembaban").isNotNull() & (col("suhu") > 80) & (col("kelembaban") > 70), True)
            .otherwise(False)
        ).withColumn(
            "status_akhir",
            when(col("peringatan_kritis_flag"), lit("Bahaya tinggi! Barang berisiko rusak"))
            .when(col("status_suhu_val") == "Suhu tinggi", lit("Suhu tinggi, kelembaban normal"))
            .when(col("status_kelembaban_val") == "Kelembaban tinggi", lit("Kelembaban tinggi, suhu aman"))
            .when(col("suhu").isNotNull() & col("kelembaban").isNotNull(), lit("Aman")) # Aman jika keduanya ada dan tidak kritis/tinggi
            .otherwise(lit("Data tidak lengkap untuk status akhir")) # Jika salah satu suhu atau kelembaban null
        )

        # Pilih kolom untuk output akhir
        output_df = processed_df.select(
            col("gudang_id"),
            col("suhu"),
            col("kelembaban"),
            col("status_akhir").alias("status") # Kolom "status" inilah yang akan diakses
        )
        
        print("Hasil Pemrosesan Gudang:")
        for row in output_df.collect():
            # Mengakses kolom 'status' yang sudah di-alias
            if row["status"] == "Bahaya tinggi! Barang berisiko rusak":
                print("[PERINGATAN KRITIS]")
                print(f"Gudang {row['gudang_id']}:")
                print(f"  - Suhu: {row['suhu']:.1f}°C" if row['suhu'] is not None else "  - Suhu: N/A")
                print(f"  - Kelembaban: {row['kelembaban']:.1f}%" if row['kelembaban'] is not None else "  - Kelembaban: N/A")
                print(f"  - Status: {row['status']}")
            # Menggunakan row["status"] untuk semua kondisi
            elif row["status"] == "Suhu tinggi, kelembaban normal":
                 print(f"[Peringatan Suhu Tinggi] Gudang {row['gudang_id']}: Suhu {row['suhu']:.1f}°C" if row['suhu'] is not None else f"[Peringatan Suhu Tinggi] Gudang {row['gudang_id']}: Suhu N/A", f", Status: {row['status']}")
            elif row["status"] == "Kelembaban tinggi, suhu aman":
                 print(f"[Peringatan Kelembaban Tinggi] Gudang {row['gudang_id']}: Kelembaban {row['kelembaban']:.1f}%" if row['kelembaban'] is not None else f"[Peringatan Kelembaban Tinggi] Gudang {row['gudang_id']}: Kelembaban N/A", f", Status: {row['status']}")
            elif row["status"] == "Aman":
                 suhu_str = f"{row['suhu']:.1f}°C" if row['suhu'] is not None else "N/A"
                 kelembaban_str = f"{row['kelembaban']:.1f}%" if row['kelembaban'] is not None else "N/A"
                 print(f"Gudang {row['gudang_id']}: Suhu {suhu_str}, Kelembaban {kelembaban_str}, Status: {row['status']}")
            else: # Untuk status "Data tidak lengkap untuk status akhir"
                 print(f"Gudang {row['gudang_id']}: Status: {row['status']}")
            print("-" * 20)
    else:
        print(f"Batch Gabungan {epoch_id} tidak ada data setelah join atau dari stream.")

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GudangMonitoringConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
        # Pastikan versi Spark Kafka connector (3.5.0) sesuai dengan versi Spark Anda.
        # Jika Spark Anda menggunakan Scala 2.13, ganti _2.12 menjadi _2.13.

    spark.sparkContext.setLogLevel("WARN")

    # 1. Baca Stream Suhu
    df_suhu_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_SUHU) \
        .option("startingOffsets", "latest") \
        .load()

    df_suhu = df_suhu_raw.selectExpr("CAST(value AS STRING) as json_value_suhu") \
        .select(from_json(col("json_value_suhu"), schema_suhu).alias("data_suhu")) \
        .select("data_suhu.gudang_id", "data_suhu.suhu", col("data_suhu.timestamp").cast(TimestampType()).alias("timestamp_suhu")) \
        .withWatermark("timestamp_suhu", "20 seconds") # Watermark sedikit lebih besar dari interval window

    # 2. Baca Stream Kelembaban
    df_kelembaban_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_KELEMBABAN) \
        .option("startingOffsets", "latest") \
        .load()

    df_kelembaban = df_kelembaban_raw.selectExpr("CAST(value AS STRING) as json_value_kelembaban") \
        .select(from_json(col("json_value_kelembaban"), schema_kelembaban).alias("data_kelembaban")) \
        .select("data_kelembaban.gudang_id", "data_kelembaban.kelembaban", col("data_kelembaban.timestamp").cast(TimestampType()).alias("timestamp_kelembaban")) \
        .withWatermark("timestamp_kelembaban", "20 seconds")

    # 3. Filtering Individual untuk Peringatan (dicetak langsung dari micro-batch)
    def print_peringatan(df, epoch_id, jenis_peringatan, metrik_nama, unit):
        if df.count() > 0:
            print(f"\n--- Batch Peringatan {jenis_peringatan} {epoch_id} ---")
            for row in df.collect():
                print(f"[{jenis_peringatan}] Gudang {row.gudang_id}: {metrik_nama} {getattr(row, metrik_nama)}{unit}")

    query_peringatan_suhu = df_suhu \
        .filter(col("suhu") > 80) \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: print_peringatan(df, epoch_id, "Suhu Tinggi", "suhu", "°C")) \
        .option("checkpointLocation", "D:/Kafka/spark_checkpoints/peringatan_suhu") \
        .start()

    query_peringatan_kelembaban = df_kelembaban \
        .filter(col("kelembaban") > 70) \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: print_peringatan(df, epoch_id, "Kelembaban Tinggi", "kelembaban", "%")) \
        .option("checkpointLocation", "D:/Kafka/spark_checkpoints/peringatan_kelembaban") \
        .start()
        
    # 4. Gabungkan Stream dengan Stream-Stream Join
    # Rename kolom gudang_id agar tidak ambigu setelah join
    df_suhu_renamed = df_suhu.withColumnRenamed("gudang_id", "g_id_s")
    df_kelembaban_renamed = df_kelembaban.withColumnRenamed("gudang_id", "g_id_k")
    
    # Kondisi join: gudang_id sama DAN timestamp berada dalam window 10 detik
    # (misalnya, suhu terjadi dalam rentang +/- 5 detik dari kelembaban)
    join_condition = expr("""
        g_id_s = g_id_k AND 
        timestamp_suhu >= timestamp_kelembaban - interval 10 seconds AND 
        timestamp_suhu <= timestamp_kelembaban + interval 10 seconds 
    """)
    
    # Menggunakan fullOuter join untuk memastikan kita mendapatkan semua data dari kedua sisi,
    # meskipun salah satunya mungkin tidak memiliki pasangan dalam window waktu.
    joined_df = df_suhu_renamed.join(
        df_kelembaban_renamed,
        join_condition,
        "fullOuter" 
    ).select(
        coalesce(col("g_id_s"), col("g_id_k")).alias("gudang_id"), # Mengambil gudang_id dari sisi yang tidak null
        col("suhu"),
        col("kelembaban"),
        # Mengambil timestamp yang lebih baru sebagai referensi, atau salah satu jika yang lain null
        when(col("timestamp_suhu").isNotNull() & col("timestamp_kelembaban").isNotNull(), greatest(col("timestamp_suhu"), col("timestamp_kelembaban")))
        .when(col("timestamp_suhu").isNotNull(), col("timestamp_suhu"))
        .otherwise(col("timestamp_kelembaban")).alias("event_time")
    )
    
    # Output gabungan ke console menggunakan fungsi process_combined_data
    query_gabungan = joined_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(process_combined_data) \
        .option("checkpointLocation", "D:/Kafka/spark_checkpoints/gabungan") \
        .start()

    print("Streaming queries dimulai. Menunggu data...")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nConsumer dihentikan oleh pengguna.")
    finally:
        print("Menghentikan streaming queries...")
        if 'query_peringatan_suhu' in locals() and query_peringatan_suhu: query_peringatan_suhu.stop()
        if 'query_peringatan_kelembaban' in locals() and query_peringatan_kelembaban: query_peringatan_kelembaban.stop()
        if 'query_gabungan' in locals() and query_gabungan: query_gabungan.stop()
        spark.stop()
        print("Spark session dihentikan.")