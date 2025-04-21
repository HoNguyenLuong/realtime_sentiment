from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType, StructField, ArrayType

from realtime_sentiment.src.video_sentiment.sentiment_analysis import analyze_emotions

def run ():
    try:
        # Khởi tạo Spark session
        spark = SparkSession.builder \
            .appName("VideoFrameProcessor") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Schema dữ liệu Kafka gửi tới
        schema = StructType() \
            .add("image", StringType()) \
            .add("metadata", StructType()
                 .add("source", StringType())
                 .add("timestamp", StringType()))

        # Đọc từ Kafka topic
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "video_frames") \
            .option("startingOffsets", "latest") \
            .load()

        # Giải mã và parse JSON
        json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # Schema cho UDF emotion
        emotion_schema = StructType([
            StructField("num_faces", IntegerType(), False),
            StructField("emotions", ArrayType(StringType()), False)
        ])

        # Đăng ký UDF
        emotion_udf = udf(analyze_emotions, emotion_schema)

        # Áp dụng UDF
        result_df = json_df.withColumn("result", emotion_udf(col("image")))

        # Tách struct thành các cột riêng
        result_df = result_df.select(
            col("image"),
            col("metadata.source").alias("source"),
            col("metadata.timestamp").alias("timestamp"),
            col("result.num_faces").alias("num_faces"),
            col("result.emotions").alias("emotions")
        )

        # Ghi kết quả ra console (hoặc thay bằng Kafka/database)
        # query = result_df.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .option("truncate", False) \
        #     .start()

        # Ghi kết quả ra Kafka
        query = result_df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "emotion_results") \
            .outputMode("append") \
            .start()

        # # Ghi kết quả ra database
        # def write_to_db(batch_df, batch_id):
        #     batch_df.write \
        #         .format("jdbc") \
        #         .option("url", "jdbc:postgresql://localhost:5432/dbname") \
        #         .option("dbtable", "emotion_results") \
        #         .option("user", "user") \
        #         .option("password", "password") \
        #         .mode("append") \
        #         .save()
        #
        # query = result_df.writeStream \
        #     .outputMode("append") \
        #     .foreachBatch(write_to_db) \
        #     .start()

        # Chờ stream kết thúc
        query.awaitTermination()

        # Dừng SparkSession
        spark.stop()
    except Exception as e:
        print(f"[video_consumer.run] Error: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()


