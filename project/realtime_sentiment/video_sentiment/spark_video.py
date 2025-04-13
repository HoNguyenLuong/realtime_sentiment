from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, TimestampType, IntegerType, StructField, ArrayType

from sentiment_analysis import analyze_emotions




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

# Schema cho UDF
emotion_schema = StructType([
    StructField("num_faces", IntegerType(), False),
    StructField("emotions", ArrayType(StringType()), False)
])
#Đăng ký UDF
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
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Chờ stream kết thúc
query.awaitTermination()

# Dừng SparkSession
spark.stop()