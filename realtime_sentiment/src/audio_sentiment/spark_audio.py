from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType
import base64

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("AudioStreamProcessor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka schema
schema = StructType() \
    .add("audio", StringType()) \
    .add("metadata", StructType()
         .add("source", StringType())
         .add("timestamp", StringType()))

# Đọc từ Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "audio_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# UDF kiểm tra độ dài file âm thanh
def audio_length(b64_audio: str) -> str:
    try:
        audio_bytes = base64.b64decode(b64_audio)
        return str(len(audio_bytes))
    except Exception as e:
        return f"Error: {e}"

# Đăng ký UDF
get_audio_length = udf(audio_length, StringType())

# Áp dụng UDF
processed_df = json_df.withColumn("audio_len", get_audio_length(col("audio"))) \
                      .withColumn("source", col("metadata.source")) \
                      .withColumn("timestamp", col("metadata.timestamp"))

# # Hiển thị ra console
# query = processed_df.select("source", "timestamp", "audio_len") \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()
#
# query.awaitTermination()