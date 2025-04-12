from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, TimestampType
import base64
import numpy as np
import cv2

from face_detection import detect_faces
from ..utils.image_utils import process_image

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

# # Decode base64 -> byte[] -> frame shape
# def extract_shape(b64_img: str) -> str:
#     try:
#         img_data = base64.b64decode(b64_img)
#         np_arr = np.frombuffer(img_data, np.uint8)
#         img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
#         if img is not None:
#             return f"{img.shape}"
#         return "Invalid frame"
#     except Exception as e:
#         return f"Error: {e}"
#
# # Đăng ký UDF
# extract_shape_udf = udf(extract_shape, StringType())
#
# # Áp dụng UDF để hiển thị frame shape
# processed_df = json_df.withColumn("frame_shape", extract_shape_udf(col("image"))) \
#                       .withColumn("source", col("metadata.source")) \
#                       .withColumn("timestamp", col("metadata.timestamp"))

# # In kết quả ra console (demo)
# query = processed_df.select("source", "timestamp", "frame_shape") \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()
#
# query.awaitTermination()