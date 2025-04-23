# consumer/spark_video.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType

from src.video_sentiment.sentiment_analysis import analyze_emotions
from .common import mark_as_processed, get_kafka_producer, logger, KAFKA_BROKER, VIDEO_TOPIC
from datetime import datetime
import io
from PIL import Image
import numpy as np

# Import thư viện MinIO và cấu hình từ vị trí khác trong dự án của bạn
# TODO: Thay đổi đường dẫn import cho phù hợp với dự án của bạn
from src.producer.config import minio_client, CONFIG


def get_frame_from_minio(bucket_name, object_name):
    """
    Lấy frame JPG từ MinIO và chuyển đổi thành numpy array

    Args:
        bucket_name (str): Tên bucket
        object_name (str): Tên object

    Returns:
        numpy.ndarray: Frame dạng numpy array hoặc None nếu có lỗi
    """
    try:
        # Lấy đối tượng từ MinIO
        response = minio_client.get_object(bucket_name, object_name)

        # Đọc dữ liệu và chuyển thành ảnh
        image_data = response.read()
        image = Image.open(io.BytesIO(image_data))

        # Chuyển thành numpy array
        frame = np.array(image)

        logger.info(f"Đã lấy frame JPG từ MinIO: {object_name}")
        return frame

    except Exception as e:
        logger.error(f"Lỗi khi lấy frame từ MinIO: {str(e)}")
        return None
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()


def process_frame(metadata_row):
    """
    Xử lý frame từ MinIO dựa trên metadata

    Args:
        metadata_row (dict): Metadata của frame

    Returns:
        dict: Kết quả xử lý bao gồm số lượng khuôn mặt và cảm xúc
    """
    try:
        # Lấy thông tin từ metadata
        bucket_name = metadata_row["bucket_name"]
        object_name = metadata_row["object_name"]

        # Lấy frame từ MinIO
        frame = get_frame_from_minio(bucket_name, object_name)

        if frame is not None:
            # Phân tích cảm xúc từ frame (truyền trực tiếp numpy array)
            num_faces, emotions = analyze_emotions(frame)

            # Đánh dấu đã xử lý
            producer = get_kafka_producer()
            mark_as_processed(producer, metadata_row)

            return {"num_faces": num_faces, "emotions": emotions}
        else:
            return {"num_faces": 0, "emotions": []}
    except Exception as e:
        logger.error(f"Lỗi khi xử lý frame: {str(e)}")
        return {"num_faces": 0, "emotions": []}

def run():
    try:
        # Khởi tạo Spark session
        # TODO: Điều chỉnh cấu hình Spark phù hợp với môi trường của bạn
        spark = SparkSession.builder \
            .appName("VideoFrameProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Schema dữ liệu Kafka gửi tới - Đây là metadata từ MinIO
        # TODO: Điều chỉnh schema phù hợp với cấu trúc metadata thực tế của bạn
        schema = StructType() \
            .add("video_id", StringType()) \
            .add("frame_id", StringType()) \
            .add("type", StringType()) \
            .add("bucket_name", StringType()) \
            .add("object_name", StringType()) \
            .add("timestamp", StringType())

        # Đọc từ Kafka topic video_frames
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", VIDEO_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Giải mã và parse JSON
        json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # Sử dụng foreachBatch để xử lý từng batch
        def process_batch(batch_df, batch_id):
            if not batch_df.isEmpty():
                # Convert DataFrame to pandas để xử lý từng hàng
                pandas_df = batch_df.toPandas()

                # Xử lý từng hàng
                results = []
                for _, row in pandas_df.iterrows():
                    metadata = row.asDict()
                    result = process_frame(metadata)
                    results.append({
                        "frame_id": metadata.get("frame_id"),
                        "video_id": metadata.get("video_id"),
                        "timestamp": metadata.get("timestamp"),
                        "bucket_name": metadata.get("bucket_name"),
                        "object_name": metadata.get("object_name"),
                        "num_faces": result.get("num_faces"),
                        "emotions": result.get("emotions"),
                        "processed_at": datetime.now().isoformat()
                    })

                # Tạo DataFrame kết quả và ghi ra Kafka
                if results:
                    result_schema = StructType([
                        StructField("frame_id", StringType(), True),
                        StructField("video_id", StringType(), True),
                        StructField("timestamp", StringType(), True),
                        StructField("bucket_name", StringType(), True),
                        StructField("object_name", StringType(), True),
                        StructField("num_faces", IntegerType(), True),
                        StructField("emotions", ArrayType(StringType()), True),
                        StructField("processed_at", StringType(), True)
                    ])

                    result_df = spark.createDataFrame(results, schema=result_schema)

                    # Ghi kết quả ra Kafka
                    # TODO: Điều chỉnh tên topic kết quả phù hợp với hệ thống của bạn
                    result_df.selectExpr("to_json(struct(*)) AS value") \
                        .write \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
                        .option("topic", "emotion_results") \
                        .save()

        # Sử dụng foreachBatch để xử lý từng batch
        query = json_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        # Chờ stream kết thúc
        query.awaitTermination()

    except Exception as e:
        logger.error(f"[video_consumer.run] Error: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
