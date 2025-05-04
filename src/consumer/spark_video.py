from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, ArrayType
from src.utils.image_utils import process_frame
from .common import logger, VIDEO_TOPIC
from src.producer.config import CONFIG, minio_client
from datetime import datetime
import numpy as np
import json
from src.producer.kafka_sender import producer


def run():
    try:
        # Định nghĩa tên topic cho kết quả xử lý emotion
        emotion_results_topic = "emotion_results"

        # Cấu hình bucket MinIO để lưu kết quả emotion
        emotion_results_bucket = "emotion-results"

        # Đảm bảo bucket tồn tại
        if not minio_client.bucket_exists(emotion_results_bucket):
            minio_client.make_bucket(emotion_results_bucket)
            logger.info(f"Created new bucket: {emotion_results_bucket}")

        # Khởi tạo Spark session kết nối với Spark master bên ngoài
        spark = SparkSession.builder \
            .appName("VideoFrameProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.driver.extraJavaOptions", "-Dkafka.bootstrap.servers=kafka:9092") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
            .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Schema dữ liệu Kafka gửi tới - Đây là metadata từ MinIO
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
            .option("kafka.bootstrap.servers", CONFIG['kafka']['bootstrap_servers']) \
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
                    if hasattr(row, 'asDict'):
                        metadata = row.asDict()
                    else:
                        metadata = row.to_dict()
                    result = process_frame(metadata)

                    # Tạo đối tượng kết quả
                    result_obj = {
                        "frame_id": metadata.get("frame_id"),
                        "video_id": metadata.get("video_id"),
                        "timestamp": metadata.get("timestamp"),
                        "bucket_name": metadata.get("bucket_name"),
                        "object_name": metadata.get("object_name"),
                        "num_faces": result.get("num_faces"),
                        "emotions": result.get("emotions"),
                        "processed_at": datetime.now().isoformat()
                    }

                    results.append(result_obj)

                    # Lưu kết quả vào MinIO
                    try:
                        # Tạo tên đối tượng dựa trên video_id và frame_id
                        object_name = f"{metadata.get('video_id')}/{metadata.get('frame_id')}_emotion.json"

                        # Chuyển đổi kết quả thành JSON string
                        result_json = json.dumps(result_obj)

                        # Lưu vào MinIO
                        # Chuyển đổi byte string thành BytesIO object
                        from io import BytesIO
                        result_bytes = result_json.encode('utf-8')
                        result_stream = BytesIO(result_bytes)

                        minio_client.put_object(
                            bucket_name=emotion_results_bucket,
                            object_name=object_name,
                            data=result_stream,
                            length=len(result_bytes),
                            content_type="application/json"
                        )

                        logger.info(f"Saved emotion result to MinIO: {emotion_results_bucket}/{object_name}")
                    except Exception as e:
                        logger.error(f"Error saving to MinIO: {e}")

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

                    # Gửi kết quả đến Kafka
                    for result in results:
                        producer.send(emotion_results_topic, value=result)

                    # Đảm bảo dữ liệu đã được gửi hết
                    producer.flush()

                    logger.info(
                        f"Successfully processed batch {batch_id} and sent {len(results)} results to topic {emotion_results_topic}")

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