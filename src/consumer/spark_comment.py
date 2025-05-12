from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, MapType, FloatType
from src.utils.comment_utils import get_comments_from_minio, comment_extractor, process_comment
from .common import logger, COMMENT_TOPIC
from src.producer.config import CONFIG, minio_client
from datetime import datetime
import json
from src.producer.kafka_sender import producer


def run():
    try:
        # Định nghĩa tên topic cho kết quả xử lý sentiment
        sentiment_results_topic = "comment_sentiment_results"

        # Cấu hình bucket MinIO để lưu kết quả sentiment
        sentiment_results_bucket = "comment-sentiment-results"

        # Đảm bảo bucket tồn tại
        if not minio_client.bucket_exists(sentiment_results_bucket):
            minio_client.make_bucket(sentiment_results_bucket)
            logger.info(f"Created new bucket: {sentiment_results_bucket}")

        # Khởi tạo Spark session kết nối với Spark master bên ngoài
        spark = SparkSession.builder \
            .appName("CommentProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint_comment") \
            .config("spark.driver.extraJavaOptions", "-Dkafka.bootstrap.servers=kafka:9092") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
            .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Schema dữ liệu Kafka gửi tới - Đây là metadata từ MinIO
        schema = StructType() \
            .add("comment_id", StringType()) \
            .add("content_id", StringType()) \
            .add("bucket_name", StringType()) \
            .add("object_name", StringType()) \
            .add("timestamp", StringType())

        # Đọc từ Kafka topic comments
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['kafka']['bootstrap_servers']) \
            .option("subscribe", COMMENT_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Giải mã và parse JSON
        json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # Sử dụng foreachBatch để xử lý từng batch
        def process_batch(batch_df, batch_id):
            """
            Xử lý từng batch dữ liệu từ Kafka stream
            Sử dụng hàm process_comment để xử lý từng comment riêng lẻ
            """
            if not batch_df.isEmpty():
                # Convert DataFrame to pandas để xử lý từng hàng
                pandas_df = batch_df.toPandas()

                # Định nghĩa tên topic cho kết quả xử lý sentiment
                sentiment_results_topic = "comment_sentiment_results"

                # Cấu hình bucket MinIO để lưu kết quả sentiment
                sentiment_results_bucket = "comment-sentiment-results"

                # Xử lý từng hàng (mỗi hàng là một file JSON)
                all_results = []
                for _, row in pandas_df.iterrows():
                    if hasattr(row, 'asDict'):
                        metadata = row.asDict()
                    else:
                        metadata = row.to_dict()

                    bucket_name = metadata.get("bucket_name")
                    object_name = metadata.get("object_name")
                    content_id = metadata.get("content_id", "unknown")

                    # Lấy toàn bộ dữ liệu từ MinIO
                    video_data = get_comments_from_minio(bucket_name, object_name)

                    if video_data is not None:
                        # Sử dụng CommentExtractor để trích xuất và chuẩn hóa comments
                        comments = comment_extractor.extract_comments(video_data)

                        # Xử lý từng comment đã được chuẩn hóa
                        for comment_obj in comments:
                            # Tạo metadata cho comment hiện tại
                            comment_metadata = {
                                "bucket_name": bucket_name,
                                "object_name": object_name,
                                "content_id": content_id,
                                "comment_id": comment_obj["id"]
                            }

                            # Kết hợp comment với metadata
                            video_data_with_comment = {"comments": [comment_obj]}

                            # Sử dụng hàm process_comment để xử lý comment
                            result = process_comment(comment_metadata)

                            # Bổ sung thêm thông tin cho result
                            result.update({
                                "comment_id": comment_obj["id"],
                                "content_id": content_id,
                                "author": comment_obj["author"],
                                "timestamp": str(comment_obj["timestamp"]),
                                "bucket_name": bucket_name,
                                "object_name": object_name,
                                "processed_at": datetime.now().isoformat()
                            })

                            all_results.append(result)

                            # Lưu kết quả vào MinIO
                            try:
                                # Tạo tên đối tượng dựa trên content_id và comment_id
                                object_name = f"{content_id}/{comment_obj['id']}_sentiment.json"

                                # Chuyển đổi kết quả thành JSON string
                                result_json = json.dumps(result)

                                # Lưu vào MinIO
                                from io import BytesIO
                                result_bytes = result_json.encode('utf-8')
                                result_stream = BytesIO(result_bytes)

                                minio_client.put_object(
                                    bucket_name=sentiment_results_bucket,
                                    object_name=object_name,
                                    data=result_stream,
                                    length=len(result_bytes),
                                    content_type="application/json"
                                )

                                logger.info(
                                    f"Saved sentiment result to MinIO: {sentiment_results_bucket}/{object_name}")
                            except Exception as e:
                                logger.error(f"Error saving to MinIO: {e}")

                # Gửi kết quả đến Kafka
                if all_results:
                    for result in all_results:
                        producer.send(sentiment_results_topic, value=result)

                    # Đảm bảo dữ liệu đã được gửi hết
                    producer.flush()

                    logger.info(
                        f"Successfully processed batch {batch_id} and sent {len(all_results)} results to topic {sentiment_results_topic}")

        # Sử dụng foreachBatch để xử lý từng batch
        query = json_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        # Chờ stream kết thúc
        query.awaitTermination()

    except Exception as e:
        logger.error(f"[comment_consumer.run] Error: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()