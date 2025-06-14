from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, MapType, FloatType
from src.utils.comment_utils import get_comments_from_minio, comment_extractor
from src.comment_sentiment.sentiment_analysis import SentimentAnalyzer
from src.comment_sentiment.emoji_analysis import EmojiAnalyzer
from src.comment_sentiment.language_detection import LanguageDetector
from .common import logger, COMMENT_TOPIC
from src.producer.config import CONFIG, minio_client
from datetime import datetime
import json
from src.producer.kafka_sender import producer
import traceback
from collections import defaultdict, Counter

# Khởi tạo các analyzer
sentiment_analyzer = SentimentAnalyzer()
emoji_analyzer = EmojiAnalyzer()
language_detector = LanguageDetector()


def run():
    try:
        logger.info("[spark_comment] >>>>>>>>>>>> run() called, starting comment consumer <<<<<<<<<<<<")
        sentiment_results_topic = "comment_sentiment_results"
        sentiment_results_bucket = "comment-sentiment-results"

        if not minio_client.bucket_exists(sentiment_results_bucket):
            minio_client.make_bucket(sentiment_results_bucket)
            logger.info(f"Created new bucket: {sentiment_results_bucket}")

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

        # Loại bỏ checkpoint cũ để đảm bảo đọc lại từ đầu
        import subprocess
        try:
            subprocess.run(['rm', '-rf', '/tmp/checkpoint_comment'], check=True)
            logger.info("[spark_comment] Removed old checkpoint directory")
        except Exception as e:
            logger.warning(f"[spark_comment] Could not remove checkpoint: {e}")

        # Schema đúng với metadata từ kafka_sender.py
        schema = StructType() \
            .add("video_id", StringType()) \
            .add("type", StringType()) \
            .add("comment_count", IntegerType()) \
            .add("success_count", IntegerType()) \
            .add("error_count", IntegerType()) \
            .add("bucket_name", StringType()) \
            .add("object_prefix", StringType()) \
            .add("timestamp", StringType())

        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['kafka']['bootstrap_servers']) \
            .option("subscribe", COMMENT_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        def process_batch(batch_df, batch_id):
            logger.info(f"[spark_comment] process_batch called for batch_id={batch_id}, count={batch_df.count()}")
            if not batch_df.isEmpty():
                pandas_df = batch_df.toPandas()

                # Dictionary to group comments by video_id
                video_results = {}

                for _, row in pandas_df.iterrows():
                    metadata = row.to_dict()
                    bucket_name = metadata.get("bucket_name")
                    object_prefix = metadata.get("object_prefix")
                    content_id = metadata.get("video_id", "unknown")
                    comment_count = metadata.get("comment_count", 0)

                    # Initialize video results if not exists
                    if content_id not in video_results:
                        video_results[content_id] = {
                            "video_id": content_id,
                            "processed_comments": 0,
                            "sentiment_counts": {"positive": 0, "negative": 0, "neutral": 0},
                            "emoji_counts": {"positive": 0, "negative": 0, "neutral": 0},
                            "languages": Counter(),
                            "details": [],
                            "processed_at": datetime.now().isoformat()
                        }

                    logger.info(f"[spark_comment] Processing {comment_count} comments with prefix: {object_prefix}")

                    try:
                        # Liệt kê tất cả objects trong prefix
                        objects = list(minio_client.list_objects(bucket_name, prefix=object_prefix))
                        logger.info(f"[spark_comment] Found {len(objects)} comment objects with prefix {object_prefix}")

                        for obj in objects:
                            process_single_comment(obj.object_name, bucket_name, content_id,
                                                   sentiment_results_bucket, video_results)
                    except Exception as e:
                        logger.error(f"[spark_comment] Error listing objects with prefix {object_prefix}: {e}")
                        logger.error(traceback.format_exc())

                # Instead of sending individual messages, send one summary message per video
                processed_count = 0
                for video_id, result in video_results.items():
                    try:
                        # Create a summary message similar to video_comments topic but with analysis results
                        summary_message = {
                            "video_id": video_id,
                            "type": "comments_analysis",
                            "comment_count": result["processed_comments"],
                            "success_count": result["processed_comments"],  # All comments successfully processed
                            "error_count": 0,  # We didn't track errors individually
                            "sentiment_summary": result["sentiment_counts"],
                            "emoji_summary": result["emoji_counts"],
                            "languages_detected": dict(result["languages"]),
                            "status": "processed",
                            "bucket_name": sentiment_results_bucket,  # Pointing to results bucket
                            "processed_at": result["processed_at"],
                            "timestamp": datetime.now().isoformat()
                        }

                        # Send the summary message to Kafka
                        producer.send(sentiment_results_topic, value=summary_message)
                        producer.flush()

                        processed_count += 1
                        logger.info(
                            f"[spark_comment] Sent summary message for video {video_id} to topic {sentiment_results_topic}")
                    except Exception as e:
                        logger.error(f"[spark_comment] Failed to send summary for video {video_id} to Kafka: {str(e)}")
                        logger.error(traceback.format_exc())

                logger.info(
                    f"[spark_comment] Successfully processed batch {batch_id} and sent {processed_count} summary messages to topic {sentiment_results_topic}")

        def process_single_comment(object_name, bucket_name, content_id, sentiment_results_bucket, video_results):
            logger.info(f"[spark_comment] process_single_comment called for object: {bucket_name}/{object_name}")
            try:
                # Lấy dữ liệu comment từ MinIO
                comment_obj = get_comments_from_minio(bucket_name, object_name)
                logger.info(f"[spark_comment] Data from MinIO: {str(comment_obj)[:200]}...")

                if not comment_obj:
                    logger.warning(f"[spark_comment] No comment data in {bucket_name}/{object_name}")
                    return

                # Phân tích ngôn ngữ
                comment_text = comment_obj.get("text", "")
                if not comment_text:
                    logger.warning(f"[spark_comment] Empty comment text in {object_name}")
                    return

                language = language_detector.detect_language(comment_text)
                # Phân tích sentiment
                sentiment_result = sentiment_analyzer.analyze(comment_text)
                # Phân tích emoji
                emoji_result = emoji_analyzer.analyze(comment_text)

                result = {
                    "comment_id": comment_obj.get("id", "unknown"),
                    "content_id": content_id,
                    "author": comment_obj.get("author", "unknown"),
                    "timestamp": str(comment_obj.get("timestamp", "")),
                    "language": language,
                    "sentiment": sentiment_result.get("sentiment", "neutral"),
                    "confidence": sentiment_result.get("confidence",
                                                       {"negative": 0.0, "neutral": 1.0, "positive": 0.0}),
                    "emoji_sentiment": emoji_result.get("emoji_sentiment", "neutral"),
                    "emoji_score": emoji_result.get("emoji_score", 0.0),
                    "emojis_found": emoji_result.get("emojis_found", []),
                    "bucket_name": bucket_name,
                    "object_name": object_name,
                    "processed_at": datetime.now().isoformat()
                }

                # Update the aggregated video results
                video_results[content_id]["processed_comments"] += 1
                video_results[content_id]["sentiment_counts"][sentiment_result.get("sentiment", "neutral")] += 1
                video_results[content_id]["emoji_counts"][emoji_result.get("emoji_sentiment", "neutral")] += 1
                video_results[content_id]["languages"][language] += 1

                # Optional: store detailed results (but don't include in Kafka message)
                video_results[content_id]["details"].append({
                    "comment_id": comment_obj.get("id", "unknown"),
                    "sentiment": sentiment_result.get("sentiment", "neutral"),
                    "emoji_sentiment": emoji_result.get("emoji_sentiment", "neutral")
                })

                # Lưu kết quả vào MinIO
                try:
                    result_object_name = f"{content_id}/{comment_obj.get('id', 'unknown')}_sentiment.json"
                    result_json = json.dumps(result)
                    from io import BytesIO
                    result_bytes = result_json.encode('utf-8')
                    result_stream = BytesIO(result_bytes)
                    minio_client.put_object(
                        bucket_name=sentiment_results_bucket,
                        object_name=result_object_name,
                        data=result_stream,
                        length=len(result_bytes),
                        content_type="application/json"
                    )
                    logger.info(
                        f"[spark_comment] Saved sentiment result to MinIO: {sentiment_results_bucket}/{result_object_name}")
                except Exception as e:
                    logger.error(f"[spark_comment] Error saving to MinIO: {e}")
            except Exception as e:
                logger.error(f"[spark_comment] Error processing comment {object_name}: {e}")
                logger.error(traceback.format_exc())

        query = json_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error(f"[comment_consumer.run] Error: {e}")
        logger.error(traceback.format_exc())
    finally:
        if 'spark' in locals():
            spark.stop()