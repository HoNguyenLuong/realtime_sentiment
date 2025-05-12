from datetime import datetime
import json
from typing import Any, Dict, List

from src.comment_sentiment.comment_extraction import CommentExtractor
from src.consumer.common import get_kafka_consumer, logger
from src.producer.config import minio_client
from src.comment_sentiment.sentiment_analysis import SentimentAnalyzer
from src.comment_sentiment.emoji_analysis import EmojiAnalyzer
from src.comment_sentiment.language_detection import LanguageDetector

# Khởi tạo các analyzer
sentiment_analyzer = SentimentAnalyzer()
emoji_analyzer = EmojiAnalyzer()
language_detector = LanguageDetector()
comment_extractor = CommentExtractor()

def get_comments_from_minio(bucket_name, object_name):
    """
    Lấy comments JSON từ MinIO

    Args:
        bucket_name (str): Tên bucket
        object_name (str): Tên object

    Returns:
        dict: Toàn bộ dữ liệu từ file JSON hoặc None nếu có lỗi
    """
    try:
        # Lấy đối tượng từ MinIO
        response = minio_client.get_object(bucket_name, object_name)

        # Đọc dữ liệu JSON
        json_data = json.loads(response.read().decode('utf-8'))

        logger.info(f"Đã lấy dữ liệu JSON từ MinIO: {object_name}")
        return json_data

    except Exception as e:
        logger.error(f"Lỗi khi lấy dữ liệu từ MinIO: {str(e)}")
        return None
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

def process_comment(video_id, comments_data):
    """
    Xử lý dữ liệu comments và lưu vào MinIO

    Args:
        video_id (str): ID của video YouTube
        comments_data (dict): Dictionary chứa thông tin video và danh sách comments
    """
    try:
        logger.info(f"Processing {len(comments_data['comments'])} comments for video {video_id}")

        # Ví dụ: Lưu toàn bộ JSON vào file
        with open(f"{video_id}_comments.json", "w", encoding="utf-8") as f:
            json.dump(comments_data, f, ensure_ascii=False, indent=2)

        logger.info(f"Successfully saved comments to {video_id}_comments.json")

        # Lưu vào MinIO
        try:
            bucket_name = "comments"
            object_name = f"{video_id}/comments.json"

            # Đảm bảo bucket tồn tại
            if not minio_client.bucket_exists(bucket_name):
                minio_client.make_bucket(bucket_name)
                logger.info(f"Created new bucket: {bucket_name}")

            # Chuyển đổi JSON thành bytes
            json_data = json.dumps(comments_data, ensure_ascii=False).encode('utf-8')
            from io import BytesIO
            data_stream = BytesIO(json_data)

            # Lưu vào MinIO
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=data_stream,
                length=len(json_data),
                content_type="application/json"
            )

            logger.info(f"Successfully uploaded comments to MinIO: {bucket_name}/{object_name}")

            # Gửi thông báo tới Kafka rằng có comment mới
            notification = {
                "content_id": video_id,
                "bucket_name": bucket_name,
                "object_name": object_name,
                "timestamp": datetime.now().isoformat()
            }

            # Gửi thông báo tới Kafka
            from src.producer.kafka_sender import producer
            producer.send("comments", value=notification)
            producer.flush()

            logger.info(f"Sent notification to Kafka for {video_id}")

        except Exception as e:
            logger.error(f"Error saving to MinIO or sending to Kafka: {e}")

    except Exception as e:
        logger.error(f"Error processing comments: {e}")
        logger.exception(e)

def get_default_result():
    return {
        "language": "unknown",
        "sentiment": "neutral",
        "confidence": {"negative": 0.0, "neutral": 1.0, "positive": 0.0},
        "emoji_sentiment": "neutral",
        "emoji_score": 0.0,
        "emojis_found": []
    }

def get_sentiment_results(topic_name: str) -> List[Dict[Any, Any]]:
    results = []
    try:
        consumer = get_kafka_consumer(topic_name)
        messages = consumer.poll(timeout_ms=5000, max_records=1000)

        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                try:
                    data = message.value

                    # Xử lý thời gian
                    processed_time = datetime.fromisoformat(data.get("processed_at", datetime.now().isoformat()))
                    timestamp = datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat()))

                    # Xử lý confidence scores
                    confidence = data.get("confidence", {"negative": 0.0, "neutral": 1.0, "positive": 0.0})

                    # Xử lý emojis để đảm bảo là list
                    emojis_found = data.get("emojis_found", [])
                    if isinstance(emojis_found, str):
                        try:
                            emojis_found = json.loads(emojis_found)
                        except:
                            emojis_found = []

                    result = {
                        "comment_id": data.get("comment_id", ""),
                        "content_id": data.get("content_id", ""),
                        "language": data.get("language", "unknown"),
                        "sentiment": data.get("sentiment", "neutral"),
                        "confidence": confidence,
                        "emoji_sentiment": data.get("emoji_sentiment", "neutral"),
                        "emoji_score": data.get("emoji_score", 0.0),
                        "emojis_found": emojis_found,
                        "extracted_at": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "processed_at": processed_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    results.append(result)
                except Exception as e:
                    logger.error(f"Lỗi xử lý message từ Kafka: {str(e)}")

        consumer.close()
    except Exception as e:
        logger.error(f"Lỗi đọc dữ liệu sentiment từ Kafka: {str(e)}")

    return results