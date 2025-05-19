from datetime import datetime
import json
from typing import Any, Dict, List
import traceback
import uuid
from src.comment_sentiment.comment_extraction import CommentExtractor
from src.consumer.common import get_kafka_consumer, logger
from src.producer.config import minio_client, CONFIG
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
    Lấy comment JSON từ MinIO

    Args:
        bucket_name (str): Tên bucket
        object_name (str): Tên object

    Returns:
        dict: Dữ liệu comment từ file JSON hoặc None nếu có lỗi
    """
    try:
        logger.info(f"[get_comments_from_minio] Getting comment from {bucket_name}/{object_name}")
        response = minio_client.get_object(bucket_name, object_name)
        comment_data = json.loads(response.read().decode('utf-8'))
        logger.info(f"[get_comments_from_minio] Successfully got comment data from {object_name}")

        # Trả về trực tiếp object comment - không wrap thêm
        return comment_data
    except Exception as e:
        logger.error(f"[get_comments_from_minio] Error getting data from MinIO: {str(e)}")
        logger.error(traceback.format_exc())
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
        logger.info(f"[process_comment] Processing comments for video {video_id}")

        # Xử lý từng comment riêng lẻ
        if isinstance(comments_data, list):
            logger.info(f"[process_comment] Processing {len(comments_data)} comments")
            for comment in comments_data:
                # Phân tích sentiment và thông tin khác cho từng comment
                process_single_comment(video_id, comment)
        elif isinstance(comments_data, dict) and "comments" in comments_data:
            comments = comments_data.get("comments", [])
            logger.info(f"[process_comment] Processing {len(comments)} comments from comments dictionary")
            for comment in comments:
                process_single_comment(video_id, comment)
        else:
            logger.warning(f"[process_comment] Unsupported comments data format")

    except Exception as e:
        logger.error(f"[process_comment] Error processing comments: {e}")
        logger.error(traceback.format_exc())


def process_single_comment(video_id, comment):
    """Process a single comment"""
    try:
        if not comment or "text" not in comment:
            return

        # Phân tích ngôn ngữ
        text = comment.get("text", "")
        language = language_detector.detect_language(text)

        # Phân tích sentiment
        sentiment = sentiment_analyzer.analyze(text)

        # Phân tích emoji
        emoji_analysis = emoji_analyzer.analyze(text)

        # Tạo kết quả
        result = {
            "comment_id": comment.get("id", "unknown"),
            "content_id": video_id,
            "text": text,
            "author": comment.get("author", "unknown"),
            "timestamp": comment.get("timestamp", ""),
            "language": language,
            "sentiment": sentiment.get("sentiment", "neutral"),
            "confidence": sentiment.get("confidence", {}),
            "emoji_sentiment": emoji_analysis.get("emoji_sentiment", "neutral"),
            "emoji_score": emoji_analysis.get("emoji_score", 0.0),
            "emojis_found": emoji_analysis.get("emojis_found", []),
            "processed_at": datetime.now().isoformat()
        }

        # TODO: Lưu kết quả vào MinIO hoặc gửi đến Kafka

    except Exception as e:
        logger.error(f"Error processing single comment: {e}")


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
        # Create a fresh consumer each time with a unique group ID

        unique_group = f"comment_consumer_{uuid.uuid4().hex[:8]}"

        consumer = get_kafka_consumer(topic_name)
        # Get messages directly instead of using poll()
        logger.info(f"[get_sentiment_results] Reading messages from topic {topic_name}")

        message_count = 0
        for message in consumer:
            try:
                data = message.value

                # Process message data...
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
                message_count += 1
            except Exception as e:
                logger.error(f"Error processing message: {e}")

        logger.info(f"[get_sentiment_results] Processed {message_count} messages from {topic_name}")
        consumer.close()
    except Exception as e:
        logger.error(f"Error reading sentiment data from Kafka: {e}")

    return results