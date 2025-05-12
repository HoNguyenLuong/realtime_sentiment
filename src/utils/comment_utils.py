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

def process_comment(metadata_row):
    try:
        bucket_name = metadata_row["bucket_name"]
        object_name = metadata_row["object_name"]
        content_id = metadata_row.get("content_id", "unknown")

        # Lấy toàn bộ dữ liệu từ MinIO
        video_data = get_comments_from_minio(bucket_name, object_name)

        if video_data is not None and "comments" in video_data:
            results = []

            # Lặp qua tất cả comment trong file
            for comment in video_data["comments"]:
                if "text" in comment:
                    comment_id = comment.get("id", "unknown")
                    comment_text = comment.get("text", "")
                    author = comment.get("author", "unknown")
                    timestamp = comment.get("timestamp", 0)

                    # Phát hiện ngôn ngữ
                    language = language_detector.detect_language(comment_text)

                    # Phân tích cảm xúc văn bản
                    sentiment_result = sentiment_analyzer.analyze(comment_text)

                    # Phân tích emoji
                    emoji_result = emoji_analyzer.analyze(comment_text)

                    # Kết hợp kết quả
                    result = {
                        "comment_id": comment_id,
                        "content_id": content_id,
                        "author": author,
                        "timestamp": timestamp,
                        "language": language,
                        "sentiment": sentiment_result["sentiment"],
                        "confidence": sentiment_result["confidence"],
                        "emoji_sentiment": emoji_result["emoji_sentiment"],
                        "emoji_score": emoji_result["emoji_score"],
                        "emojis_found": emoji_result["emojis_found"]
                    }

                    results.append(result)

            # Trả về kết quả của comment được chỉ định trong metadata
            # Hoặc kết quả đầu tiên nếu không có comment_id cụ thể
            target_comment_id = metadata_row.get("comment_id")
            if target_comment_id:
                for result in results:
                    if result["comment_id"] == target_comment_id:
                        return result

            # Nếu không tìm thấy comment cụ thể hoặc không có comment_id, trả về kết quả mặc định
            return results[0] if results else get_default_result()
        else:
            return get_default_result()
    except Exception as e:
        logger.error(f"Lỗi khi xử lý comment: {str(e)}")
        return get_default_result()

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