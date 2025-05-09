import datetime
import json
from typing import Any, Dict, List
from src.consumer.common import get_kafka_consumer, logger

def get_audio_sentiment_results(topic_name: str) -> List[Dict[Any, Any]]:
    """
    Lấy kết quả phân tích cảm xúc từ Kafka topic cho audio.

    Args:
        topic_name (str): Tên của Kafka topic.

    Returns:
        List[Dict[Any, Any]]: Danh sách kết quả phân tích cảm xúc âm thanh.
    """
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

                    # Đảm bảo data có tất cả các trường cần thiết
                    result = {
                        "chunk_id": data.get("chunk_id", ""),
                        "video_id": data.get("video_id", ""),  # Thay đổi từ audio_id thành video_id để phù hợp với code mới
                        "text": data.get("text", ""),
                        "sentiment": data.get("sentiment", {}),
                        "emotion": data.get("emotion", []),  # Changed from {} to [] to match JSON format
                        "extracted_at": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "processed_at": processed_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    results.append(result)
                except Exception as e:
                    logger.error(f"Lỗi xử lý message âm thanh từ Kafka: {str(e)}")

        consumer.close()
    except Exception as e:
        logger.error(f"Lỗi đọc dữ liệu âm thanh từ Kafka: {str(e)}")

    return results