import datetime
import json
from typing import Any, Dict, List
from src.consumer.common import logger

def get_audio_sentiment_results(topic_data: str) -> List[Dict[Any, Any]]:
    """
    Lấy kết quả phân tích cảm xúc từ Kafka topic cho audio.

    Args:
        topic_data (str): Dữ liệu từ Kafka topic dạng chuỗi JSON lines.

    Returns:
        List[Dict[Any, Any]]: Danh sách kết quả phân tích cảm xúc âm thanh.
    """
    json_lines = topic_data.strip().split('\n')
    results = []

    for line in json_lines:
        try:
            data = json.loads(line)

            # Chuyển đổi thời gian thành datetime object
            processed_time = datetime.datetime.fromisoformat(data["processed_at"])
            timestamp = datetime.datetime.fromisoformat(data["timestamp"])

            # Format thời gian đẹp hơn
            processed_time_str = processed_time.strftime("%Y-%m-%d %H:%M:%S")
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

            result = {
                "chunk_id": data["chunk_id"],
                "audio_id": data["audio_id"],
                "text": data["text"],
                "sentiment": data["sentiment"],
                "emotion": data["emotion"],
                "extracted_at": timestamp_str,
                "processed_at": processed_time_str
            }

            results.append(result)
        except json.JSONDecodeError:
            logger.error(f"Lỗi khi parse JSON: {line}")
        except Exception as e:
            logger.error(f"Lỗi xử lý dữ liệu âm thanh: {str(e)}")

    return results