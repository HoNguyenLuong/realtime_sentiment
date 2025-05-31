import datetime
import json
from typing import Any, Dict, List
from src.consumer.common import logger
from src.producer.config import minio_client


def get_audio_sentiment_results(topic_name: str, video_id: str = None) -> List[Dict[Any, Any]]:
    """
    Lấy kết quả phân tích cảm xúc từ MinIO bucket cho audio.

    Args:
        topic_name (str): Tên của topic Kafka (chỉ dùng để xác định các thông số liên quan)
        video_id (str, optional): ID của video để lọc kết quả. Defaults to None.

    Returns:
        List[Dict[Any, Any]]: Danh sách kết quả phân tích cảm xúc âm thanh.
    """
    try:
        # Bucket nơi kết quả audio được lưu trữ
        audio_results_bucket = "audio-results"
        results = []

        # Liệt kê tất cả các objects trong bucket
        objects = minio_client.list_objects(audio_results_bucket, recursive=True)

        for obj in objects:
            try:
                # Lấy nội dung JSON từ object
                data = minio_client.get_object(audio_results_bucket, obj.object_name)
                content = data.read().decode('utf-8')
                audio_data = json.loads(content)

                # Lọc theo video_id nếu được cung cấp
                if video_id and audio_data.get("video_id") != video_id:
                    continue

                # Kiểm tra xem có các trường cần thiết không
                if all(key in audio_data for key in
                       ["chunk_id", "video_id", "text", "sentiment", "emotion", "timestamp", "processed_at"]):
                    # Chuyển đổi thời gian thành datetime object
                    try:
                        processed_time = datetime.datetime.fromisoformat(audio_data["processed_at"])
                        timestamp = datetime.datetime.fromisoformat(audio_data["timestamp"])

                        # Format thời gian đẹp hơn
                        processed_time_str = processed_time.strftime("%Y-%m-%d %H:%M:%S")
                        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        # Nếu không thể parse thời gian, giữ nguyên giá trị
                        processed_time_str = audio_data["processed_at"]
                        timestamp_str = audio_data["timestamp"]

                    # Tạo kết quả với định dạng đồng nhất
                    result = {
                        "chunk_id": audio_data["chunk_id"],
                        "video_id": audio_data["video_id"],
                        "text": audio_data["text"],
                        "sentiment": audio_data["sentiment"],
                        "emotion": audio_data["emotion"],
                        "extracted_at": timestamp_str,
                        "processed_at": processed_time_str
                    }

                    results.append(result)
            except Exception as e:
                logger.error(f"Lỗi khi xử lý object {obj.object_name}: {str(e)}")

        # Sắp xếp kết quả theo thời gian xử lý (mới nhất trước)
        results.sort(key=lambda x: x.get("processed_at", ""), reverse=True)

        return results
    except Exception as e:
        logger.error(f"Lỗi khi lấy kết quả audio: {str(e)}")
        return []