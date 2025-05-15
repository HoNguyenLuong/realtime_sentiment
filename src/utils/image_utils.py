from datetime import datetime
import io
import json
from typing import Any, Dict, List
from PIL import Image
from src.consumer.common import get_kafka_consumer, mark_as_processed, logger
from src.producer.controller import get_kafka_producer
from src.video_sentiment.sentiment_analysis import analyze_emotions
from src.producer.config import minio_client
import numpy as np
import json

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
    try:
        # Các xử lý hiện tại...
        bucket_name = metadata_row["bucket_name"]
        object_name = metadata_row["object_name"]

        # Lấy frame từ MinIO
        frame = get_frame_from_minio(bucket_name, object_name)

        if frame is not None:
            # Phân tích cảm xúc
            num_faces, emotions = analyze_emotions(frame)

            # Đánh dấu đã xử lý trong Kafka
            producer = get_kafka_producer()
            mark_as_processed(producer, metadata_row)

            return {"num_faces": num_faces, "emotions": emotions}
        else:
            return {"num_faces": 0, "emotions": []}
    except Exception as e:
        logger.error(f"Lỗi khi xử lý frame: {str(e)}")
        return {"num_faces": 0, "emotions": []}

def get_sentiment_results(topic_name: str) -> list:
    """
    Đọc và xử lý dữ liệu cảm xúc từ Kafka topic
    """
    results = []
    try:
        consumer = get_kafka_consumer(topic_name)
        messages = consumer.poll(timeout_ms=5000, max_records=1000)

        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                try:
                    data = message.value

                    # Log dữ liệu gốc để debug
                    logger.debug(f"Raw data from Kafka: {data}")

                    # Xử lý frame_id để đảm bảo là string
                    frame_id = str(data.get("frame_id", "0"))

                    # Xử lý emotions để đảm bảo định dạng đúng
                    emotions = data.get("emotions", [])

                    # Chuyển emotions array thành object nếu cần
                    emotions_dict = {}
                    if isinstance(emotions, list):
                        for emotion in emotions:
                            emotions_dict[emotion] = 1
                    elif isinstance(emotions, dict):
                        emotions_dict = emotions

                    # Đảm bảo tất cả các loại cảm xúc đều có trong dict
                    for emotion in ["happy", "sad", "angry", "surprise", "fear", "neutral", "disgust"]:
                        if emotion not in emotions_dict:
                            emotions_dict[emotion] = 0

                    # Định dạng lại thời gian để dễ đọc
                    processed_at = data.get("processed_at")
                    if processed_at:
                        try:
                            dt = datetime.fromisoformat(processed_at.replace('Z', '+00:00'))
                            processed_at = dt.strftime("%Y-%m-%d %H:%M:%S")
                        except (ValueError, TypeError):
                            processed_at = str(processed_at)

                    # Tạo object kết quả theo định dạng chuẩn
                    result = {
                        "frame_id": frame_id,
                        "video_id": data.get("video_id", ""),
                        "num_faces": int(data.get("num_faces", 0)),
                        "emotions": emotions_dict,
                        "processed_at": processed_at
                    }

                    results.append(result)
                    logger.info(f"Processed frame {frame_id} with {len(emotions_dict)} emotions")

                except Exception as e:
                    logger.error(f"Error processing Kafka message: {str(e)}")

        consumer.close()
        logger.info(f"Total frames processed: {len(results)}")

    except Exception as e:
        logger.error(f"Error reading sentiment data from Kafka: {str(e)}")

    return results

# Phiên bản cài tiến --> đánh dấu các frame đã được xử lý bằng commit ở offset thay vì lưu ở topic mới.
# def process_frame(metadata_row, consumer):
#     try:
#         bucket_name = metadata_row["bucket_name"]
#         object_name = metadata_row["object_name"]
#
#         # Lấy frame từ MinIO
#         frame = get_frame_from_minio(bucket_name, object_name)
#
#         if frame is not None:
#             # Phân tích cảm xúc
#             num_faces, emotions = analyze_emotions(frame)
#
#             # Lưu kết quả phân tích vào database hoặc hệ thống lưu trữ khác nếu cần
#             # (Không gửi vào topic Kafka khác)
#
#             # Xóa frame từ MinIO sau khi xử lý
#             try:
#                 minio_client.remove_object(bucket_name, object_name)
#                 logger.info(f"Đã xóa frame đã xử lý: {object_name}")
#             except Exception as e:
#                 logger.error(f"Lỗi khi xóa frame từ MinIO: {str(e)}")
#
#             # Đánh dấu message đã được xử lý bằng cách commit offset
#             consumer.commit()
#
#             return {"num_faces": num_faces, "emotions": emotions}
#         else:
#             # Vẫn commit offset ngay cả khi không có frame
#             consumer.commit()
#             return {"num_faces": 0, "emotions": []}
#     except Exception as e:
#         logger.error(f"Lỗi khi xử lý frame: {str(e)}")
#         # Trong trường hợp lỗi, bạn có thể quyết định không commit offset
#         # để message có thể được xử lý lại, tùy thuộc vào yêu cầu của ứng dụng
#         return {"num_faces": 0, "emotions": []}