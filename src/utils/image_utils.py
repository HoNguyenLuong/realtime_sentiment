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

                    # Xử lý num_faces để đảm bảo là số nguyên
                    num_faces = data.get("num_faces", 0)
                    try:
                        num_faces = int(num_faces)  # Chuyển đổi thành số nếu là chuỗi
                    except (ValueError, TypeError):
                        num_faces = 0

                    # Xử lý emotions: đảm bảo đúng định dạng
                    emotions = data.get("emotions", [])
                    emotions_dict = {}

                    # Nếu emotions là list, chuyển thành dict với giá trị
                    if isinstance(emotions, list):
                        for emotion in emotions:
                            emotions_dict[emotion] = 1  # Gán giá trị 1 cho mỗi emotion được phát hiện
                    # Nếu đã là dict, sử dụng trực tiếp
                    elif isinstance(emotions, dict):
                        emotions_dict = emotions

                    # Đảm bảo tất cả các trường emotions đều có
                    for emotion in ["happy", "sad", "angry", "surprise", "fear", "neutral", "disgust"]:
                        if emotion not in emotions_dict:
                            emotions_dict[emotion] = 0

                    result = {
                        "frame_id": data.get("frame_id", 0),
                        "video_id": data.get("video_id", ""),
                        "num_faces": num_faces,
                        "emotions": emotions_dict,
                        "extracted_at": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "processed_at": processed_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                    results.append(result)

                    # Debug: In ra kết quả để kiểm tra
                    print(f"Processed frame: {result['frame_id']} with emotions: {result['emotions']}")

                except Exception as e:
                    logger.error(f"Lỗi xử lý message từ Kafka: {str(e)}")
                    print(f"Error processing message: {str(e)}")

        consumer.close()
    except Exception as e:
        logger.error(f"Lỗi đọc dữ liệu sentiment từ Kafka: {str(e)}")
        print(f"Error reading from Kafka: {str(e)}")

    # Debug: In ra tổng số kết quả
    print(f"Total frames processed: {len(results)}")
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