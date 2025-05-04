import datetime
import io
import json
from typing import Any, Dict, List
from PIL import Image
from src.consumer.common import get_kafka_producer, mark_as_processed, logger
from src.video_sentiment.sentiment_analysis import analyze_emotions
from src.producer.config import minio_client
import numpy as np

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


def get_sentiment_results(topic_data: str) -> List[Dict[Any, Any]]:
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
                "frame_id": data["frame_id"],
                "video_id": data["video_id"],
                "num_faces": data["num_faces"],
                "emotions": data["emotions"],
                "extracted_at": timestamp_str,
                "processed_at": processed_time_str
            }

            results.append(result)
        except json.JSONDecodeError:
            print(f"Lỗi khi parse JSON: {line}")
        except Exception as e:
            print(f"Lỗi xử lý dữ liệu: {str(e)}")

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