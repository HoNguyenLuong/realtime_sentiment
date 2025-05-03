import io
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

            # Sau khi xử lý xong và kết quả đã gửi đi, xóa object từ MinIO
            try:
                minio_client.remove_object(bucket_name, object_name)
                logger.info(f"Đã xóa frame đã xử lý: {object_name}")
            except Exception as e:
                logger.error(f"Lỗi khi xóa frame từ MinIO: {str(e)}")

            return {"num_faces": num_faces, "emotions": emotions}
        else:
            return {"num_faces": 0, "emotions": []}
    except Exception as e:
        logger.error(f"Lỗi khi xử lý frame: {str(e)}")
        return {"num_faces": 0, "emotions": []}