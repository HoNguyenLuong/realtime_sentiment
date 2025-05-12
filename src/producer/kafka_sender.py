import json
import numpy as np
from kafka import KafkaProducer
from src.producer.config import CONFIG
from .config import minio_client
import io
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(4, 0)
)

def send_metadata(topic, metadata: dict):
    """Send metadata to Kafka topic"""
    producer.send(topic, value=metadata)
    producer.flush()  # Đảm bảo dữ liệu đã được gửi hết trước khi tiếp tục

def send_frame(video_id, frame_id, frame_bytes):
    """Upload raw frame to MinIO and send metadata to Kafka"""
    timestamp = datetime.utcnow().isoformat()
    object_name = f"{video_id}/frame_{frame_id}.jpg"

    # Sử dụng bucket_name từ CONFIG
    bucket_name = CONFIG['video']['frames_bucket']

    # Convert bytes to BytesIO stream
    frame_stream = io.BytesIO(frame_bytes)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=frame_stream,
        length=len(frame_bytes),
        content_type="image/jpeg"
    )

    # Send metadata
    send_metadata("video_frames", {
        "video_id": video_id,
        "frame_id": frame_id,
        "type": "frame",
        "bucket_name": bucket_name,  # Thêm bucket_name vào metadata
        "object_name": object_name,  # Thay minio_path bằng object_name
        "timestamp": timestamp
    })

def send_audio_file(video_id,chunk_id, audio_data, ext="wav"):
    """Upload audio file to MinIO and send metadata to Kafka"""
    timestamp = datetime.utcnow().isoformat()
    object_name = f"{video_id}/audio.{ext}"

    # Sử dụng bucket_name từ CONFIG
    bucket_name = CONFIG['audio']['audio_bucket']

    # Convert bytes to BytesIO stream
    audio_stream = io.BytesIO(audio_data)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=audio_stream,
        length=len(audio_data),
        content_type="audio/wav"
    )
    # Send metadata
    send_metadata("audio_stream", {
        "video_id": video_id,
        "chunk_id": chunk_id,
        "type": "audio",
        "format": ext,
        "bucket_name": bucket_name,  # Thêm bucket_name vào metadata
        "object_name": object_name,  # Thay minio_path bằng object_name
        "timestamp": timestamp
    })

def send_comments(video_id, comments_data):
    """
    Upload comments data to MinIO and send metadata to Kafka

    Args:
        video_id (str): ID của video YouTube
        comments_data (list): Danh sách các comments đã được parse
    """
    timestamp = datetime.utcnow().isoformat()
    object_name = f"{video_id}/comments.json"

    # Sử dụng bucket_name từ CONFIG
    bucket_name = CONFIG['comments']['comments_bucket']

    # Chuyển comments thành định dạng JSON để lưu trữ
    comments_json = json.dumps(comments_data, ensure_ascii=False)
    comments_bytes = comments_json.encode('utf-8')

    # Tạo stream từ bytes
    comments_stream = io.BytesIO(comments_bytes)

    # Upload to MinIO
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=comments_stream,
        length=len(comments_bytes),
        content_type="application/json"
    )

    # Gửi metadata
    send_metadata("video_comments", {
        "video_id": video_id,
        "type": "comments",
        "comment_count": len(comments_data),
        "bucket_name": bucket_name,
        "object_name": object_name,
        "timestamp": timestamp
    })

def close_producer():
    producer.flush()
    producer.close()
