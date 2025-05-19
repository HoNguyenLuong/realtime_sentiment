import json
import numpy as np
from kafka import KafkaProducer
from src.producer.config import CONFIG
from .config import minio_client
import io
from datetime import datetime
from ..consumer.common import logger

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
    Upload each comment as a separate file to MinIO and send metadata to Kafka

    Args:
        video_id (str): YouTube video ID
        comments_data (list): List of parsed comments
    """
    timestamp = datetime.utcnow().isoformat()
    bucket_name = CONFIG['comments']['comments_bucket']
    object_prefix = f"{video_id}/comments/"

    # Create detailed log of what we're about to process
    logger.info(f"[send_comments] Processing {len(comments_data)} comments for video {video_id}")
    if len(comments_data) > 0:
        # Log a sample comment for debugging
        sample = comments_data[0]
        logger.info(f"[send_comments] Sample comment structure: {json.dumps(sample)[:200]}...")

    success_count = 0
    error_count = 0

    # Make sure MinIO client is properly initialized
    if not hasattr(minio_client, 'put_object'):
        logger.error("[send_comments] MinIO client is not properly initialized")
        return

    # Process each comment
    for idx, comment in enumerate(comments_data):
        try:
            # Convert comment to JSON string with proper encoding
            comment_json = json.dumps(comment, ensure_ascii=False)
            comment_bytes = comment_json.encode('utf-8')

            # Create BytesIO object from bytes
            comment_stream = io.BytesIO(comment_bytes)

            # Set unique object name for this comment
            object_name = f"{object_prefix}comment_{idx}_{comment.get('id', 'unknown')}.json"

            # Log before uploading
            logger.debug(
                f"[send_comments] Uploading comment {idx} to MinIO as {object_name}, size: {len(comment_bytes)} bytes")

            # Upload to MinIO with error handling
            try:
                minio_client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=comment_stream,
                    length=len(comment_bytes),
                    content_type="application/json"
                )
                logger.info(
                    f"[send_comments] Successfully uploaded comment {idx} for video {video_id} to MinIO as {object_name}")
                success_count += 1
            except Exception as upload_error:
                logger.error(f"[send_comments] MinIO upload error for comment {idx}: {upload_error}")
                error_count += 1

        except Exception as e:
            logger.error(f"[send_comments] Failed to process comment {idx} for video {video_id}: {e}")
            error_count += 1

    # Send metadata to Kafka
    try:
        send_metadata("video_comments", {
            "video_id": video_id,
            "type": "comments",
            "comment_count": len(comments_data),
            "success_count": success_count,
            "error_count": error_count,
            "bucket_name": bucket_name,
            "object_prefix": object_prefix,
            "timestamp": timestamp
        })
        logger.info(f"[send_comments] Sent metadata for {len(comments_data)} comments of video {video_id} to Kafka")
    except Exception as e:
        logger.error(f"[send_comments] Failed to send metadata for video {video_id} to Kafka: {e}")

    logger.info(
        f"[send_comments] Finished processing: {success_count} comments uploaded, {error_count} errors for video {video_id}")

def close_producer():
    producer.flush()
    producer.close()
