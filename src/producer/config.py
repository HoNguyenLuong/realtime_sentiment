import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

CONFIG = {
    'kafka': {
        'bootstrap_servers': os.getenv('KAFKA_SERVERS', 'kafka1:9092,kafka2:9094,kafka3:9096'),
    },
    'video': {
        'width': int(os.getenv('VIDEO_WIDTH', 640)),
        'height': int(os.getenv('VIDEO_HEIGHT', 360)),
        'frame_format': os.getenv('FRAME_FORMAT', '.jpg'),
        'frames_bucket': os.getenv('FRAMES_BUCKET', 'youtube-frames'),  # Thêm cấu hình bucket frames
    },
    'audio': {
        'chunk_size': int(os.getenv('AUDIO_CHUNK_SIZE', 4096)),
        'audio_bucket': os.getenv('AUDIO_BUCKET', 'youtube-audio'),  # Thêm cấu hình bucket audio
    },
    'processing': {
        'max_videos': int(os.getenv('MAX_VIDEOS', 50)),
    }
}

# MinIO client
minio_client = Minio(
    os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)

# Ensure buckets exist
buckets = [CONFIG['video']['frames_bucket'], CONFIG['audio']['audio_bucket']]
for bucket in buckets:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)