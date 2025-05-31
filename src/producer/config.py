import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

CONFIG = {
    'kafka': {
        'bootstrap_servers': os.getenv('KAFKA_SERVERS', 'localhost:9092'),
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
    'comments': {
        'comments_bucket': os.getenv('COMMENTS_BUCKET', 'youtube-comments'),  # Thêm cấu hình bucket comments
        'max_comments': int(os.getenv('MAX_COMMENTS', 1000)),  # Số lượng comments tối đa muốn lấy
        'sort': os.getenv('COMMENTS_SORT', 'new'),  # Sắp xếp comments (new/top/best)
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

MINIO_BUCKET = "sentiment-results"
FUSION_OBJECT_NAME = "fusion-results.json"
# Ensure buckets exist
buckets = [
    CONFIG['video']['frames_bucket'],
    CONFIG['audio']['audio_bucket'],
    CONFIG['comments']['comments_bucket'], # Thêm bucket comments
    MINIO_BUCKET
]
for bucket in buckets:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)