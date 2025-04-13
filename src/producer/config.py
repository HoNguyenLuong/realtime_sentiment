import os
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    'kafka': {
        'bootstrap_servers': os.getenv('KAFKA_SERVERS', 'localhost:9092'),
    },
    'video': {
        'width': int(os.getenv('VIDEO_WIDTH', 640)),
        'height': int(os.getenv('VIDEO_HEIGHT', 360)),
        'frame_format': os.getenv('FRAME_FORMAT', '.jpg'),
    },
    'audio': {
        'chunk_size': int(os.getenv('AUDIO_CHUNK_SIZE', 4096)),
    },
    'processing': {
        'max_videos': int(os.getenv('MAX_VIDEOS', 50)),
    }
}
