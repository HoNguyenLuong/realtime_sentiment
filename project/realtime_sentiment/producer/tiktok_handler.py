import base64
import ffmpeg
import logging
import numpy as np
import requests
import tempfile
import cv2
import hashlib
import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

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
    }
}

producer = KafkaProducer(
    bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(4, 0)
)

def get_video_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def send_frame(video_id, frame_id, frame_bytes, source='tiktok'):
    try:
        if isinstance(frame_bytes, np.ndarray):
            frame_bytes = frame_bytes.tobytes()
        producer.send('video_frames', value={
            'video_id': video_id,
            'frame_id': frame_id,
            'source': source,
            'data': base64.b64encode(frame_bytes).decode('utf-8')
        })
    except Exception as e:
        logger.error(f"Error sending frame {frame_id}: {e}")

def send_audio(video_id, chunk_id, chunk, source='tiktok'):
    try:
        if isinstance(chunk, np.ndarray):
            chunk = chunk.tobytes()
        producer.send('audio_stream', value={
            'video_id': video_id,
            'chunk_id': chunk_id,
            'source': source,
            'data': base64.b64encode(chunk).decode('utf-8')
        })
    except Exception as e:
        logger.error(f"Error sending audio chunk {chunk_id}: {e}")

def download_and_stream_tiktok_video(url):
    video_id = get_video_id(url)
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=True) as temp_video:
            response = requests.get(url, stream=True)
            for chunk in response.iter_content(chunk_size=8192):
                temp_video.write(chunk)
            temp_video.flush()

            # Audio
            audio_process = ffmpeg.input(temp_video.name).output('pipe:', format='mp3', acodec='libmp3lame', vn=None).run_async(pipe_stdout=True)
            chunk_id = 0
            while True:
                audio_chunk = audio_process.stdout.read(CONFIG['audio']['chunk_size'])
                if not audio_chunk:
                    break
                send_audio(video_id, chunk_id, audio_chunk)
                chunk_id += 1

            # Video
            video_process = ffmpeg.input(temp_video.name).output('pipe:', format='rawvideo', pix_fmt='bgr24',
                s=f"{CONFIG['video']['width']}x{CONFIG['video']['height']}").run_async(pipe_stdout=True)
            frame_id = 0
            while True:
                in_bytes = video_process.stdout.read(CONFIG['video']['width'] * CONFIG['video']['height'] * 3)
                if not in_bytes:
                    break
                frame = np.frombuffer(in_bytes, np.uint8).reshape([CONFIG['video']['height'], CONFIG['video']['width'], 3])
                _, buffer = cv2.imencode(CONFIG['video']['frame_format'], frame)
                send_frame(video_id, frame_id, buffer.tobytes())
                frame_id += 1

            logger.info(f"Sent {frame_id} frames & {chunk_id} audio chunks from TikTok {url}")
    except Exception as e:
        logger.error(f"Error processing TikTok: {e}")
