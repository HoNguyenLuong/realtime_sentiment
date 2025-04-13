import subprocess
import base64
import cv2
import numpy as np
import logging
import json
import hashlib
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

CONFIG = {
    'kafka': {
        'bootstrap_servers': os.getenv('KAFKA_SERVERS', 'localhost:9092'),
    }
}

producer = KafkaProducer(
    bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(4, 0)
)

def get_video_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def send_frame(video_id, frame_id, frame_bytes, source='youtube'):
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

def send_audio(video_id, chunk_id, chunk, source='youtube'):
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

def stream_youtube_video_and_extract(url):
    video_id = get_video_id(url)
    try:
        process = subprocess.Popen(['yt-dlp', '-f', 'best', '-o', '-', url], stdout=subprocess.PIPE)
        ffmpeg_process = subprocess.Popen(
            ['ffmpeg', '-i', 'pipe:0', '-f', 'rawvideo', '-pix_fmt', 'bgr24', '-vf', 'scale=640:360', 'pipe:1'],
            stdin=process.stdout, stdout=subprocess.PIPE
        )
        process.stdout.close()
        width, height = 640, 360
        frame_id = 0
        buffer_size = width * height * 3

        while True:
            in_bytes = ffmpeg_process.stdout.read(buffer_size)
            if not in_bytes:
                break
            frame = np.frombuffer(in_bytes, np.uint8).reshape([height, width, 3])
            _, buffer = cv2.imencode('.jpg', frame)
            send_frame(video_id, frame_id, buffer.tobytes())
            frame_id += 1

        logger.info(f"Sent {frame_id} frames from YouTube {url}")
    except Exception as e:
        logger.error(f"Error extracting video: {e}")

def extract_audio_stream(url):
    video_id = get_video_id(url)
    try:
        process = subprocess.Popen(['yt-dlp', '-f', 'bestaudio', '-o', '-', url], stdout=subprocess.PIPE)
        ffmpeg_process = subprocess.Popen(
            ['ffmpeg', '-i', 'pipe:0', '-f', 's16le', '-acodec', 'pcm_s16le', '-ac', '1', '-ar', '16000', 'pipe:1'],
            stdin=process.stdout, stdout=subprocess.PIPE
        )
        process.stdout.close()
        chunk_size = 1024
        chunk_id = 0

        while True:
            chunk = ffmpeg_process.stdout.read(chunk_size)
            if not chunk:
                break
            send_audio(video_id, chunk_id, chunk)
            chunk_id += 1

        logger.info(f"Sent {chunk_id} audio chunks from YouTube {url}")
    except Exception as e:
        logger.error(f"Error extracting audio: {e}")
