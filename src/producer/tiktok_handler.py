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
from src.producer.kafka_sender import send_frame, send_audio_file
from src.producer.config import CONFIG

load_dotenv()
logger = logging.getLogger(__name__)

PROCESSED_FILE = "processed_tiktok_ids.txt"
processed_ids = set()

if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, 'r') as f:
        processed_ids = set(line.strip() for line in f)

def get_video_id(url):
    return hashlib.md5(url.encode()).hexdigest()

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
                send_audio_file(video_id, chunk_id, audio_chunk)
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

def process_tiktok_url(url):
    video_id = get_video_id(url)

    if video_id in processed_ids:
        logger.info(f"Skipping already processed video: {video_id}")
        return

    if "tiktok.com" in url:
        download_and_stream_tiktok_video(url)

    with open(PROCESSED_FILE, 'a') as f:
        f.write(video_id + "\n")
    processed_ids.add(video_id)

def process_tiktok_urls(url_list):
    for url in url_list:
        process_tiktok_url(url)
