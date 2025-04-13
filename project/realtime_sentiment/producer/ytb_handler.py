import subprocess
import threading
import base64
import cv2
import numpy as np
import logging
import json
import hashlib
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from realtime_sentiment.producer.kafka_sender import send_frame, send_audio
from realtime_sentiment.producer.config import CONFIG


load_dotenv()
logger = logging.getLogger(__name__)

PROCESSED_FILE = "processed_ytb_ids.txt"
processed_ids = set()

if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, 'r') as f:
        processed_ids = set(line.strip() for line in f)

def get_video_id(url):
    return hashlib.md5(url.encode()).hexdigest()

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
        
def collect_youtube_urls_from_playlist(list_or_channel_url):
    try:
        cmd = ['yt-dlp', '--flat-playlist', '--get-id', list_or_channel_url]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, text=True)
        video_ids = result.stdout.strip().splitlines()
        urls = [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids]
        logger.info(f"Collected {len(urls)} YouTube videos from {list_or_channel_url}")
        return urls
    except Exception as e:
        logger.error(f"Error collecting YouTube URLs: {e}")
        return []

def process_ytb_url(url):
    video_id = get_video_id(url)

    if video_id in processed_ids:
        logger.info(f"Skipping already processed video: {video_id}")
        return

    if "youtube.com" in url or "youtu.be" in url:
        # YouTube: Process audio + video in parallel with threading
        try:
            t1 = threading.Thread(target=stream_youtube_video_and_extract, args=(url,))
            t2 = threading.Thread(target=extract_audio_stream, args=(url,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except Exception as e:
            logger.error(f"Error processing YouTube video: {e}")
    else:
        logger.warning(f"Unsupported URL: {url}")

    with open(PROCESSED_FILE, 'a') as f:
        f.write(video_id + "\n")
    processed_ids.add(video_id)

def process_ytb_urls(list_or_channel_url):
    url_list = collect_youtube_urls_from_playlist(list_or_channel_url)
    for url in url_list:
        process_ytb_url(url)