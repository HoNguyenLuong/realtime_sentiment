import subprocess
import threading
import base64
import cv2
import numpy as np
import logging
import json
import hashlib
import os
from urllib.parse import urlparse, parse_qs
from kafka import KafkaProducer
from dotenv import load_dotenv
from src.producer.kafka_sender import send_frame, send_audio_file
from src.producer.config import CONFIG

load_dotenv()
logger = logging.getLogger(__name__)

PROCESSED_FILE = "processed_ytb_ids.txt"
processed_ids = set()

if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, 'r') as f:
        processed_ids = set(line.strip() for line in f)


def get_video_id(url):
    try:
        # Parse URL
        parsed_url = urlparse(url)
        query = parse_qs(parsed_url.query)
        # Nếu là URL có tham số `v`
        if 'v' in query:
            return query['v'][0]
        # Nếu là dạng youtu.be/<id>
        if parsed_url.netloc == 'youtu.be':
            return parsed_url.path.lstrip('/')
    except Exception as e:
        logger.error(f"Failed to extract video ID from URL: {url}, error: {e}")
    # Fallback: hash URL nếu không parse được
    return hashlib.md5(url.encode()).hexdigest()


def stream_youtube_video_and_extract(url, video_id):
    width, height = CONFIG['video']['width'], CONFIG['video']['height']

    try:
        process = subprocess.Popen(
            ['yt-dlp', '-f', 'best', '-o', '-', url],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        ffmpeg_process = subprocess.Popen(
            ['ffmpeg',
             '-i', 'pipe:0',
             '-f', 'rawvideo',
             '-pix_fmt', 'bgr24',
             '-vf', f'scale={width}:{height}',
             '-vsync', '0',
             'pipe:1'],
            stdin=process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        process.stdout.close()
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

        logger.info(f"[YouTube] Sent {frame_id} frames for video {video_id}")

    except Exception as e:
        logger.error(f"Error extracting YouTube frames: {e}")


def extract_audio_stream(url, video_id):
    chunk_duration = 10

    try:
        temp_audio_file = f"/tmp/{video_id}_audio.wav"
        temp_downloaded = f"/tmp/{video_id}_downloaded"

        subprocess.run(
            ['yt-dlp', '-f', 'bestaudio', '-o', temp_downloaded, url],
            check=True
        )

        subprocess.run([
            'ffmpeg', '-i', temp_downloaded, '-f', 'wav', '-acodec', 'pcm_s16le',
            '-ac', '1', '-ar', '16000', temp_audio_file
        ], check=True)

        if os.path.exists(temp_downloaded):
            os.remove(temp_downloaded)

        chunk_id = 0
        total_duration = float(subprocess.check_output([
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1', temp_audio_file
        ], text=True).strip())

        for start_time in range(0, int(total_duration), chunk_duration):
            output_chunk = f"/tmp/{video_id}_chunk_{chunk_id}.wav"
            subprocess.run([
                'ffmpeg', '-v', 'error', '-i', temp_audio_file,
                '-ss', str(start_time), '-t', str(chunk_duration),
                '-c', 'copy', output_chunk
            ], check=True)

            with open(output_chunk, 'rb') as f:
                audio_data = f.read()

            if len(audio_data) > 100:
                send_audio_file(video_id, audio_data, f"chunk_{chunk_id}.wav")
                chunk_id += 1

            os.remove(output_chunk)

        os.remove(temp_audio_file)
        logger.info(f"[YouTube] Sent {chunk_id} audio chunks for video {video_id}")
    except Exception as e:
        logger.error(f"Error extracting YouTube audio: {e}")
        logger.exception(e)


# Hàm mới cần thêm vào
def process_ytb_url(url, video_id):
    """
    Xử lý một video YouTube đơn lẻ
    """
    if video_id in processed_ids:
        logger.info(f"Skipping already processed video: {video_id}")
        return

    try:
        # Chạy song song việc trích xuất video và audio
        t1 = threading.Thread(target=stream_youtube_video_and_extract, args=(url, video_id))
        t2 = threading.Thread(target=extract_audio_stream, args=(url, video_id))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Đánh dấu video đã được xử lý
        with open(PROCESSED_FILE, 'a') as f:
            f.write(video_id + "\n")
        processed_ids.add(video_id)

        logger.info(f"Successfully processed YouTube video: {video_id}")
    except Exception as e:
        logger.error(f"Error processing YouTube video {video_id}: {e}")
        logger.exception(e)


def collect_youtube_urls_from_playlist(list_or_channel_url):
    try:
        cmd = ['yt-dlp', '--flat-playlist', '--get-id', '--get-title', list_or_channel_url]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, text=True)
        lines = result.stdout.strip().splitlines()

        # Extract IDs and titles (yt-dlp outputs title and ID alternating lines when using both flags)
        videos = []
        for i in range(0, len(lines), 2):
            if i + 1 < len(lines):  # Make sure we have both title and ID
                title = lines[i]
                video_id = lines[i + 1]
                videos.append({
                    "id": video_id,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "title": title
                })

        logger.info(f"Collected {len(videos)} YouTube videos from {list_or_channel_url}")
        return videos
    except Exception as e:
        logger.error(f"Error collecting YouTube URLs: {e}")
        return []


def process_ytb_urls(list_or_channel_url):
    # Lấy thông tin chi tiết về từng video trong playlist/channel
    videos = collect_youtube_urls_from_playlist(list_or_channel_url)

    if not videos:
        logger.warning(f"No videos found in: {list_or_channel_url}")
        return

    logger.info(f"Processing {len(videos)} videos from playlist/channel")

    # Đếm số lượng video đã được xử lý và bỏ qua
    processed_count = 0
    skipped_count = 0

    for video in videos:
        video_id = video["id"]
        url = video["url"]
        title = video["title"]

        # Kiểm tra xem video đã được xử lý chưa
        if video_id in processed_ids:
            logger.info(f"Skipping already processed video: {video_id} - {title}")
            skipped_count += 1
            continue

        logger.info(f"Processing video: {video_id} - {title}")

        try:
            # Xử lý video hiện tại
            t1 = threading.Thread(target=stream_youtube_video_and_extract, args=(url, video_id))
            t2 = threading.Thread(target=extract_audio_stream, args=(url, video_id))
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            # Đánh dấu video đã được xử lý
            with open(PROCESSED_FILE, 'a') as f:
                f.write(video_id + "\n")
            processed_ids.add(video_id)
            processed_count += 1

        except Exception as e:
            logger.error(f"Error processing video {video_id} - {title}: {e}")

    logger.info(f"Finished processing playlist/channel. Processed: {processed_count}, Skipped: {skipped_count}")


def process_url(url):
    if "youtube.com" in url or "youtu.be" in url:
        # Kiểm tra xem URL có phải là playlist hoặc channel không
        if "playlist?list=" in url or "/channel/" in url or "/c/" in url or "/user/" in url:
            logger.info(f"Detected YouTube playlist/channel: {url}")
            process_ytb_urls(url)
        else:
            # Xử lý video đơn lẻ
            video_id = get_video_id(url)
            logger.info(f"Processing single YouTube video: {url}, ID: {video_id}")
            process_ytb_url(url, video_id=video_id)
    else:
        logger.warning(f"⚠️ Unsupported URL: {url}")


def process_multiple_urls(youtube_urls=None):
    if youtube_urls is None:
        logger.warning("No YouTube URLs provided")
        return

    if isinstance(youtube_urls, list):
        for url in youtube_urls:
            # Kiểm tra xem URL có phải là playlist/channel không
            if "playlist?list=" in url or "/channel/" in url or "/c/" in url or "/user/" in url:
                logger.info(f"Processing YouTube playlist/channel: {url}")
                process_ytb_urls(url)
            else:
                # Xử lý như video đơn lẻ
                video_id = get_video_id(url)
                logger.info(f"Processing single YouTube video: {url}, ID: {video_id}")
                process_ytb_url(url, video_id=video_id)
    else:
        url = youtube_urls
        # Kiểm tra xem URL có phải là playlist/channel không
        if "playlist?list=" in url or "/channel/" in url or "/c/" in url or "/user/" in url:
            logger.info(f"Processing YouTube playlist/channel: {url}")
            process_ytb_urls(url)
        else:
            # Xử lý như video đơn lẻ
            video_id = get_video_id(url)
            logger.info(f"Processing single YouTube video: {url}, ID: {video_id}")
            process_ytb_url(url, video_id=video_id)