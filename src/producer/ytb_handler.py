import json
import subprocess
import threading
import time
import cv2
import numpy as np
import logging
import hashlib
import os
from urllib.parse import urlparse, parse_qs
from kafka import KafkaProducer
from dotenv import load_dotenv
from src.producer.kafka_sender import send_frame, send_audio_file, send_comments
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
             '-vf', f'fps=2,scale={width}:{height}',
             '-vsync', '0',
             'pipe:1'],
            stdin=process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE  # Capture stderr for debugging
        )
        process.stdout.close()
        frame_id = 0

        # Đọc đúng kích thước buffer: width * height * 3 (3 channels BGR)
        buffer_size = width * height * 3

        while True:
            # Đọc đúng buffer_size bytes
            in_bytes = ffmpeg_process.stdout.read(buffer_size)
            if not in_bytes:
                break

            # Kiểm tra kích thước thực tế của dữ liệu
            actual_size = len(in_bytes)
            # Loại bỏ dòng log này để tránh ghi log quá nhiều
            # logger.info(f"Read {actual_size} bytes for frame {frame_id}")

            if actual_size != buffer_size:
                # Nếu kích thước không đúng, tính toán kích thước thực tế
                if actual_size % 3 == 0:  # Đảm bảo chia hết cho 3 (BGR)
                    actual_pixels = actual_size // 3
                    # Tìm kích thước phù hợp (giả sử tỷ lệ 16:9)
                    actual_height = int(np.sqrt(actual_pixels * 9 / 16))
                    actual_width = actual_pixels // actual_height

                    logger.warning(
                        f"Frame size mismatch! Expected {width}x{height} but got data for {actual_width}x{actual_height}")

                    # Reshape với kích thước thực tế
                    frame = np.frombuffer(in_bytes, np.uint8).reshape([actual_height, actual_width, 3])
                else:
                    logger.error(f"Received incomplete frame data: {actual_size} bytes")
                    continue
            else:
                # Kích thước đúng như mong đợi
                frame = np.frombuffer(in_bytes, np.uint8).reshape([height, width, 3])

            _, buffer = cv2.imencode('.jpg', frame)
            send_frame(video_id, frame_id, buffer.tobytes())
            frame_id += 1

        # Ghi log một lần ở cuối quá trình xử lý
        logger.info(f"[YouTube] Sent {frame_id} frames for video {video_id}")

        # Kiểm tra lỗi từ ffmpeg
        if ffmpeg_process.poll() is not None and ffmpeg_process.returncode != 0:
            ffmpeg_error = ffmpeg_process.stderr.read().decode('utf-8', errors='ignore')
            logger.error(f"FFmpeg error: {ffmpeg_error}")
    except Exception as e:
        logger.error(f"Error extracting YouTube frames: {e}")
        import traceback
        logger.error(traceback.format_exc())

# def stream_youtube_video_and_extract(url, video_id):
#     width, height = CONFIG['video']['width'], CONFIG['video']['height']
#
#     try:
#         process = subprocess.Popen(
#             ['yt-dlp', '-f', 'best', '-o', '-', url],
#             stdout=subprocess.PIPE,
#             stderr=subprocess.DEVNULL
#         )
#
#         # Thêm tham số -skip_frame và fps cụ thể
#         ffmpeg_process = subprocess.Popen(
#             ['ffmpeg',
#              '-i', 'pipe:0',
#              '-f', 'rawvideo',
#              '-pix_fmt', 'bgr24',
#              '-vf', f'fps=2,scale={width}:{height}',  # Sử dụng fps trong -vf
#              '-vsync', '0',
#              'pipe:1'],
#             stdin=process.stdout,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.DEVNULL
#         )
#
#         process.stdout.close()
#         frame_id = 0
#         buffer_size = width * height
#         while True:
#             in_bytes = ffmpeg_process.stdout.read(buffer_size)
#             if not in_bytes:
#                 break
#             frame = np.frombuffer(in_bytes, np.uint8).reshape([height, width, 3])
#             _, buffer = cv2.imencode('.jpg', frame)
#             send_frame(video_id, frame_id, buffer.tobytes())
#             frame_id += 1
#
#         logger.info(f"[YouTube] Sent {frame_id} frames for video {video_id}")
#
#     except Exception as e:
#         logger.error(f"Error extracting YouTube frames: {e}")


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
                send_audio_file(video_id, chunk_id, audio_data, f"chunk_{chunk_id}.wav")
                chunk_id += 1

            os.remove(output_chunk)

        os.remove(temp_audio_file)
        logger.info(f"[YouTube] Sent {chunk_id} audio chunks for video {video_id}")
    except Exception as e:
        logger.error(f"Error extracting YouTube audio: {e}")
        logger.exception(e)

# Hàm lấy audio dưới dạng stream mỗi 10 giây.
def extract_livestream_audio(url, stream_id):
    try:
        # Sử dụng yt-dlp với tham số streaming
        ytdlp_process = subprocess.Popen(
            ['yt-dlp', '-f', 'bestaudio', '--no-part', '--no-continue', '-o', '-', url],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        # Pipe trực tiếp sang ffmpeg để xử lý audio theo thời gian thực
        ffmpeg_process = subprocess.Popen(
            ['ffmpeg',
             '-i', 'pipe:0',
             '-f', 'segment',
             '-segment_time', '10',
             '-ac', '1',
             '-ar', '16000',
             '-acodec', 'pcm_s16le',
             '-f', 'wav',
             '/tmp/%d_chunk.wav'],
            stdin=ytdlp_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        ytdlp_process.stdout.close()

        chunk_id = 0
        while True:
            # Kiểm tra file chunk mới được tạo ra
            chunk_file = f"/tmp/{chunk_id}_chunk.wav"
            if os.path.exists(chunk_file):
                # Đợi file hoàn thành ghi
                time.sleep(0.5)

                with open(chunk_file, 'rb') as f:
                    audio_data = f.read()

                if len(audio_data) > 100:
                    send_audio_file(stream_id, chunk_id, audio_data, f"chunk_{chunk_id}.wav")

                os.remove(chunk_file)
                chunk_id += 1

            # Kiểm tra xem tiến trình ffmpeg còn hoạt động không
            if ffmpeg_process.poll() is not None:
                break

            time.sleep(0.5)

    except Exception as e:
        logger.error(f"Error extracting livestream audio: {e}")
        logger.exception(e)

def extract_youtube_comments(url, video_id):
    """
    Trích xuất comment từ một video YouTube

    Args:
        url (str): URL của video YouTube
        video_id (str): ID của video YouTube
    """
    try:
        logger.info(f"Extracting comments for YouTube video: {video_id}")

        # Lấy cấu hình từ CONFIG
        max_comments = CONFIG['comments']['max_comments']
        sort_order = CONFIG['comments']['sort']

        # Sử dụng yt-dlp để lấy comments
        cmd = [
            'yt-dlp',
            '--write-comments',
            '--no-download',
            '--extractor-args', f'youtube:comment_sort={sort_order};max_comments={max_comments}',
            '-O', 'comments',
            '--dump-json',
            url
        ]

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)

        # Phân tích kết quả JSON
        comments_data = []
        for line in result.stdout.strip().split('\n'):
            if line:  # Đảm bảo line không trống
                try:
                    comment = json.loads(line)
                    comments_data.append(comment)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse comment JSON: {line[:100]}...")

        if not comments_data:
            logger.warning(f"No comments extracted for video {video_id}")
            return

        logger.info(f"Extracted {len(comments_data)} comments for video {video_id}")

        # Gửi comments sử dụng hàm send_comments
        send_comments(video_id, comments_data)

        logger.info(f"Successfully processed comments for YouTube video: {video_id}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}: {e.stderr}")
    except Exception as e:
        logger.error(f"Error extracting YouTube comments: {e}")
        logger.exception(e)

def extract_livestream_comments(url, video_id):
    """
    Trích xuất comments từ một YouTube livestream

    Args:
        url (str): URL của YouTube livestream
        video_id (str): ID của YouTube livestream
    """
    try:
        logger.info(f"Extracting comments for YouTube livestream: {video_id}")

        # Lấy cấu hình từ CONFIG
        max_comments = CONFIG['comments']['max_comments']

        # Đối với livestream, chúng ta sẽ sử dụng YouTube API hoặc yt-dlp với các tham số cụ thể cho livestream
        # Ví dụ: sử dụng tham số --get-comments của yt-dlp với tùy chọn cho livestream
        cmd = [
            'yt-dlp',
            '--write-comments',
            '--no-download',
            '--extractor-args',
            f'youtube:comment_sort=new;max_comments={max_comments};live_chat=1',  # Chỉ định lấy live chat
            '-O', 'comments',
            '--dump-json',
            url
        ]

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)

        # Phân tích kết quả JSON
        comments_data = []
        for line in result.stdout.strip().split('\n'):
            if line:  # Đảm bảo line không trống
                try:
                    comment = json.loads(line)
                    comments_data.append(comment)
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse livestream comment JSON: {line[:100]}...")

        if not comments_data:
            logger.warning(f"No comments extracted for livestream {video_id}")
            return

        logger.info(f"Extracted {len(comments_data)} comments for livestream {video_id}")

        # Gửi comments sử dụng hàm send_comments
        send_comments(video_id, comments_data)

        logger.info(f"Successfully processed comments for YouTube livestream: {video_id}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with return code {e.returncode}: {e.stderr}")
    except Exception as e:
        logger.error(f"Error extracting YouTube livestream comments: {e}")
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

# Hàm check livestream
def check_if_livestream(url):
    """
    Kiểm tra xem URL có phải là livestream hay không
    """
    try:
        cmd = ['yt-dlp', '--print', '%(is_live)s', url]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, check=True, text=True)
        is_live = result.stdout.strip().lower()

        # yt-dlp trả về 'True' nếu là livestream
        return is_live == 'true'
    except Exception as e:
        logger.error(f"Error checking if URL is livestream: {e}")
        return False

def process_ytb_url(url, video_id):
    """
    Xử lý một video YouTube đơn lẻ, phân biệt giữa video thường và livestream cho phần audio
    Nay thêm chức năng lấy comments cho cả video thường và livestream
    """
    if video_id in processed_ids:
        logger.info(f"Skipping already processed video: {video_id}")
        return

    try:
        # Kiểm tra xem đây có phải là livestream không
        is_livestream = check_if_livestream(url)

        # Luôn trích xuất video, bất kể là livestream hay video thường
        t1 = threading.Thread(target=stream_youtube_video_and_extract, args=(url, video_id))
        t1.start()

        # Chọn phương thức trích xuất audio phù hợp dựa trên loại nội dung
        if is_livestream:
            logger.info(f"Detected YouTube livestream: {video_id}, using livestream audio extraction")
            t2 = threading.Thread(target=extract_livestream_audio, args=(url, video_id))

            # Thêm thread để lấy comments từ livestream
            logger.info(f"Extracting comments for livestream: {video_id}")
            t3 = threading.Thread(target=extract_livestream_comments, args=(url, video_id))
        else:
            logger.info(f"Detected regular YouTube video: {video_id}, using standard audio extraction")
            t2 = threading.Thread(target=extract_audio_stream, args=(url, video_id))

            # Thêm thread để lấy comments từ video thường
            logger.info(f"Extracting comments for video: {video_id}")
            t3 = threading.Thread(target=extract_youtube_comments, args=(url, video_id))

        t2.start()
        t3.start()

        # Đợi tất cả các thread hoàn thành
        t1.join()
        t2.join()
        t3.join()

        # Đánh dấu video đã được xử lý
        with open(PROCESSED_FILE, 'a') as f:
            f.write(video_id + "\n")
        processed_ids.add(video_id)

        logger.info(f"Successfully processed YouTube video/livestream: {video_id}")

    except Exception as e:
        logger.error(f"Error processing YouTube video {video_id}: {e}")
        logger.exception(e)


def process_ytb_urls(list_or_channel_url):
    """
    Xử lý nhiều video từ playlist hoặc channel
    """
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
            # Sử dụng hàm process_ytb_url để xử lý từng video (áp dụng logic phân biệt livestream)
            process_ytb_url(url, video_id)
            processed_count += 1
        except Exception as e:
            logger.error(f"Error processing video {video_id} - {title}: {e}")

    logger.info(f"Finished processing playlist/channel. Processed: {processed_count}, Skipped: {skipped_count}")


def process_url(url):
    """
    Xử lý một URL (có thể là video đơn, playlist, hoặc channel)
    """
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
    """
    Xử lý nhiều URL YouTube
    """
    if youtube_urls is None:
        logger.warning("No YouTube URLs provided")
        return

    if isinstance(youtube_urls, list):
        for url in youtube_urls:
            process_url(url)
    else:
        # Xử lý trường hợp chỉ có một URL
        process_url(youtube_urls)