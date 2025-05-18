import subprocess
import json
import threading
import logging
import os

# Thiết lập logger đơn giản
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = {
    'comments': {
        'max_comments': 100,
        'sort': 'top'  # hoặc 'new'
    }
}

def send_comments(video_id, comments):
    filename = f"comments_{video_id}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        for comment in comments:
            json_line = json.dumps(comment, ensure_ascii=False)
            f.write(json_line + "\n")
    logger.info(f"[TEST] Saved {len(comments)} comments (one JSON per line) to {filename}")



def extract_youtube_comments(url, video_id):
    try:
        thread_name = threading.current_thread().name
        logger.info(f"[{thread_name}] Extracting comments for YouTube video: {video_id}")

        max_comments = CONFIG['comments']['max_comments']
        sort_order = CONFIG['comments']['sort']

        # Chỉ định tên file info.json để dễ kiểm soát
        info_filename = f"{video_id}.info.json"

        cmd = [
            'yt-dlp',
            '--write-info-json',
            '--write-comments',
            '--no-download',
            '--skip-download',
            '--output', video_id,  # Đặt tên file đầu ra là video_id
            '--extractor-args', f'youtube:comment_sort={sort_order};max_comments={max_comments}',
            url
        ]

        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            logger.error(f"[{thread_name}] yt-dlp command failed: {result.stderr.strip()}")
            return

        # Đọc file info.json
        if not os.path.exists(info_filename):
            logger.error(f"[{thread_name}] Cannot find downloaded info JSON file: {info_filename}")
            return

        with open(info_filename, 'r', encoding='utf-8') as f:
            video_info = json.load(f)

        raw_comments = video_info.get('comments', [])

        comments = []
        for comment in raw_comments:
            comment_obj = {
                "id": comment.get("id", "unknown"),
                "text": comment.get("text", ""),
                "author": comment.get("author", "unknown"),
                "timestamp": int(comment.get("timestamp", 0)) if comment.get("timestamp") else 0,
                "parent_id": comment.get("parent", "root"),
                "likes": int(comment.get("like_count", 0))
            }
            comments.append(comment_obj)

            if "replies" in comment:
                for reply in comment.get("replies", []):
                    reply_obj = {
                        "id": reply.get("id", "unknown"),
                        "text": reply.get("text", ""),
                        "author": reply.get("author", "unknown"),
                        "timestamp": int(reply.get("timestamp", 0)) if reply.get("timestamp") else 0,
                        "parent_id": comment.get("id", "unknown"),
                        "likes": int(reply.get("like_count", 0))
                    }
                    comments.append(reply_obj)

        if not comments:
            logger.warning(f"[{thread_name}] No comments extracted for video {video_id}")
            return

        logger.info(f"[{thread_name}] Extracted {len(comments)} comments for video {video_id}")
        send_comments(video_id, comments)

        # Xóa các file không cần thiết (chỉ giữ lại .info.json và file kết quả)
        for file in os.listdir():
            if file.startswith(video_id) and not (file.endswith('.info.json') or file.endswith('.json')):
                try:
                    os.remove(file)
                except Exception as e:
                    logger.warning(f"Could not remove file {file}: {e}")

    except Exception as e:
        logger.error(f"[{thread_name}] Unexpected error: {str(e)}")
        logger.exception(e)


# ---------- TEST ---------------
if __name__ == '__main__':
    youtube_url = 'https://www.youtube.com/watch?v=xkgq5lCr00o'  # <-- Thay bằng video thật nếu muốn
    video_id = 'xkgq5lCr00o'
    extract_youtube_comments(youtube_url, video_id)
