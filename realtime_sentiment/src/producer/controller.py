from src.producer.ytb_handler import process_ytb_urls, process_ytb_url, get_video_id
from src.producer.tiktok_handler import process_tiktok_urls, process_tiktok_url
import logging

logger = logging.getLogger(__name__)


def process_url(url):
    """
    Xử lý một URL đơn lẻ dựa trên loại nội dung (YouTube hoặc TikTok)
    """
    logger.info(f"Processing URL: {url}")

    if "tiktok.com" in url:
        process_tiktok_url(url)
    elif "youtube.com" in url or "youtu.be" in url:
        # Kiểm tra xem URL có phải là playlist/channel không
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


def process_multiple_urls(youtube_urls=None, tiktok_urls=None):
    """
    Xử lý nhiều URLs YouTube và TikTok
    """
    if youtube_urls:
        if isinstance(youtube_urls, list):
            for url in youtube_urls:
                if "playlist?list=" in url or "/channel/" in url or "/c/" in url or "/user/" in url:
                    logger.info(f"Processing YouTube playlist/channel: {url}")
                    process_ytb_urls(url)
                else:
                    video_id = get_video_id(url)
                    logger.info(f"Processing single YouTube video: {url}, ID: {video_id}")
                    process_ytb_url(url, video_id=video_id)
        else:
            # Nếu chỉ có một URL YouTube
            url = youtube_urls
            if "playlist?list=" in url or "/channel/" in url or "/c/" in url or "/user/" in url:
                logger.info(f"Processing YouTube playlist/channel: {url}")
                process_ytb_urls(url)
            else:
                video_id = get_video_id(url)
                logger.info(f"Processing single YouTube video: {url}, ID: {video_id}")
                process_ytb_url(url, video_id=video_id)

    if tiktok_urls:
        if isinstance(tiktok_urls, list):
            process_tiktok_urls(tiktok_urls)
        else:
            # Nếu chỉ có một URL TikTok
            process_tiktok_url(tiktok_urls)