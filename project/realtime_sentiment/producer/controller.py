from realtime_sentiment.producer.ytb_handler import process_ytb_urls, process_ytb_url
from realtime_sentiment.producer.tiktok_handler import process_tiktok_urls, process_tiktok_url

def process_url(url):
    if "tiktok.com" in url:
        process_tiktok_url(url)
    elif "youtube.com" in url or "youtu.be" in url:
        process_ytb_url(url)
    else:
        print(f"⚠️ Unsupported URL: {url}")

def process_multiple_urls(youtube_playlist=None, tiktok_urls=None):
    if youtube_playlist:
        process_ytb_urls(youtube_playlist)

    if tiktok_urls:
        process_tiktok_urls(tiktok_urls)
