import threading
from .ytb_handler import stream_youtube_video_and_extract, extract_audio_stream
from .tiktok_handler import download_and_stream_tiktok_video

def process_url(url):
    if "tiktok.com" in url:
        download_and_stream_tiktok_video(url)
    elif "youtube.com" in url:
        t1 = threading.Thread(target=stream_youtube_video_and_extract, args=(url,))
        t2 = threading.Thread(target=extract_audio_stream, args=(url,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
