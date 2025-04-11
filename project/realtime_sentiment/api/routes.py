from fastapi import APIRouter, HTTPException
from realtime_sentiment.producer.ytb_handler import stream_youtube_video_and_extract
from realtime_sentiment.producer.tiktok_handler import download_and_stream_tiktok_video

router = APIRouter()

@router.post("/youtube")
def process_youtube(url: str):
    try:
        stream_youtube_video_and_extract(url)
        return {"status": "YouTube processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tiktok")
def process_tiktok(url: str):
    try:
        download_and_stream_tiktok_video(url)
        return {"status": "TikTok processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))