from fastapi import APIRouter, HTTPException
from realtime_sentiment.src.producer.controller import process_url
router = APIRouter()

@router.post("/social_media")
def process_media(url: str):
    try:
        process_url(url)
        return {"status": "Video processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))