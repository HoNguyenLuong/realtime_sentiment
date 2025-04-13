from fastapi import APIRouter, HTTPException
from realtime_sentiment.producer.controller import process_url, process_multiple_urls

router = APIRouter()

@router.post("/process")
def process_any_url(url: str):
    try:
        process_url(url)
        return {"status": "Processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process_batch")
def process_batch_urls(urls: list[str]):
    try:
        process_multiple_urls(urls)
        return {"status": f"Started processing {len(urls)} URLs"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
