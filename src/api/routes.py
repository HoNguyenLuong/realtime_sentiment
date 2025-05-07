from fastapi import APIRouter, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from src.producer.controller import process_url, process_multiple_urls
from src.consumer.spark_video import run as run_video_consumer
from src.utils.image_utils import get_sentiment_results

# Initialize router
router = APIRouter()

# Templates configuration
templates = Jinja2Templates(directory="templates")
templates.env.globals["get_flashed_messages"] = lambda: []

# Cache for sentiment results
sentiment_results = {
    "comment_sentiment": {"positive": 0, "negative": 0, "neutral": 0},
    "video_sentiment": []
}

def prepare_sentiment_data(result):
    """Prepare sentiment data for safe display."""
    if result is None:
        return {"positive": 0, "negative": 0, "neutral": 0, "status": "processing", "total": 0}

    result["positive"] = int(result.get("positive", 0))
    result["negative"] = int(result.get("negative", 0))
    result["neutral"] = int(result.get("neutral", 0))
    result["total"] = result["positive"] + result["negative"] + result["neutral"]

    return result

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Home page."""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "result": None,
        "youtube_url": "",
        "messages": []
    })

@router.post("/", response_class=HTMLResponse)
async def process_youtube(request: Request, youtube_url: str = Form(...)):
    """Process YouTube URL and display results."""
    messages = []

    if not youtube_url:
        messages.append({"category": "warning", "message": "Please provide a valid YouTube URL."})
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "Please provide a valid YouTube URL.",
            "youtube_url": "",
            "messages": messages
        })

    try:
        result = process_url(youtube_url)
        messages.append({"category": "success", "message": "URL processed successfully!"})

        processed_result = prepare_sentiment_data(result)
        sentiment_results["comment_sentiment"] = processed_result

        return templates.TemplateResponse("index.html", {
            "request": request,
            "result": processed_result,
            "youtube_url": youtube_url,
            "messages": messages
        })
    except Exception as e:
        messages.append({"category": "danger", "message": f"Error processing URL: {str(e)}"})
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": f"Error processing URL: {str(e)}",
            "youtube_url": youtube_url,
            "messages": messages
        })

@router.get("/api/get_results")
async def get_results():
    """API to fetch the latest comment sentiment results."""
    try:
        return sentiment_results["comment_sentiment"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/get_video_sentiments")
async def get_video_sentiments():
    """API to fetch video frame sentiment analysis results."""
    try:
        results = get_sentiment_results("video_frames")
        sentiment_results["video_sentiment"] = results
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/video_emotions", response_class=HTMLResponse)
async def video_emotions_page(request: Request):
    """Page to display video frame sentiment analysis."""
    try:
        if not sentiment_results["video_sentiment"]:
            results = get_sentiment_results("video_frames")
            sentiment_results["video_sentiment"] = results
        else:
            results = sentiment_results["video_sentiment"]

        return templates.TemplateResponse("video_emotions.html", {
            "request": request,
            "results": results,
            "messages": []
        })
    except Exception as e:
        return RedirectResponse(url="/", status_code=303)

@router.get("/api/status")
async def status():
    """API to check system status."""
    return {
        "status": "running",
        "comments_analyzed": sum(sentiment_results["comment_sentiment"].values()),
        "video_frames_analyzed": len(sentiment_results["video_sentiment"]),
    }

@router.post("/process")
def process_any_url(url: str):
    """Process a single URL."""
    try:
        process_url(url)
        return {"status": "Processing started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process_batch")
def process_batch_urls(urls: list[str]):
    """Process multiple URLs."""
    try:
        process_multiple_urls(urls)
        return {"status": f"Started processing {len(urls)} URLs"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))