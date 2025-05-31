# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# import threading
# from contextlib import asynccontextmanager
# from src.api.routes import router as api_router
# from src.producer.controller import process_url
# # Import thêm consumer xử lý video
# from src.consumer.spark_video import run as run_video_consumer
# from src.consumer.spark_audio import run as run_audio_consumer
# from src.consumer.spark_comment import run as run_comment_consumer
# import time
#
# # Phần lifespan handler
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     # Tạo và khởi động thread cho consumer xử lý video
#     # video_consumer_thread = threading.Thread(target=run_audio_consumer, daemon=True)
#     # video_consumer_thread.start()
#     # print("Video consumer started in background")
#
#     video_consumer_thread = threading.Thread(target=run_comment_consumer, daemon=True)
#     video_consumer_thread.start()
#     print("Video consumer started in background")
#     #
#     # video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
#     # video_consumer_thread.start()
#     # print("Video consumer started in background")
#     # # Cho consumer thời gian để khởi động hoàn toàn
#     time.sleep(5)
#
#     # ✅ Chạy producer test với 1 link cụ thể
#     test_url = "https://youtu.be/dQ27hrKxSQY?si=_c6N--5cZWZqbiwI"
#     if test_url:  # Chỉ chạy producer test nếu có URL
#         threading.Thread(target=process_url, args=(test_url,), daemon=True).start()
#         print(f"Producer test started with URL: {test_url}")
#
#     yield  # Ứng dụng chạy ở đây
#
#     # Code chạy khi tắt (shutdown)
#     print("Shutting down...")
# # Khởi tạo app với lifespan
# app = FastAPI(lifespan=lifespan)
#
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
# app.include_router(api_router)

# @app.get("/")
# def root():
#     return {"message": "Kafka Video-Audio Streaming API"}

from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
import threading
from contextlib import asynccontextmanager
import time
import os
# Import các module cần thiết
from src.api.routes import router as api_router
from src.producer.controller import process_url
from src.producer.config import minio_client, MINIO_BUCKET, FUSION_OBJECT_NAME
from src.consumer.spark_video import run as run_video_consumer
from src.utils.comment_utils import get_sentiment_results as get_comment_sentiment_results
from src.utils.image_utils import get_sentiment_results as get_video_sentiment_results  # Đổi tên để tránh nhầm lẫn
from src.utils.audio_utils import get_audio_sentiment_results
from src.utils.fusion_utils import get_fusion_sentiment_results, get_fusion_component_results
from src.consumer.spark_audio import run as run_audio_consumer
from src.consumer.spark_comment import run as run_comment_consumer
from src.consumer.fusion_consumer import run as run_fusion_consumer

# Cache kết quả sentiment - với các key rõ ràng để tránh nhầm lẫn
sentiment_results = {
    "comment_sentiment": {"positive": 0, "negative": 0, "neutral": 0},
    "video_sentiment": [],
    "audio_sentiment": [],
    "comment_details": [],
    "fusion_sentiment": {}
}
# Phần lifespan handler để khởi động background tasks
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tạo và khởi động thread cho consumer xử lý video
    video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
    video_consumer_thread.start()
    print("✅ Video consumer started in background")

    # Tạo và khởi động thread cho consumer xử lý audio
    audio_consumer_thread = threading.Thread(target=run_audio_consumer, daemon=True)
    audio_consumer_thread.start()
    print("✅ Audio consumer started in background")

    comment_consumer_thread = threading.Thread(target=run_comment_consumer, daemon=True)
    comment_consumer_thread.start()
    print("✅ Comment consumer started in background")

    fusion_consumer_thread = threading.Thread(target=run_fusion_consumer, daemon=True)
    fusion_consumer_thread.start()
    print("✅ Fusion consumer started in background")

    # Cho consumer thời gian để khởi động hoàn toàn
    time.sleep(5)

    yield  # Ứng dụng chạy ở đây

    # Code chạy khi tắt (shutdown)
    print("🛑 Shutting down...")

# Khởi tạo app với lifespan
app = FastAPI(lifespan=lifespan)

# Templates config
templates = Jinja2Templates(directory="templates")
# Thêm hàm get_flashed_messages giả vào context
templates.env.globals["get_flashed_messages"] = lambda: []

# Nếu có thư mục static, mount nó
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Cấu hình CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Thêm API routes từ module khác
app.include_router(api_router)

def prepare_sentiment_data(result):
    """
    Chuẩn bị dữ liệu sentiment để hiển thị an toàn
    """
    if result is None:
        return {"positive": 0, "negative": 0, "neutral": 0, "status": "processing", "total": 0}

    # Đảm bảo các key sentiment luôn tồn tại
    if "positive" not in result:
        result["positive"] = 0
    if "negative" not in result:
        result["negative"] = 0
    if "neutral" not in result:
        result["neutral"] = 0

    # Đảm bảo các giá trị là số
    result["positive"] = int(result["positive"]) if result["positive"] is not None else 0
    result["negative"] = int(result["negative"]) if result["negative"] is not None else 0
    result["neutral"] = int(result["neutral"]) if result["neutral"] is not None else 0

    # Tính tổng
    result["total"] = result["positive"] + result["negative"] + result["neutral"]

    return result
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Trang chủ"""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "result": None,
        "youtube_url": "",
        "messages": []  # Thêm messages trống để thay thế flash messages
    })

@app.post("/", response_class=HTMLResponse)
async def process_youtube(request: Request, youtube_url: str = Form(...)):
    """Xử lý URL YouTube và hiển thị kết quả trên giao diện"""
    messages = []  # Danh sách thông báo để thay thế flash messages

    if not youtube_url:
        messages.append({"category": "warning", "message": "Vui lòng cung cấp URL YouTube hợp lệ"})
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": "Vui lòng cung cấp URL YouTube hợp lệ",
            "youtube_url": "",
            "messages": messages
        })

    try:
        # Gọi hàm process_url để xử lý URL từ producer
        result = process_url(youtube_url)
        messages.append({"category": "success", "message": "Xử lý URL thành công!"})

        # Chuẩn bị và cache kết quả comment sentiment
        processed_result = prepare_sentiment_data(result)
        sentiment_results["comment_sentiment"] = processed_result

        # Trả về template với kết quả
        return templates.TemplateResponse("index.html", {
            "request": request,
            "result": processed_result,
            "youtube_url": youtube_url,
            "messages": messages
        })
    except Exception as e:
        messages.append({"category": "danger", "message": f"Lỗi xử lý URL: {str(e)}"})
        return templates.TemplateResponse("index.html", {
            "request": request,
            "error": f"Lỗi xử lý URL: {str(e)}",
            "youtube_url": youtube_url,
            "messages": messages
        })

@app.get("/api/get_results")
async def get_results():
    """API endpoint để lấy kết quả sentiment mới nhất của comments"""
    try:
        # Trả về kết quả từ cache
        return sentiment_results["comment_sentiment"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/get_video_sentiments")
async def get_video_sentiments():
    try:
        results = get_video_sentiment_results("emotion_results")

        # Explicitly convert results to JSON-serializable format
        json_safe_results = []
        for frame in results:
            json_safe_frame = {
                "frame_id": frame.get("frame_id", 0),
                "video_id": frame.get("video_id", ""),
                "num_faces": int(frame.get("num_faces", 0)),
                "emotions": frame.get("emotions", {}),
                "processed_at": frame.get("processed_at", "")
            }
            json_safe_results.append(json_safe_frame)

        sentiment_results["video_sentiment"] = json_safe_results
        return json_safe_results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/get_audio_sentiments")
async def get_audio_sentiments(video_id: str = None):
    """API endpoint để lấy kết quả phân tích cảm xúc từ audio"""
    try:
        # Lấy kết quả sentiment từ audio sử dụng hàm từ utils
        results = get_audio_sentiment_results("audio_results")

        # Lọc kết quả theo video_id nếu được cung cấp
        if video_id:
            results = [r for r in results if r.get("video_id") == video_id]

        # Sắp xếp lại kết quả theo thời gian xử lý (mới nhất trước)
        results.sort(key=lambda x: x.get("processed_at", ""), reverse=True)

        # Cache kết quả
        sentiment_results["audio_sentiment"] = results

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/video_emotions", response_class=HTMLResponse)
async def video_emotions_page(request: Request):
    """Trang hiển thị phân tích cảm xúc từ frames video"""
    try:
        # Lấy kết quả từ cache hoặc gọi lại hàm
        if not sentiment_results["video_sentiment"]:
            results = get_video_sentiment_results("emotion_results")
            sentiment_results["video_sentiment"] = results
        else:
            results = sentiment_results["video_sentiment"]

        return templates.TemplateResponse("video_emotions.html", {
            "request": request,
            "results": results,
            "messages": []  # Thêm messages trống
        })
    except Exception as e:
        # Redirect về trang chủ với thông báo lỗi
        return RedirectResponse(url="/", status_code=303)

@app.get("/audio_emotions", response_class=HTMLResponse)
async def audio_emotions_page(request: Request):
    """Trang hiển thị phân tích cảm xúc từ audio"""
    try:
        # Lấy kết quả từ cache hoặc gọi lại hàm
        if not sentiment_results["audio_sentiment"]:
            results = get_audio_sentiment_results("audio_results")
            sentiment_results["audio_sentiment"] = results
        else:
            results = sentiment_results["audio_sentiment"]

        return templates.TemplateResponse("audio_emotions.html", {
            "request": request,
            "results": results,
            "messages": []  # Thêm messages trống
        })
    except Exception as e:
        messages = [{"category": "danger", "message": f"Lỗi khi tải dữ liệu audio: {str(e)}"}]
        # Redirect về trang chủ với thông báo lỗi
        return RedirectResponse(url="/", status_code=303)

# API endpoint bổ sung để debug
@app.get("/api/status")
async def status():
    """Endpoint kiểm tra trạng thái của hệ thống"""
    return {
        "status": "running",
        "comments_analyzed": sum(sentiment_results["comment_sentiment"].values()) if sentiment_results["comment_sentiment"] else 0,
        "video_frames_analyzed": len(sentiment_results["video_sentiment"]),
        "audio_chunks_analyzed": len(sentiment_results["audio_sentiment"]),  # Thêm số lượng audio chunks
    }

@app.get("/api/get_comment_sentiments")
async def get_comment_sentiments(video_id: str = None):
    """API endpoint để lấy kết quả phân tích cảm xúc từ comments"""
    try:
        # Lấy kết quả sentiment từ comments sử dụng hàm từ utils
        results = get_comment_sentiment_results("comment_sentiment_results")

        # Lọc kết quả theo video_id nếu được cung cấp
        if video_id:
            results = [r for r in results if r.get("content_id") == video_id]

        # Cập nhật cache cho kết quả comment sentiment
        comment_sentiment_summary = {"positive": 0, "negative": 0, "neutral": 0}
        languages_detected = {}

        for result in results:
            sentiment = result.get("sentiment", "neutral")
            if sentiment in comment_sentiment_summary:
                comment_sentiment_summary[sentiment] += 1

            # Ghi nhận ngôn ngữ
            language = result.get("language", "unknown")
            if language in languages_detected:
                languages_detected[language] += 1
            else:
                languages_detected[language] = 1

        # Cập nhật cache với kết quả tổng hợp
        sentiment_results["comment_sentiment"] = {
            **comment_sentiment_summary,
            "total": len(results),
            "languages": languages_detected
        }

        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/comment_sentiment", response_class=HTMLResponse)
async def comment_sentiment_page(request: Request, video_id: str = None):
    """Trang hiển thị phân tích cảm xúc từ comments"""
    try:
        # Lấy kết quả sentiment từ comments
        results = get_comment_sentiment_results("comment_sentiment_results")

        # Lọc theo video_id nếu có
        if video_id:
            filtered_results = [r for r in results if r.get("content_id") == video_id]
        else:
            filtered_results = results

        # Tạo tổng hợp theo sentiment
        summary = {"positive": 0, "negative": 0, "neutral": 0}
        languages = {}

        for result in filtered_results:
            sentiment = result.get("sentiment", "neutral")
            if sentiment in summary:
                summary[sentiment] += 1

            # Ghi nhận ngôn ngữ
            language = result.get("language", "unknown")
            if language in languages:
                languages[language] += 1
            else:
                languages[language] = 1

        # Cập nhật cache
        sentiment_results["comment_sentiment"] = {
            **summary,
            "total": len(filtered_results),
            "languages": languages
        }

        return templates.TemplateResponse("comment_sentiment.html", {
            "request": request,
            "results": filtered_results,
            "summary": summary,
            "languages": languages,
            "video_id": video_id,
            "total_comments": len(filtered_results),
            "messages": []
        })
    except Exception as e:
        messages = [{"category": "danger", "message": f"Lỗi khi tải dữ liệu comments: {str(e)}"}]
        return templates.TemplateResponse("comment_sentiment.html", {
            "request": request,
            "results": [],
            "summary": {"positive": 0, "negative": 0, "neutral": 0},
            "languages": {},
            "video_id": video_id,
            "total_comments": 0,
            "messages": messages
        })

@app.get("/fusion_sentiment", response_class=HTMLResponse)
async def fusion_sentiment_page(request: Request):
    """Trang hiển thị kết quả phân tích cảm xúc tổng hợp (fusion)"""
    try:
        # Get video_id from query parameters if available
        video_id = request.query_params.get('video_id')

        # Create a cache key based on video_id if available
        cache_key = f"fusion_sentiment_{video_id}" if video_id else "fusion_sentiment"

        # Kiểm tra xem đã có kết quả fusion trong cache chưa
        if cache_key not in sentiment_results:
            # Nếu chưa, thử lấy từ fusion utils
            result = get_fusion_sentiment_results(video_id=video_id)
            if result:
                sentiment_results[cache_key] = result
            else:
                # Nếu không có kết quả, hiển thị thông báo đang xử lý
                video_msg = f" cho video ID: {video_id}" if video_id else ""
                messages = [{"category": "info",
                             "message": f"Đang xử lý phân tích cảm xúc tổng hợp{video_msg}. Trang sẽ tự động cập nhật khi có kết quả."}]
                return templates.TemplateResponse("fusion_sentiment.html", {
                    "request": request,
                    "result": {},
                    "video_id": video_id,  # Pass video_id to template
                    "auto_refresh": True,  # Thêm flag để template biết cần auto-refresh
                    "messages": messages
                })

        # Lấy thêm kết quả thành phần nếu có template hiển thị chi tiết
        component_results = None
        try:
            # If your get_fusion_component_results doesn't yet support video_id,
            # you might need to modify that function too
            component_results = get_fusion_component_results(video_id=video_id)
        except Exception:
            pass  # Bỏ qua lỗi khi không lấy được kết quả thành phần

        return templates.TemplateResponse("fusion_sentiment.html", {
            "request": request,
            "result": sentiment_results[cache_key],
            "components": component_results,
            "video_id": video_id,  # Pass video_id to template
            "auto_refresh": False,  # Không cần auto-refresh nữa vì đã có kết quả
            "messages": []
        })
    except Exception as e:
        video_id = request.query_params.get('video_id')
        video_msg = f" cho video ID: {video_id}" if video_id else ""
        messages = [{"category": "danger", "message": f"Lỗi khi tải dữ liệu fusion{video_msg}: {str(e)}"}]
        return templates.TemplateResponse("fusion_sentiment.html", {
            "request": request,
            "result": {},
            "video_id": video_id,  # Pass video_id to template
            "auto_refresh": True,  # Vẫn cần auto-refresh để thử lại
            "messages": messages
        })
# Run uvicorn server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)