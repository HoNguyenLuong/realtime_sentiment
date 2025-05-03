from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
from contextlib import asynccontextmanager
from src.api.routes import router as api_router
from src.producer.controller import process_url
# Import thêm consumer xử lý video
from src.consumer.spark_video import run as run_video_consumer
import time

# Phần lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Tạo và khởi động thread cho consumer xử lý video
    video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
    video_consumer_thread.start()
    print("Video consumer started in background")

    # Cho consumer thời gian để khởi động hoàn toàn
    time.sleep(5)

    # ✅ Chạy producer test với 1 link cụ thể
    test_url = "https://youtube.com/playlist?list=PL1A6fR5hha3hmMT07If8KJ7UnQXdBt1EU&si=EJCGHbg32WQSwISe"
    if test_url:  # Chỉ chạy producer test nếu có URL
        threading.Thread(target=process_url, args=(test_url,), daemon=True).start()
        print(f"Producer test started with URL: {test_url}")

    yield  # Ứng dụng chạy ở đây

    # Code chạy khi tắt (shutdown)
    print("Shutting down...")
# Khởi tạo app với lifespan
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

# @app.get("/")
# def root():
#     return {"message": "Kafka Video-Audio Streaming API"}


