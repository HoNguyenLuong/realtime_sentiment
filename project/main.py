from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
from contextlib import asynccontextmanager
from realtime_sentiment.api.routes import router as api_router
from realtime_sentiment.producer.controller import process_url 
from realtime_sentiment.consumer.common import get_kafka_consumer

# Phần lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code chạy khi khởi động (startup)
    print("Starting consumers...")
    
    # ✅ Chạy producer test với 1 link cụ thể
    test_url = "https://www.youtube.com/watch?v=iOIgsASasX4"
    threading.Thread(target=process_url, args=(test_url,), daemon=True).start()
    
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