from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading

from consumer.spark_video import run as consume_video_frames
from consumer.spark_audio import run as consume_audio_stream
from api.routes import router as api_router
from producer.controller import process_url
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/")
def root():
    return {"message": "Kafka Video-Audio Streaming API"}

@app.on_event("startup")
def start_consumers():
    threading.Thread(target=consume_video_frames, daemon=True).start()
    threading.Thread(target=consume_audio_stream, daemon=True).start()

if __name__ == "__main__":
    url = "https://www.youtube.com/watch?v=GhOZFZStyPs"
    process_url(url)