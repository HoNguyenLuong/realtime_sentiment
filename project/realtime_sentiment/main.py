from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading

from consumer.video_consumer import run as consume_video_frames
from consumer.audio_consumer import run as consume_audio_stream
from api.routes import router as api_router

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
