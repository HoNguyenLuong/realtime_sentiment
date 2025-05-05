from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware
from src.api.api import app as flask_app
import threading
from contextlib import asynccontextmanager
from src.consumer.spark_video import run as run_video_consumer
import time

@asynccontextmanager
async def lifespan(app: FastAPI):
    video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
    video_consumer_thread.start()
    print("Video consumer started in background")
    time.sleep(5)
    yield
    print("Shutting down...")

app = FastAPI(lifespan=lifespan)

# Mount the Flask app as a WSGI middleware
app.add_middleware(WSGIMiddleware, app=flask_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=5000, reload=True)