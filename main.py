from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import threading
import time
import os

# Import the router from routes.py
from src.api.routes import router as api_router
from src.consumer.spark_video import run as run_video_consumer

# Lifespan handler to start background tasks
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the video consumer in a background thread
    video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
    video_consumer_thread.start()
    print("✅ Video consumer started in background")

    # Allow time for the consumer to fully initialize
    time.sleep(5)

    yield  # Application runs here

    # Code to execute during shutdown
    print("🛑 Shutting down...")

# Initialize the FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)

# Mount static files if the directory exists
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the API router
app.include_router(api_router)

# Run the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)