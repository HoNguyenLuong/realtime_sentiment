import os
import time
import socket
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading
from contextlib import asynccontextmanager
from src.api.routes import router as api_router
from src.producer.controller import process_url
from src.consumer.spark_video import run as run_video_consumer

# Set default environment variables
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("SPARK_MASTER", "spark://localhost:7077")
os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
os.environ.setdefault("MINIO_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MINIO_SECRET_KEY", "minioadmin")
os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
os.environ.setdefault("PYSPARK_PYTHON", "python3.9")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", "python3.9")

def wait_for_kafka():
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    host, port = kafka_servers.split(":")
    port = int(port)

    print(f"🔄 Waiting for Kafka to be ready at {host}:{port}...")

    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("✅ Kafka is up!")
                break
        except (socket.timeout, ConnectionRefusedError):
            print(f"⏳ Kafka ({host}:{port}) not ready yet...")
            time.sleep(2)

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start video consumer thread
    video_consumer_thread = threading.Thread(target=run_video_consumer, daemon=True)
    video_consumer_thread.start()
    print("Video consumer started in background")

    # Allow consumer to fully start
    time.sleep(5)

    # Run producer test with a specific link
    test_url = "https://youtube.com/playlist?list=PL1A6fR5hha3hmMT07If8KJ7UnQXdBt1EU&si=EJCGHbg32WQSwISe"
    if test_url:
        threading.Thread(target=process_url, args=(test_url,), daemon=True).start()
        print(f"Producer test started with URL: {test_url}")

    yield  # Application runs here

    # Code to run on shutdown
    print("Shutting down...")

# Initialize app with lifespan
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

if __name__ == "__main__":
    # Wait for Kafka to be ready
    wait_for_kafka()

    # Start the application
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)