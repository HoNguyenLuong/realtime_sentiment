import os
import threading
import time
import json
from kafka import KafkaConsumer
from src.utils.fusion_utils import run_fusion_analysis
from src.producer.config import CONFIG
import traceback

# Kafka topics để lắng nghe
COMMENT_TOPIC = "video_comments"
VIDEO_TOPIC = "video_frames"
AUDIO_TOPIC = "audio_stream"

# Cache cho theo dõi số lượng dữ liệu mới
data_counters = {
    "comments": 0,
    "video_frames": 0,
    "audio_chunks": 0
}

# Ngưỡng để kích hoạt phân tích fusion
THRESHOLD = {
    "comments": 0,
    "video_frames": 5,
    "audio_chunks": 1
}

# Flag kiểm soát trạng thái fusion
fusion_running = False
fusion_last_run = 0
MIN_INTERVAL = 60  # giây

def should_run_fusion():
    """Kiểm tra điều kiện chạy fusion analysis"""
    global fusion_last_run
    current_time = time.time()
    if current_time - fusion_last_run < MIN_INTERVAL:
        return False

    if (data_counters["comments"] >= THRESHOLD["comments"] and
        data_counters["video_frames"] >= THRESHOLD["video_frames"] and
        data_counters["audio_chunks"] >= THRESHOLD["audio_chunks"]):
        return True

    return False

def reset_counters():
    """Reset bộ đếm"""
    global data_counters
    data_counters = {
        "comments": 0,
        "video_frames": 0,
        "audio_chunks": 0
    }

def process_message(topic):
    """Tăng bộ đếm theo topic"""
    if topic == COMMENT_TOPIC:
        data_counters["comments"] += 1
    elif topic == VIDEO_TOPIC:
        data_counters["video_frames"] += 1
    elif topic == AUDIO_TOPIC:
        data_counters["audio_chunks"] += 1

def run():
    """Consumer lắng nghe các topic và trigger fusion khi đủ dữ liệu"""
    print("▶️ Starting fusion consumer...")

    group_id = f"{os.getenv('KAFKA_GROUP_ID', 'default-group')}_fusion"

    consumer = KafkaConsumer(
        COMMENT_TOPIC, VIDEO_TOPIC, AUDIO_TOPIC,
        bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
        group_id=group_id,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    global fusion_running, fusion_last_run

    try:
        for message in consumer:
            topic = message.topic
            process_message(topic)

            if not fusion_running and should_run_fusion():
                fusion_running = True
                print(f"🔄 Running fusion analysis with new data: {data_counters}")

                try:
                    run_fusion_analysis()
                    fusion_last_run = time.time()
                    reset_counters()
                except Exception as e:
                    print(f"❌ Error running fusion analysis: {str(e)}")
                    traceback.print_exc()
                finally:
                    fusion_running = False

    except KeyboardInterrupt:
        print("🛑 Stopping fusion consumer...")
    finally:
        consumer.close()
