# consumer/common.py
import base64
import json
import logging

from kafka import KafkaConsumer

# Kafka config
KAFKA_BROKER = 'localhost:9092'
VIDEO_TOPIC = 'video_frames'
AUDIO_TOPIC = 'audio_stream'

def get_kafka_consumer(topic: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id=f"{topic}_consumer_group"
    )

# decode base64 to bytes
def decode_base64(data_str: str) -> bytes:
    return base64.b64decode(data_str)

# logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")
