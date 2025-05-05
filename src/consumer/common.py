# consumer/common.py
import base64
import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.producer.config import CONFIG  # <<< Thêm dòng này để dùng chung CONFIG

# Kafka topics
VIDEO_TOPIC = 'video_frames'
AUDIO_TOPIC = 'audio_stream'
PROCESSED_TOPIC = 'processed_frames'  # Topic để đánh dấu đã xử lý xong

def get_kafka_consumer(topic: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],  # <<< lấy từ CONFIG
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id=f"{topic}_consumer_group"
    )


def mark_as_processed(producer, metadata):
    """
    Đánh dấu metadata đã được xử lý bằng cách gửi thông báo tới topic processed_frames
    """
    try:
        producer.send(
            PROCESSED_TOPIC,
            {
                'frame_id': metadata.get('frame_id'),
                'video_id': metadata.get('video_id'),
                'bucket_name': metadata.get('bucket_name'),
                'object_name': metadata.get('object_name'),
                'status': 'processed',
                'timestamp': metadata.get('timestamp')
            }
        )
        producer.flush()
        logger.info(f"Đã đánh dấu metadata đã xử lý: {metadata.get('frame_id')}")
    except Exception as e:
        logger.error(f"Lỗi khi đánh dấu metadata đã xử lý: {str(e)}")

# decode base64 to bytes
def decode_base64(data_str: str) -> bytes:
    return base64.b64decode(data_str)


# logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")
