# consumer/common.py
import json
import logging
from kafka import KafkaConsumer, KafkaProducer

# Kafka config
KAFKA_BROKER = 'localhost:9092'
VIDEO_TOPIC = 'video_frames'
AUDIO_TOPIC = 'audio_stream'
PROCESSED_TOPIC = 'processed_frames'  # Topic để đánh dấu đã xử lý xong


def get_kafka_consumer(topic: str):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id=f"{topic}_consumer_group"
    )


def get_kafka_producer():
    """
    Tạo và trả về một Kafka producer

    Returns:
        KafkaProducer: Kafka producer instance
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


def mark_as_processed(producer, metadata):
    """
    Đánh dấu metadata đã được xử lý bằng cách gửi thông báo tới topic processed_frames

    Args:
        producer (KafkaProducer): Kafka producer instance
        metadata (dict): Metadata cần đánh dấu đã xử lý
    """
    try:
        # Gửi xác nhận đã xử lý xong
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


# logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")