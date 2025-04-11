from common import get_kafka_consumer, decode_base64, logger, VIDEO_TOPIC
import cv2
import numpy as np

def process_video_frame(image_bytes: bytes, metadata: dict):
    nparr = np.frombuffer(image_bytes, np.uint8)
    frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if frame is not None:
        logger.info(f"[VIDEO] Processing frame from {metadata.get('source')} at {metadata.get('timestamp')}")
        logger.info(f"[VIDEO] Frame shape: {frame.shape}")
        # TODO: Gọi emotion detector, face recognition, v.v.
    else:
        logger.warning("Failed to decode frame.")

def run():
    consumer = get_kafka_consumer(VIDEO_TOPIC)
    logger.info(f"Listening to Kafka topic: {VIDEO_TOPIC}")

    for msg in consumer:
        data = msg.value
        image_b64 = data.get("image")
        metadata = data.get("metadata", {})

        if image_b64:
            image_bytes = decode_base64(image_b64)
            process_video_frame(image_bytes, metadata)
        else:
            logger.warning("No image content found in message.")
