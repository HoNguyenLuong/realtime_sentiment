from .common import get_kafka_consumer, decode_base64, logger, AUDIO_TOPIC

def process_audio_chunk(audio_bytes: bytes, metadata: dict):
    # TODO: gọi model STT, sentiment analysis từ giọng nói ở đây
    logger.info(f"[AUDIO] Processing audio chunk from {metadata.get('source')} at {metadata.get('timestamp')}")
    logger.info(f"[AUDIO] Audio bytes length: {len(audio_bytes)}")

def run():
    consumer = get_kafka_consumer(AUDIO_TOPIC)
    logger.info(f"Listening to Kafka topic: {AUDIO_TOPIC}")

    for msg in consumer:
        data = msg.value
        audio_b64 = data.get("audio")
        metadata = data.get("metadata", {})

        if audio_b64:
            audio_bytes = decode_base64(audio_b64)
            process_audio_chunk(audio_bytes, metadata)
        else:
            logger.warning("No audio content found in message.")
