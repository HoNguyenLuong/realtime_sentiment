import base64
import json
import numpy as np
from kafka import KafkaProducer
from config import CONFIG

producer = KafkaProducer(
    bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(4, 0)
)

def send_frame(video_id, frame_id, frame_bytes, source='youtube'):
    if isinstance(frame_bytes, np.ndarray):
        frame_bytes = frame_bytes.tobytes()
    producer.send('video_frames', {
        'video_id': video_id,
        'frame_id': frame_id,
        'source': source,
        'data': base64.b64encode(frame_bytes).decode('utf-8')
    })

def send_audio(video_id, chunk_id, chunk, source='youtube'):
    if isinstance(chunk, np.ndarray):
        chunk = chunk.tobytes()
    producer.send('audio_stream', {
        'video_id': video_id,
        'chunk_id': chunk_id,
        'source': source,
        'data': base64.b64encode(chunk).decode('utf-8')
    })

def close_producer():
    producer.flush()
    producer.close()
