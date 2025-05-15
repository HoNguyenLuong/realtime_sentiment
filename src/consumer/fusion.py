from collections import Counter
from datetime import datetime
import json, io
from typing import List, Dict

from collections import defaultdict, Counter
from src.utils.audio_utils import get_audio_sentiment_results
from src.utils.image_utils import get_frame_sentiment_results
from src.utils.comment_utils import get_comment_sentiment_results
from src.llm.call_llm import use_llm_for_sentiment

# Mapping emotion → sentiment with confidence scores
emotion_to_sentiment = {
    "happy": ("positive", 1.0),
    "sad": ("negative", 0.8),
    "angry": ("negative", 0.9),
    "fear": ("negative", 0.7),
    "surprise": ("neutral", 0.6),  # Surprise có thể tích cực hoặc tiêu cực
    "neutral": ("neutral", 1.0)
}

# Map sentiment labels to numeric scores
label_to_score = {"positive": 1, "neutral": 0, "negative": -1}


def aggregate_sentiment_from_frames(frame_results: List[Dict]) -> Dict[str, Dict]:
    video_sentiment_scores = defaultdict(list)
    video_frame_counts = defaultdict(int)

    for item in frame_results:
        video_id = item.get("video_id", "")
        emotions = item.get("emotions", {})
        video_frame_counts[video_id] += 1

        for emotion in emotions:
            sentiment, weight = emotion_to_sentiment.get(emotion, ("neutral", 1.0))
            video_sentiment_scores[video_id].append((sentiment, weight))

    video_final_stats = {}

    for video_id, sentiments in video_sentiment_scores.items():
        sentiment_weights = defaultdict(float)
        for sentiment, weight in sentiments:
            sentiment_weights[sentiment] += weight

        final_sentiment = max(sentiment_weights.items(), key=lambda x: x[1])[0]
        video_final_stats[video_id] = {
            "sentiment": final_sentiment,
            "num_frames": video_frame_counts[video_id]
        }

    return video_final_stats


def aggregate_sentiment_from_audio(audio_results: List[Dict]) -> Dict[str, Dict]:
    audio_sentiment_scores = defaultdict(list)
    audio_chunk_counts = defaultdict(int)

    for item in audio_results:
        audio_id = item.get("audio_id", "")
        emotion = item.get("emotion", "")
        sentiment, weight = emotion_to_sentiment.get(emotion, ("neutral", 1.0))
        audio_sentiment_scores[audio_id].append((sentiment, weight))
        audio_chunk_counts[audio_id] += 1

    audio_final_stats = {}

    for audio_id, sentiments in audio_sentiment_scores.items():
        sentiment_weights = defaultdict(float)
        for sentiment, weight in sentiments:
            sentiment_weights[sentiment] += weight

        final_sentiment = max(sentiment_weights.items(), key=lambda x: x[1])[0]
        audio_final_stats[audio_id] = {
            "sentiment": final_sentiment,
            "num_chunks": audio_chunk_counts[audio_id]
        }

    return audio_final_stats

def aggregate_comment_sentiment(comment_results: List[Dict]) -> Dict[str, Dict]:
    # Khởi tạo dictionary để lưu kết quả sentiment của từng content_id hoặc video_id
    content_sentiment_scores = defaultdict(list)
    content_comment_counts = defaultdict(int)

    # Duyệt qua từng kết quả comment để tính toán sentiment tổng hợp
    for item in comment_results:
        content_id = item.get("content_id", "")
        sentiment = item.get("sentiment", "neutral")  # Nhãn cảm xúc của comment

        # Lưu trữ kết quả sentiment vào dictionary
        content_sentiment_scores[content_id].append(sentiment)
        content_comment_counts[content_id] += 1

    # Tổng hợp kết quả cho từng content_id
    content_final_stats = {}
    for content_id, sentiments in content_sentiment_scores.items():
        # Đếm số lượng từng loại sentiment (positive, negative, neutral)
        sentiment_count = defaultdict(int)
        for sentiment in sentiments:
            sentiment_count[sentiment] += 1

        # Chọn sentiment chiếm ưu thế (dominant sentiment)
        final_sentiment = max(sentiment_count.items(), key=lambda x: x[1])[0]
        content_final_stats[content_id] = {
            "sentiment": final_sentiment,
            "num_comments": content_comment_counts[content_id]
        }

    return content_final_stats

def get_overall_video_sentiments(topic_name: str) -> Dict[str, dict]:
    frame_results = get_frame_sentiment_results(topic_name)
    return aggregate_sentiment_from_frames(frame_results)

def get_overall_audio_sentiments(topic_data: str) -> Dict[str, dict]:
    audio_results = get_audio_sentiment_results(topic_data)
    return aggregate_sentiment_from_audio(audio_results)


def get_overall_comment_sentiment(topic_name: str) -> Dict[str, Dict]:
    # Lấy kết quả sentiment của các comment từ Kafka
    comment_results = get_comment_sentiment_results(topic_name)

    # Tổng hợp sentiment từ các comment
    return aggregate_comment_sentiment(comment_results)


def generate_final_sentiment_result():
    cmt_res = get_overall_comment_sentiment("video_comments")
    frame_res = get_overall_video_sentiments("video_frames")
    audio_res = get_overall_audio_sentiments("audio_stream")

    # 2. Gộp tổng số lượng frame và chunk từ tất cả video/audio
    num_cmt = sum(item.get("num_comments", 0) for item in cmt_res.values())
    num_frames = sum(item.get("num_frames", 0) for item in frame_res.values())
    num_chunks = sum(item.get("num_chunks", 0) for item in audio_res.values())

    # Gọi LLM để tổng hợp kết quả cuối cùng
    final_result = use_llm_for_sentiment(audio_res, frame_res, num_frames,num_chunks)

    return final_result


def process_and_save_fusion_results_to_minio(client, bucket_name, object_name):
    fusion_results = generate_final_sentiment_result()

    json_data = json.dumps(fusion_results, indent=2).encode("utf-8")
    data_stream = io.BytesIO(json_data)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(json_data),
        content_type="application/json"
    )

def load_sentiment_result_from_minio(client, bucket_name: str, object_name: str) -> dict:
    """
    Tải kết quả sentiment đã lưu từ MinIO.

    Args:
        client: Đối tượng MinIO client đã kết nối.
        bucket_name (str): Tên bucket trong MinIO.
        object_name (str): Đường dẫn/tên file cần đọc.

    Returns:
        dict: Dữ liệu sentiment đã được load.
    """
    response = client.get_object(bucket_name, object_name)
    json_bytes = response.read()
    return json.loads(json_bytes.decode("utf-8"))
