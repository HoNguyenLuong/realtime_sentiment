from collections import Counter
from datetime import datetime
import json, io
from typing import List, Dict

from collections import defaultdict, Counter
from src.utils.audio_utils import get_audio_sentiment_results
from src.utils.image_utils import get_sentiment_results
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


def get_overall_video_sentiments(topic_name: str) -> dict[str, dict]:
    frame_results = get_sentiment_results(topic_name)
    return aggregate_sentiment_from_frames(frame_results)

def get_overall_audio_sentiments(topic_data: str) -> dict[str, dict]:
    audio_results = get_audio_sentiment_results(topic_data)
    return aggregate_sentiment_from_audio(audio_results)

def generate_final_sentiment_result():
    frame_res = get_overall_video_sentiments("video_frames")
    audio_res = get_overall_audio_sentiments("audio_stream")

    # 2. Gộp tổng số lượng frame và chunk từ tất cả video/audio
    num_frames = sum(item.get("num_frames", 0) for item in frame_res.values())
    num_chunks = sum(item.get("num_chunks", 0) for item in audio_res.values())

    # Gọi LLM để tổng hợp kết quả cuối cùng
    final_result = use_llm_for_sentiment(,frame_res, audio_res, ,num_chunks, num_frames)

# def parse_timestamp(ts_str):
#     """Phân tích chuỗi timestamp thành đối tượng datetime."""
#     try:
#         return datetime.fromisoformat(ts_str)
#     except ValueError:
#         # Xử lý trường hợp timestamp không đúng định dạng
#         print(f"Lỗi phân tích timestamp: {ts_str}")
#         return None
#
#
# def filter_frames_in_chunk(frames, start_time, end_time):
#     """Lọc frames nằm trong khoảng thời gian của một chunk audio."""
#     if not frames:
#         return []
#
#     start = parse_timestamp(start_time)
#     end = parse_timestamp(end_time)
#
#     if not start or not end:
#         return []
#
#     return [f for f in frames if start <= parse_timestamp(f["timestamp"]) <= end]
#
#
# def calculate_emotion_distribution(frames):
#     """Tính toán phân phối cảm xúc từ các frames."""
#     all_emotions = []
#     for frame in frames:
#         all_emotions.extend(frame.get("emotions", []))
#
#     if not all_emotions:
#         return {"neutral": 1.0}
#
#     counter = Counter(all_emotions)
#     total = sum(counter.values())
#     return {emotion: count / total for emotion, count in counter.items()}
#
#
# def get_chunk_time_range(chunk_index, chunk_duration=30):
#     """Tính toán khoảng thời gian cho một chunk dựa trên index."""
#     start_seconds = chunk_index * chunk_duration
#     end_seconds = (chunk_index + 1) * chunk_duration
#
#     # Chuyển đổi giây sang chuỗi thời gian
#     # Trong trường hợp thực tế, cần phải có timestamp gốc
#     base_time = datetime.fromisoformat("2025-05-04T16:13:00")
#
#     # Sử dụng timedelta để thêm giây vào base_time
#     from datetime import timedelta
#     start_time = base_time + timedelta(seconds=start_seconds)
#     end_time = base_time + timedelta(seconds=end_seconds)
#
#     return start_time.isoformat(), end_time.isoformat()
#
#
# def weighted_sentiment_score(sentiment_dict):
#     """Tính toán điểm sentiment có trọng số từ dict sentiment."""
#     score = 0
#     for label, weight in sentiment_dict.items():
#         label_lower = label.lower()  # Chuẩn hóa key
#         if label_lower in label_to_score:
#             score += label_to_score[label_lower] * weight
#     return score
#
#
# def fuse_audio_video_chunk(audio_chunk, frames_in_chunk, audio_weight=0.6):
#     """
#     Kết hợp sentiment từ audio và video với trọng số.
#
#     Args:
#         audio_chunk: Dict chứa thông tin sentiment từ audio
#         frames_in_chunk: List frames trong khoảng thời gian của chunk
#         audio_weight: Trọng số cho audio (0-1)
#
#     Returns:
#         String: "positive", "negative", hoặc "neutral"
#     """
#     # Xử lý dữ liệu audio
#     if not audio_chunk.get("sentiment"):
#         audio_score = 0
#     else:
#         # Lấy nhãn sentiment có score cao nhất từ audio
#         audio_sentiment_label = max(audio_chunk["sentiment"], key=audio_chunk["sentiment"].get)
#         audio_sentiment_score = audio_chunk["sentiment"][audio_sentiment_label]
#         audio_score = label_to_score.get(audio_sentiment_label.lower(), 0) * audio_sentiment_score
#
#     # Xử lý dữ liệu video
#     if not frames_in_chunk:
#         video_score = 0
#     else:
#         # Tính phân phối cảm xúc từ video
#         emotion_distribution = calculate_emotion_distribution(frames_in_chunk)
#
#         # Tính điểm sentiment tổng hợp từ phân phối cảm xúc
#         video_score = 0
#         for emotion, proportion in emotion_distribution.items():
#             sentiment_label, confidence = emotion_to_sentiment.get(emotion, ("neutral", 0.5))
#             video_score += label_to_score.get(sentiment_label, 0) * proportion * confidence
#
#     # Kết hợp với trọng số
#     combined_score = audio_weight * audio_score + (1 - audio_weight) * video_score
#
#     # Quyết định nhãn sentiment cuối cùng
#     if combined_score > 0.2:
#         return "positive"
#     elif combined_score < -0.2:
#         return "negative"
#     else:
#         return "neutral"
#
#
# def process_data(audio_chunks, frames, chunk_duration=30):
#     """
#     Xử lý toàn bộ dữ liệu âm thanh và video để tạo kết quả tổng hợp.
#
#     Args:
#         audio_chunks: List các chunk audio đã được phân tích
#         frames: List các frame video đã được phân tích
#         chunk_duration: Thời lượng mỗi chunk (giây)
#
#     Returns:
#         List kết quả sentiment được kết hợp cho mỗi chunk
#     """
#     results = []
#
#     for i, chunk in enumerate(audio_chunks):
#         start_time, end_time = get_chunk_time_range(i, chunk_duration)
#         frames_in_chunk = filter_frames_in_chunk(frames, start_time, end_time)
#
#         fusion_result = fuse_audio_video_chunk(chunk, frames_in_chunk)
#
#         results.append({
#             "chunk_index": i,
#             "time_range": f"{i * chunk_duration}s - {(i + 1) * chunk_duration}s",
#             "audio_text": chunk.get("text", ""),
#             "audio_sentiment": chunk.get("sentiment", {}),
#             "audio_emotion": chunk.get("emotion", []),
#             "video_frames_count": len(frames_in_chunk),
#             "fused_sentiment": fusion_result
#         })
#
#     return results

def process_and_save_fusion_results_to_minio(client, fusion_results, bucket_name, object_name):

    frames_results = get_sentiment_results("emotion_frames")
    audio_results = get_audio_sentiment_results()
    fusion_results = process_data(audio_results, frames_results)

    json_data = json.dumps(fusion_results, indent=2).encode("utf-8")
    data_stream = io.BytesIO(json_data)

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=len(json_data),
        content_type="application/json"
    )
