from collections import Counter
from datetime import datetime

# Mapping emotion → sentiment
emotion_to_sentiment = {
    "happy": ("positive", 1.0),
    "sad": ("negative", 1.0),
    "angry": ("negative", 1.0),
    "fear": ("negative", 0.7),
    "surprise": ("neutral", 0.8),
    "neutral": ("neutral", 1.0)
}
label_to_score = {"positive": 1, "neutral": 0, "negative": -1}

def filter_frames_in_chunk(frames, start_time, end_time):
    start = datetime.fromisoformat(start_time)
    end = datetime.fromisoformat(end_time)
    return [f for f in frames if start <= datetime.fromisoformat(f["timestamp"]) <= end]

def majority_emotion(frames):
    all_emotions = []
    for f in frames:
        all_emotions.extend(f["emotions"])
    if not all_emotions:
        return "neutral"
    counter = Counter(all_emotions)
    return counter.most_common(1)[0][0]


def fuse_audio_video_chunk(audio_chunk, frames_in_chunk, audio_weight=0.5):
    # chọn nhãn sentiment có score cao nhất từ audio
    audio_sentiment_label = max(audio_chunk["sentiment"], key=audio_chunk["sentiment"].get)
    audio_sentiment_score = audio_chunk["sentiment"][audio_sentiment_label]

    # xác định emotion chính từ video
    dominant_emotion = majority_emotion(frames_in_chunk)
    video_sentiment_label, video_score = emotion_to_sentiment.get(dominant_emotion, ("neutral", 1.0))

    # map label to score
    label_to_score = {"positive": 1, "neutral": 0, "negative": -1}
    audio_val = label_to_score[audio_sentiment_label]
    video_val = label_to_score[video_sentiment_label]

    # weighted fusion
    combined_score = audio_weight * audio_sentiment_score * audio_val + (1 - audio_weight) * video_score * video_val

    # decision
    if combined_score > 0.2:
        return "positive"
    elif combined_score < -0.2:
        return "negative"
    else:
        return "neutral"
