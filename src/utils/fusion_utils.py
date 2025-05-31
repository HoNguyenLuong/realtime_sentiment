# src/utils/fusion_utils.py
import os
from src.consumer.fusion import (
    load_sentiment_result_from_minio,
    generate_final_sentiment_result,
    process_and_save_fusion_results_to_minio,
    get_overall_video_sentiments,
    get_overall_audio_sentiments,
    get_overall_comment_sentiment
)
from src.producer.config import minio_client, MINIO_BUCKET, FUSION_OBJECT_NAME

# Cache cho kết quả fusion sentiment
_fusion_results_cache = {}

def is_valid_fusion_result(data):
    """
    Kiểm tra dữ liệu có hợp lệ hay không (không phải None, không rỗng, có đủ trường cần thiết).
    """
    return (
        isinstance(data, dict)
        and "weighted_results" in data
        and isinstance(data.get("weighted_results"), dict)
        and "overall_sentiment" in data
    )

def get_fusion_sentiment_results(video_id=None, object_name=None):
    global _fusion_results_cache

    if not object_name:
        object_name = FUSION_OBJECT_NAME
        # If video_id is provided, check for specific file
        if video_id:
            base, ext = os.path.splitext(object_name)
            object_name = f"{base}_{video_id}{ext}"

    # Check if results are already in cache
    if video_id and video_id in _fusion_results_cache:
        return _fusion_results_cache[video_id]
    elif not video_id and _fusion_results_cache:
        return _fusion_results_cache

    try:
        result = load_sentiment_result_from_minio(minio_client, MINIO_BUCKET, object_name)
        if not is_valid_fusion_result(result):
            print("⚠️ Fusion result is invalid or incomplete")
            return {}

        # Store in appropriate cache location
        if video_id:
            if not isinstance(_fusion_results_cache, dict):
                _fusion_results_cache = {}
            _fusion_results_cache[video_id] = result
        else:
            _fusion_results_cache = result

        return result
    except Exception as e:
        print(f"⚠️ Warning: Could not load fusion sentiment results: {str(e)}")
        return {}


def run_fusion_analysis(url_id=None):
    """
    Chạy phân tích cảm xúc tổng hợp và cập nhật cache

    Args:
        url_id (str, optional): ID của video để lưu kết quả riêng biệt

    Returns:
        Dict chứa kết quả fusion sentiment
    """
    global _fusion_results_cache

    # Tạo kết quả fusion
    fusion_result = generate_final_sentiment_result()

    # Lưu vào cache
    if url_id:
        _fusion_results_cache[url_id] = fusion_result
    else:
        _fusion_results_cache = fusion_result

    # Lưu vào MinIO
    try:
        saved_object_name = process_and_save_fusion_results_to_minio(
            minio_client, MINIO_BUCKET, FUSION_OBJECT_NAME, url_id)
        print(f"✅ Saved fusion results to MinIO: {saved_object_name}")
    except Exception as e:
        print(f"⚠️ Warning: Failed to save fusion results to MinIO: {str(e)}")

    return fusion_result


def get_fusion_component_results(video_id=None):
    """
    Lấy kết quả thành phần từ các nguồn khác nhau để hiển thị chi tiết

    Args:
        video_id (str, optional): ID của video để lấy kết quả riêng biệt

    Returns:
        Dict chứa kết quả sentiment từ video, audio và comments
    """
    # If you have separate functions to filter by video_id, use them here
    # Otherwise, you might need to implement filtering in these functions
    video_sentiments = get_overall_video_sentiments("video_frames")
    audio_sentiments = get_overall_audio_sentiments("audio_stream")
    comment_sentiments = get_overall_comment_sentiment("video_comments")

    # Filter results by video_id if provided
    if video_id:
        # Extract only the data for the specified video_id
        # This assumes the sentiment results contain data indexed by video_id
        # Adjust according to your data structure
        video_sentiments = {k: v for k, v in video_sentiments.items() if k == video_id}
        audio_sentiments = {k: v for k, v in audio_sentiments.items() if k == video_id}
        comment_sentiments = {k: v for k, v in comment_sentiments.items() if k == video_id}

    return {
        "video": video_sentiments,
        "audio": audio_sentiments,
        "comments": comment_sentiments
    }

def clear_fusion_cache():
    """
    Xóa cache kết quả fusion để buộc phải tải lại từ MinIO hoặc tính toán lại
    """
    global _fusion_results_cache
    _fusion_results_cache = {}