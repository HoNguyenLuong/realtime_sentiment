# src/utils/fusion_utils.py
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

def get_fusion_sentiment_results(object_name=None):
    global _fusion_results_cache

    if not object_name:
        object_name = FUSION_OBJECT_NAME

    if _fusion_results_cache:
        return _fusion_results_cache

    try:
        result = load_sentiment_result_from_minio(minio_client, MINIO_BUCKET, object_name)
        if not is_valid_fusion_result(result):
            print("⚠️ Fusion result is invalid or incomplete")
            return {}
        _fusion_results_cache = result
        return _fusion_results_cache
    except Exception as e:
        print(f"⚠️ Warning: Could not load fusion sentiment results: {str(e)}")
        return {}


def run_fusion_analysis():
    """
    Chạy phân tích cảm xúc tổng hợp và cập nhật cache

    Returns:
        Dict chứa kết quả fusion sentiment
    """
    global _fusion_results_cache

    # Tạo kết quả fusion
    fusion_result = generate_final_sentiment_result()

    # Lưu vào cache
    _fusion_results_cache = fusion_result

    # Lưu vào MinIO
    try:
        process_and_save_fusion_results_to_minio(
            minio_client, MINIO_BUCKET, FUSION_OBJECT_NAME)
    except Exception as e:
        print(f"Warning: Failed to save fusion results to MinIO: {str(e)}")

    return fusion_result


def get_fusion_component_results():
    """
    Lấy kết quả thành phần từ các nguồn khác nhau để hiển thị chi tiết

    Returns:
        Dict chứa kết quả sentiment từ video, audio và comments
    """
    video_sentiments = get_overall_video_sentiments("video_frames")
    audio_sentiments = get_overall_audio_sentiments("audio_stream")
    comment_sentiments = get_overall_comment_sentiment("video_comments")

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