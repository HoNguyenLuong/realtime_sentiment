import logging
import os
import json
from src.llm.openAI import call_gpt4o, load_text_file
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_api_key(key_file_name):
    """
    Đọc API key từ biến môi trường hoặc file.

    Args:
        key_file_name: Tên biến môi trường hoặc tên file chứa API key

    Returns:
        str: API key dạng chuỗi
    """
    # Bước 1: Đọc từ biến môi trường trước tiên (theo cách 2)
    env_key = os.environ.get(key_file_name)
    if env_key:
        logger.info(f"Sử dụng API key từ biến môi trường {key_file_name}")
        return env_key

    # Bước 2: Nếu không có trong môi trường, thử đọc từ file (giữ nguyên code gốc)
    try:
        # Đường dẫn tương đối từ thư mục gốc dự án
        base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        key_path = os.path.join(base_path, key_file_name)

        # Nếu không tìm thấy ở đường dẫn gốc, thử tìm trong thư mục hiện tại
        if not os.path.exists(key_path):
            key_path = key_file_name

        # Thử mở file để đọc API key
        if os.path.exists(key_path):
            with open(key_path, 'r') as file:
                api_key = file.read().strip()
                logger.info(f"Sử dụng API key từ file {key_path}")
                return api_key
        else:
            raise FileNotFoundError(f"File {key_path} không tồn tại")

    except Exception as e:
        # Ghi log chi tiết về lỗi
        logger.error(f"Không thể đọc API key từ file {key_file_name}: {str(e)}")
        logger.error(f"Đường dẫn thử nghiệm: {key_path}")
        logger.error(f"Thư mục hiện tại: {os.getcwd()}")
        logger.error(f"Vui lòng thiết lập biến môi trường {key_file_name} hoặc tạo file chứa API key")
        raise

def call_llm_api(prompt):
    api_key = load_api_key("OPENAI_KEY_MAIN")  # Hoặc đổi thành key khác nếu cần
    current_dir = os.path.dirname(os.path.abspath(__file__))
    system_prompt = os.path.join(current_dir, "system_promt.txt")

    return call_gpt4o(prompt, api_key, system_prompt)


def use_llm_for_sentiment(
        text_result=None,
        audio_result=None,
        frame_result=None,
        num_comments=0,
        num_audio_chunks=0,
        num_frames=0
):
    """
    Tạo prompt từ dữ liệu và gọi GPT-4o để tổng hợp sentiment cuối cùng.

    Returns:
        dict: Kết quả phân tích từ LLM
    """
    try:
        # Tạo prompt từ các phần riêng biệt thay vì dùng template file
        # Phần 1: Giới thiệu và dữ liệu đầu vào
        intro = """You are performing sentiment analysis for a YouTube video using three sources of data: comments (text), audio, and video frames.

Each source has undergone individual sentiment analysis, and the summarized results are provided below:

1. **Text (YouTube Comments)**:
{text}

2. **Audio (Extracted from video)**:
{audio}

3. **Video Frames (Face/emotion detection)**:
{frames}

---

**Your Task:**

1. Combine all available sources (text, audio, frames) to determine the overall sentiment of the video.
2. If any source is missing, explain how the final result is affected and base your decision on the sources available.
3. The **sentiment score** must be a float between `-1` and `1`, where:
   - `-1` = strongly negative,
   - `0` = neutral,
   - `1` = strongly positive.
4. The **sentiment label** must be one of: `"positive"`, `"neutral"`, `"negative"`.
5. Provide a **confidence score** (0 to 1).
6. In your **description**, explain:
   - Which signals dominate (audio, text, or frames)
   - What emotions were most frequent
   - How many comments, frames, and audio chunks were analyzed ({num_comments} comments, {num_frames} frames, and {num_audio_chunks} audio chunks)
   - Why the final sentiment makes sense

---

**Your response must be in the following strict JSON format**:"""

        # Phần 2: Ví dụ JSON được tạo tách biệt
        # Tạo mô tả ví dụ có chứa các biến đã được thay thế
        description_example = f"Out of {num_frames} analyzed frames and {num_audio_chunks} audio chunks, the dominant emotion was happiness and joy. Although no comment data was available, the visual and audio cues strongly indicate a positive experience. The analysis shows consistent smiling faces and excited tones in speech, suggesting the video content was positively received."

        # Tạo ví dụ JSON không sử dụng format string để tránh lỗi
        json_example = {
            "sentiment_score": 0.35,
            "sentiment_label": "positive",
            "confidence": 0.87,
            "description": description_example
        }

        # Chuyển đổi đối tượng JSON thành chuỗi có định dạng đẹp
        json_example_str = json.dumps(json_example, indent=2)

        # Gói trong cặp dấu ```json và ```
        json_example_formatted = f"```json\n{json_example_str}\n```"

        # Chuẩn bị dữ liệu
        text_data = "No comment data is available." if text_result is None else text_result
        audio_data = "No audio data is available." if audio_result is None else audio_result
        frame_data = "No frame data is available." if frame_result is None else frame_result

        # Kết hợp mọi thứ lại với nhau và thay thế các biến
        prompt = intro.format(
            text=text_data,
            audio=audio_data,
            frames=frame_data,
            num_comments=num_comments,
            num_audio_chunks=num_audio_chunks,
            num_frames=num_frames
        ) + "\n\n" + json_example_formatted

        # Gọi GPT-4o để tổng hợp kết quả
        response = call_llm_api(prompt)
        return {
            "llm_response": response
        }
    except Exception as e:
        logger.error(f"Lỗi khi format prompt hoặc gọi LLM: {str(e)}")
        return {
            "error": "LLM processing failed",
            "exception": str(e)
        }
# import logging
# import os
#
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)
#
#
# from src.llm.openAI import call_gpt4o, load_text_file, load_api_key
#
# def call_llm_api(prompt):
#     api_key = load_api_key("OPENAI_KEY_MAIN")  # Hoặc đổi thành key khác nếu cần
#     system_prompt = load_text_file("system_promt.txt")
#
#     return call_gpt4o(prompt, api_key, system_prompt)
#
# def use_llm_for_sentiment(
#                                     text_result=None,
#                                     audio_result=None,
#                                     frame_result=None,
#                                     num_comments=0,
#                                     num_audio_chunks=0,
#                                     num_frames=0
#                                     ):
#     """
#     Tạo prompt từ dữ liệu và gọi GPT-4o để tổng hợp sentiment cuối cùng.
#
#     Returns:
#         dict: Kết quả phân tích từ LLM
#     """
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     file_path = os.path.join(current_dir, "prompt.txt")
#     prompt_template = load_text_file(file_path)
#     # prompt_template = load_text_file("/app/src/llm/prompt.txt")
#
#     # Chuẩn bị dữ liệu
#     text_data = "No comment data is available." if text_result is None else text_result
#     audio_data = "No audio data is available." if audio_result is None else audio_result
#     frame_data = "No frame data is available." if frame_result is None else frame_result
#
#     # Tạo prompt từ template
#     filled_prompt = prompt_template.format(
#         text=text_data,
#         audio=audio_data,
#         frames=frame_data,
#         num_comments=num_comments,
#         num_audio_chunks=num_audio_chunks,
#         num_frames=num_frames
#     )
#
#     # Gọi GPT-4o để tổng hợp kết quả
#     try:
#         response = call_llm_api(filled_prompt)
#         return {
#             "llm_response": response
#         }
#     except Exception as e:
#         logger.error(f"Lỗi khi gọi LLM: {str(e)}")
#         return {
#             "error": "LLM processing failed",
#             "exception": str(e)
#         }