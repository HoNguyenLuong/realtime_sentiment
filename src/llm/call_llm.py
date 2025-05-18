import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


from src.llm.openAI import call_gpt4o, load_text_file, load_api_key

def call_llm_api(prompt):
    api_key = load_api_key("OPENAI_KEY_MAIN")  # Hoặc đổi thành key khác nếu cần
    system_prompt = load_text_file("system_promt.txt")

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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, "prompt.txt")
    prompt_template = load_text_file(file_path)
    # prompt_template = load_text_file("/app/src/llm/prompt.txt")

    # Chuẩn bị dữ liệu
    text_data = "No comment data is available." if text_result is None else text_result
    audio_data = "No audio data is available." if audio_result is None else audio_result
    frame_data = "No frame data is available." if frame_result is None else frame_result

    # Tạo prompt từ template
    filled_prompt = prompt_template.format(
        text=text_data,
        audio=audio_data,
        frames=frame_data,
        num_comments=num_comments,
        num_audio_chunks=num_audio_chunks,
        num_frames=num_frames
    )

    # Gọi GPT-4o để tổng hợp kết quả
    try:
        response = call_llm_api(filled_prompt)
        return {
            "llm_response": response
        }
    except Exception as e:
        logger.error(f"Lỗi khi gọi LLM: {str(e)}")
        return {
            "error": "LLM processing failed",
            "exception": str(e)
        }

