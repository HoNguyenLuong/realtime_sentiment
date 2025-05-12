import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


from openAI import call_gpt4o, load_text_file, load_api_key

def call_llm_api(prompt):
    api_key = load_api_key("OPENAI_KEY_MAIN")  # Hoặc đổi thành key khác nếu cần
    system_prompt = load_text_file("system_promt.txt")

    return call_gpt4o(prompt, api_key, system_prompt)

def use_llm_for_sentiment(
                                    text_result: dict,
                                    audio_result: dict,
                                    frame_result: dict,
                                    num_comments: int,
                                    num_audio_chunks: int,
                                    num_frames: int
                                    ):
    """
    Tạo prompt từ dữ liệu và gọi GPT-4o để tổng hợp sentiment cuối cùng.

    Returns:
        dict: Kết quả phân tích từ LLM
    """
    prompt_template = load_text_file("prompt.txt")

    # 1. Tạo prompt từ template
    filled_prompt = prompt_template.format(
        # text=text_result,
        audio=audio_result,
        frames=frame_result,
        # num_comments=num_comments,
        num_audio_chunks=num_audio_chunks,
        num_frames=num_frames
    )

    # 2. Gọi GPT-4o để tổng hợp kết quả
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

