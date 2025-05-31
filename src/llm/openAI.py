import openai
import json
import os
import datetime
import logging

logger = logging.getLogger(__name__)


def load_text_file(filename):
    """
    Đọc nội dung từ file văn bản.

    Args:
        filename: Đường dẫn đến file cần đọc

    Returns:
        str: Nội dung file dạng văn bản
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return f.read().strip()
    except Exception as e:
        logger.error(f"Lỗi khi đọc file {filename}: {str(e)}")
        raise


def load_api_key(key_file_name):
    """
    Đọc API key từ biến môi trường hoặc file.

    Args:
        key_file_name: Tên biến môi trường hoặc tên file chứa API key

    Returns:
        str: API key dạng chuỗi
    """
    # Bước 1: Đọc từ biến môi trường trước tiên
    env_key = os.environ.get(key_file_name)
    if env_key:
        logger.info(f"Sử dụng API key từ biến môi trường {key_file_name}")
        return env_key

    # Bước 2: Nếu không có trong môi trường, thử đọc từ file
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


def call_gpt4o(prompt, api_key, system_prompt=None, filename="gpt4o_response.json"):
    """
    Gọi API OpenAI với GPT-4o (phiên bản mới của OpenAI API >=1.10.0)

    Args:
        prompt: Nội dung prompt gửi đến API
        api_key: OpenAI API key
        system_prompt: Nội dung system prompt (optional)
        filename: Tên file để lưu response (optional)

    Returns:
        str: Nội dung phản hồi từ API
    """
    # Tạo client với API key
    client = openai.OpenAI(api_key=api_key)
    messages = []

    if system_prompt:
        # Nếu system_prompt là đường dẫn file, đọc nội dung từ file
        if os.path.isfile(system_prompt):
            try:
                system_content = load_text_file(system_prompt)
            except Exception as e:
                logger.warning(f"Không thể đọc file system prompt: {str(e)}")
                system_content = system_prompt
        else:
            system_content = system_prompt

        messages.append({"role": "system", "content": system_content})

    messages.append({"role": "user", "content": prompt})

    try:
        # Gọi API OpenAI với cú pháp mới
        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=messages,
            temperature=0.7,
            max_tokens=1000
        )

        # Lấy nội dung phản hồi
        response_content = response.choices[0].message.content

        # Tự động tạo tên file với timestamp nếu không chỉ định
        if filename == "gpt4o_response.json":
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gpt4o_response_{timestamp}.json"

        # Chuyển đổi response thành JSON để lưu
        # Lưu ý: phiên bản mới của API trả về object, không phải dict như phiên bản cũ
        # Nên cần chuyển đổi thành dict trước khi lưu
        response_dict = {
            "id": response.id,
            "model": response.model,
            "object": response.object,
            "created": response.created,
            "choices": [
                {
                    "index": choice.index,
                    "message": {
                        "role": choice.message.role,
                        "content": choice.message.content
                    },
                    "finish_reason": choice.finish_reason
                } for choice in response.choices
            ],
            "usage": {
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens
            }
        }

        # # Tự động lưu response vào file
        # with open(filename, 'w', encoding='utf-8') as f:
        #     json.dump(response_dict, f, indent=2, ensure_ascii=False)
        # logger.info(f"Response saved to {os.path.abspath(filename)}")

        # Trả về nội dung tin nhắn
        return response_content

    except Exception as e:
        error_message = f"Error: {str(e)}"
        logger.error(error_message)
        return error_message