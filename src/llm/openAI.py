import openai
import json
import os
import datetime

def load_text_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return f.read().strip()

def load_api_key(filename):
    return load_text_file(filename)


def call_gpt4o(prompt, api_key, system_prompt=None, filename="gpt4o_response.json"):


    openai.api_key = api_key
    messages = []

    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    messages.append({"role": "user", "content": prompt})

    try:
        # Gọi API OpenAI
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.7,
            max_tokens=1000
        )

        # In ra toàn bộ response để xem cấu trúc đầy đủ
        print("Full API Response:")
        print(json.dumps(response, indent=2, ensure_ascii=False))

        # Tự động tạo tên file với timestamp nếu không chỉ định
        if filename == "gpt4o_response.json":
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gpt4o_response_{timestamp}.json"

        # Tự động lưu response vào file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(response, f, indent=2, ensure_ascii=False)
        print(f"Response saved to {os.path.abspath(filename)}")

        # Trả về nội dung tin nhắn
        return response['choices'][0]['message']['content']

    except Exception as e:
        error_message = f"Error: {str(e)}"
        print(error_message)
        return error_message


