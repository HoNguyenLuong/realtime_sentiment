import openai

def load_text_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return f.read().strip()

def load_api_key(filename):
    return load_text_file(filename)

def call_gpt4o(prompt, api_key, system_prompt=None):
    openai.api_key = api_key

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})

    messages.append({"role": "user", "content": prompt})

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o",
            messages=messages,
            temperature=0.7,
            max_tokens=1000
        )
        return response['choices'][0]['message']['content']
    except Exception as e:
        return f"Error: {str(e)}"


