import os
import chardet
import re

def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        raw_data = f.read()
    result = chardet.detect(raw_data)
    return result["encoding"]

def clean_and_convert_to_utf8(file_path):
    try:
        encoding = detect_encoding(file_path)
        with open(file_path, "r", encoding=encoding, errors="ignore") as f:
            content = f.read()

        # Loại bỏ BOM hoặc ký tự không hợp lệ như U+0A0D
        content = content.replace('\u0A0D', '\n')
        content = re.sub(r'[^\x09\x0A\x0D\x20-\x7E\u00A0-\uFFFF]', '', content)

        # Chuẩn hóa newline về LF
        content = content.replace('\r\n', '\n').replace('\r', '\n')

        with open(file_path, "w", encoding="utf-8", newline='\n') as f:
            f.write(content)

        print(f"[✓] Cleaned & converted {file_path} to UTF-8.")
    except Exception as e:
        print(f"[✗] Failed to convert {file_path}: {e}")

def convert_all_init_files(root_dir):
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file == "__init__.py":
                file_path = os.path.join(root, file)
                clean_and_convert_to_utf8(file_path)


