import base64
import cv2
import numpy as np
from typing import Tuple, Optional


# ===== CONFIG =====
MIN_HEIGHT = 100
MIN_WIDTH = 100
TARGET_SIZE = (224, 224)  # Resize nếu cần


def decode_base64_to_image(b64_img: str) -> Optional[np.ndarray]:
    """
    Giải mã chuỗi base64 thành ảnh OpenCV.
    Trả về None nếu không hợp lệ.
    """
    try:
        img_data = base64.b64decode(b64_img)
        np_arr = np.frombuffer(img_data, np.uint8)
        img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        return img if img is not None else None
    except Exception as e:
        print(f"[decode_base64_to_image] Decode failed: {e}")
        return None


def is_valid_resolution(img: np.ndarray, min_height: int = MIN_HEIGHT, min_width: int = MIN_WIDTH) -> bool:
    """
    Kiểm tra xem ảnh có đạt độ phân giải tối thiểu không.
    """
    h, w = img.shape[:2]
    return h >= min_height and w >= min_width


def resize_if_needed(img: np.ndarray, target_size: Tuple[int, int] = TARGET_SIZE) -> np.ndarray:
    """
    Resize ảnh về kích thước chuẩn nếu khác với target.
    """
    h, w = img.shape[:2]
    if (h, w) != target_size:
        return cv2.resize(img, target_size)
    return img


def process_image(b64_img: str) -> Tuple[bool, str, Optional[np.ndarray]]:
    """
    Hàm tổng quát xử lý ảnh:
    - Sanity check
    - Check độ phân giải
    - Resize nếu cần
    Trả về: (is_valid, message, image)
    """
    img = decode_base64_to_image(b64_img)
    if img is None:
        return False, "Image decode failed", None

    if not is_valid_resolution(img):
        return False, f"Image too small: {img.shape}", None

    img_resized = resize_if_needed(img)
    return True, f"Image valid: {img_resized.shape}", img_resized
