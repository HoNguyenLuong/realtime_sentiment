import cv2
import numpy as np
from typing import Optional, List, Tuple


def detect_landmarks(face_img: np.ndarray) -> Optional[List[Tuple[int, int]]]:
    """
    Phát hiện các điểm đặc trưng (landmarks) trên ảnh khuôn mặt: mắt trái, mắt phải, mũi.

    Args:
        face_img: Ảnh khuôn mặt (NumPy array, định dạng BGR từ OpenCV).

    Returns:
        Optional[List[Tuple[int, int]]]: Danh sách 3 tọa độ (x, y) của mắt trái, mắt phải, mũi.
        Trả về None nếu thất bại.
    """
    try:
        if not isinstance(face_img, np.ndarray):
            raise ValueError("Đầu vào phải là mảng NumPy")

        # Chuyển sang grayscale
        gray = cv2.cvtColor(face_img, cv2.COLOR_BGR2GRAY)

        # Tải Haar Cascade cho mắt và mũi
        eye_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_eye.xml')
        nose_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_mcs_nose.xml')

        if eye_cascade.empty() or nose_cascade.empty():
            raise IOError("Không thể tải classifier")

        # Phát hiện mắt
        eyes = eye_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(20, 20))
        # Phát hiện mũi
        noses = nose_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(20, 20))

        landmarks = []

        # Xử lý mắt (giả định tối đa 2 mắt)
        if len(eyes) >= 2:
            # Sắp xếp mắt theo x để phân biệt trái/phải
            eyes = sorted(eyes, key=lambda x: x[0])[:2]
            # Tính tâm của mỗi mắt
            for (x, y, w, h) in eyes:
                center_x = x + w // 2
                center_y = y + h // 2
                landmarks.append((center_x, center_y))
        else:
            return None

        # Xử lý mũi (giả định 1 mũi)
        if len(noses) >= 1:
            x, y, w, h = noses[0]  # Lấy mũi đầu tiên
            center_x = x + w // 2
            center_y = y + h // 2
            landmarks.append((center_x, center_y))
        else:
            return None

        # Kiểm tra số lượng landmarks
        if len(landmarks) != 3:
            return None

        return landmarks
    except Exception as e:
        print(f"[detect_landmarks] Error: {e}")
        return None