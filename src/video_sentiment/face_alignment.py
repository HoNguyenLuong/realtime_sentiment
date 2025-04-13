import cv2
import numpy as np
from typing import Optional, Tuple, List


def align_face(face_img: np.ndarray, landmarks: List[Tuple[int, int]], output_size: Tuple[int, int] = (112, 112)) -> \
Optional[np.ndarray]:
    """
    Căn chỉnh khuôn mặt dựa trên landmarks để mắt và mũi ở vị trí cố định.

    Args:
        face_img: Ảnh khuôn mặt (NumPy array, định dạng BGR từ OpenCV).
        landmarks: Danh sách tọa độ (x, y) của 68 landmarks từ detect_landmarks.
        output_size: Kích thước ảnh đầu ra (width, height).

    Returns:
        Optional[np.ndarray]: Ảnh khuôn mặt đã căn chỉnh (NumPy array, định dạng BGR).
        Trả về None nếu thất bại.
    """
    try:
        if not isinstance(face_img, np.ndarray):
            raise ValueError("Đầu vào phải là mảng NumPy")
        if not landmarks or len(landmarks) !=    68:
            raise ValueError("Landmarks không hợp lệ")

        # Chọn hai điểm mắt (landmark 36 và 45 cho mắt trái và phải)
        left_eye = np.mean([landmarks[36], landmarks[39]], axis=0).astype(np.float32)
        right_eye = np.mean([landmarks[42], landmarks[45]], axis=0).astype(np.float32)

        # Tính góc xoay để căn chỉnh mắt theo đường ngang
        dY = right_eye[1] - left_eye[1]
        dX = right_eye[0] - left_eye[0]
        angle = np.degrees(np.arctan2(dY, dX))

        # Tính tâm giữa hai mắt
        eyes_center = ((left_eye[0] + right_eye[0]) / 2, (left_eye[1] + right_eye[1]) / 2)

        # Tạo ma trận xoay
        M = cv2.getRotationMatrix2D(eyes_center, angle, scale=1.0)

        # Xoay ảnh
        h, w = face_img.shape[:2]
        aligned_img = cv2.warpAffine(face_img, M, (w, h), flags=cv2.INTER_CUBIC)

        # Resize về kích thước chuẩn
        aligned_img = cv2.resize(aligned_img, output_size)

        return aligned_img
    except Exception as e:
        print(f"[align_face] Error: {e}")
        return None