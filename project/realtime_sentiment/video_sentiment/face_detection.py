import base64
import cv2
import numpy as np


def detect_faces(image):
    # Đảm bảo đầu vào là mảng NumPy
    if not isinstance(image, np.ndarray):
        raise ValueError("Đầu vào phải là mảng NumPy")

    # Chuyển ảnh sang thang độ xám vì Haar Cascade hoạt động trên ảnh xám
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Tải pre-trained Haar Cascade classifier cho phát hiện khuôn mặt
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

    # Kiểm tra xem classifier có tải thành công hay không
    if face_cascade.empty():
        raise IOError("Không thể tải classifier phát hiện khuôn mặt")

    # Phát hiện khuôn mặt trong ảnh xám
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)

    # Trả về danh sách các khuôn mặt phát hiện được (mỗi khuôn mặt là một tuple (x, y, w, h))
    return faces
