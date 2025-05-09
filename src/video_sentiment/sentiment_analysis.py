import numpy as np
from typing import Tuple, List
from deepface import DeepFace
from .face_detection import detect_faces
from .face_alignment import align_face
from .landmark_detection import detect_landmarks

def process_emotions_deepface(face_images: List[np.ndarray]) -> List[str]:
    """
    Bước 3: Phân tích cảm xúc trên danh sách khuôn mặt, dùng DeepFace với MTCNN.

    Args:
        face_images: Danh sách mảng NumPy của các khuôn mặt.
# TODO: Thay đổi đường dẫn import cho phù hợp với dự án của bạn
    Returns:
        List[str]: Danh sách nhãn cảm xúc (ví dụ: ["happy", "sad", "error"]).
    """
    emotions = []
    for face_img in face_images:
        try:
            # Kiểm tra ảnh hợp lệ
            if face_img is None or face_img.size == 0:
                emotions.append("error")
                continue
            # Phân tích cảm xúc với DeepFace, MTCNN tự căn chỉnh
            result = DeepFace.analyze(
                face_img,
                actions=['emotion'],
                detector_backend='mtcnn',
                enforce_detection=False
            )
            emotions.append(result[0]['dominant_emotion'])
        except Exception as e:
            print(f"[process_emotions_deepface] Error processing face: {e}")
            emotions.append("error")
    return emotions


def process_emotions_integrated(face_images: List[np.ndarray]) -> List[str]:
    """
    Bước 3: Phân tích cảm xúc trên danh sách khuôn mặt, dùng OpenCV để căn chỉnh trước.

    Args:
        face_images: Danh sách mảng NumPy của các khuôn mặt.

    Returns:
        List[str]: Danh sách nhãn cảm xúc (ví dụ: ["happy", "sad", "error"]).
    """
    emotions = []
    for face_img in face_images:
        try:
            # Kiểm tra ảnh hợp lệ
            if face_img is None or face_img.size == 0:
                emotions.append("error")
                continue
            # Phát hiện landmarks (mắt trái, mắt phải, mũi)
            landmarks = detect_landmarks(face_img)
            if landmarks is None:
                emotions.append("error")
                continue
            # Căn chỉnh khuôn mặt dựa trên landmarks
            aligned_img = align_face(face_img, landmarks)
            if aligned_img is None:
                emotions.append("error")
                continue
            # Phân tích cảm xúc với DeepFace, dùng backend nhẹ vì đã căn chỉnh
            result = DeepFace.analyze(
                aligned_img,
                actions=['emotion'],
                detector_backend='opencv',
                enforce_detection=False
            )
            emotions.append(result[0]['dominant_emotion'])
        except Exception as e:
            print(f"[process_emotions_integrated] Error processing face: {e}")
            emotions.append("error")
    return emotions

def analyze_emotions(image: np.ndarray) -> Tuple[int, List[str]]:
    """
    Phân tích cảm xúc từ ảnh gốc, gộp các bước chung và gọi hàm bước 3 riêng.

    Args:
        image: Mảng NumPy của ảnh gốc (dạng BGR từ OpenCV).

    Returns:
        Tuple[int, List[str]]: (số lượng khuôn mặt, danh sách nhãn cảm xúc).
    """
    try:
        # Kiểm tra đầu vào là mảng NumPy hợp lệ
        if not isinstance(image, np.ndarray) or image.size == 0:
            print("[analyze_emotions] Invalid image input")
            return -1, ["error"]

        # Bước 2: Phát hiện khuôn mặt, trả về số khuôn mặt và danh sách ảnh
        num_faces, face_images = detect_faces(image)
        face_images = face_images[:3]  # Giới hạn xử lý tối đa 3 khuôn mặt
        if num_faces <= 0:
            print("[analyze_emotions] Step 2: No faces detected")
            return 0, []

        # Bước 3: Phân tích cảm xúc, chọn phương thức
        # emotions = process_emotions_deepface(face_images)
        emotions = process_emotions_integrated(face_images)

        return num_faces, emotions
    except Exception as e:
        print(f"[analyze_emotions] Error: {e}")
        return -1, ["error"]