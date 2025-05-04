import cv2
import numpy as np
from typing import Optional, List, Tuple

def detect_landmarks(face_img: np.ndarray) -> List[Tuple[int, int]]:
    """
    Phát hiện các điểm đặc trưng (landmarks) trên ảnh khuôn mặt: mắt trái, mắt phải, mũi.
    Luôn trả về chính xác 3 điểm cho align_face, ngay cả khi cần ước tính.

    Args:
        face_img: Ảnh khuôn mặt đầu vào dạng ndarray

    Returns:
        Danh sách chính xác 3 điểm đặc trưng [(x1, y1), (x2, y2), (x3, y3)]
        Theo thứ tự: [mắt trái, mắt phải, mũi]
    """
    try:
        # Kiểm tra ảnh đầu vào
        if face_img is None or face_img.size == 0:
            # Tạo landmarks ước tính nếu không có ảnh hợp lệ
            img_h, img_w = 100, 100  # Giá trị mặc định
            print("[detect_landmarks] Ảnh không hợp lệ, tạo landmarks mặc định")
            return [(int(img_w * 0.3), int(img_h * 0.4)),
                    (int(img_w * 0.7), int(img_h * 0.4)),
                    (int(img_w * 0.5), int(img_h * 0.6))]

        img_h, img_w = face_img.shape[:2]
        gray = cv2.cvtColor(face_img, cv2.COLOR_BGR2GRAY) if len(face_img.shape) == 3 else face_img

        # Phát hiện mắt
        eye_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_eye.xml')
        left_eye_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_lefteye_2splits.xml')
        right_eye_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_righteye_2splits.xml')

        # Kiểm tra xem các bộ phát hiện có được tải thành công không
        if eye_cascade.empty():
            print("[detect_landmarks] Không thể tải eye_cascade")
        if left_eye_cascade.empty():
            print("[detect_landmarks] Không thể tải left_eye_cascade")
        if right_eye_cascade.empty():
            print("[detect_landmarks] Không thể tải right_eye_cascade")

        # Các tham số cho detectMultiScale - có thể điều chỉnh để cải thiện hiệu suất
        scale_factor = 1.1
        min_neighbors = 5
        min_size = (int(img_w * 0.05), int(img_h * 0.05))

        # Thử tất cả các bộ phát hiện
        eyes = eye_cascade.detectMultiScale(gray, scale_factor, min_neighbors, minSize=min_size)
        left_eyes = left_eye_cascade.detectMultiScale(gray, scale_factor, min_neighbors - 2,
                                                      minSize=min_size)  # Giảm minNeighbors để dễ phát hiện hơn
        right_eyes = right_eye_cascade.detectMultiScale(gray, scale_factor, min_neighbors - 2, minSize=min_size)

        # Khởi tạo vị trí các điểm đặc trưng
        left_eye_pos = None
        right_eye_pos = None
        nose_pos = None

        # Trường hợp 1: Phát hiện được cả mắt trái và mắt phải riêng biệt
        if len(left_eyes) > 0 and len(right_eyes) > 0:
            print(f"[detect_landmarks] Đã phát hiện {len(left_eyes)} mắt trái và {len(right_eyes)} mắt phải")

            # Lấy mắt trái lớn nhất
            left_eye = max(left_eyes, key=lambda x: x[2] * x[3])
            left_eye_center = (left_eye[0] + left_eye[2] // 2, left_eye[1] + left_eye[3] // 2)

            # Lấy mắt phải lớn nhất
            right_eye = max(right_eyes, key=lambda x: x[2] * x[3])
            right_eye_center = (right_eye[0] + right_eye[2] // 2, right_eye[1] + right_eye[3] // 2)

            # Đảm bảo mắt trái ở bên trái, mắt phải ở bên phải trong hình ảnh
            if left_eye_center[0] > right_eye_center[0]:
                left_eye_pos, right_eye_pos = right_eye_center, left_eye_center
                print("[detect_landmarks] Đã hoán đổi vị trí mắt trái và phải")
            else:
                left_eye_pos, right_eye_pos = left_eye_center, right_eye_center

        # Trường hợp 2: Phát hiện được ít nhất 2 mắt bằng eye_cascade chung
        elif len(eyes) >= 2:
            print(f"[detect_landmarks] Đã phát hiện {len(eyes)} mắt bằng eye_cascade chung")

            # Sắp xếp mắt theo tọa độ x
            sorted_eyes = sorted(eyes, key=lambda x: x[0])

            # Lấy hai mắt có kích thước lớn nhất
            largest_eyes = sorted(eyes, key=lambda x: x[2] * x[3], reverse=True)[:2]
            # Sắp xếp hai mắt lớn nhất theo tọa độ x
            sorted_largest_eyes = sorted(largest_eyes, key=lambda x: x[0])

            left_eye = sorted_largest_eyes[0]
            right_eye = sorted_largest_eyes[1]

            left_eye_pos = (left_eye[0] + left_eye[2] // 2, left_eye[1] + left_eye[3] // 2)
            right_eye_pos = (right_eye[0] + right_eye[2] // 2, right_eye[1] + right_eye[3] // 2)

        # Trường hợp 3: Chỉ phát hiện được 1 mắt
        elif len(eyes) == 1 or len(left_eyes) == 1 or len(right_eyes) == 1:
            print("[detect_landmarks] Chỉ phát hiện được 1 mắt")

            # Xác định mắt đã phát hiện được
            detected_eye = None
            if len(eyes) == 1:
                detected_eye = eyes[0]
            elif len(left_eyes) == 1:
                detected_eye = left_eyes[0]
            elif len(right_eyes) == 1:
                detected_eye = right_eyes[0]

            # Tính tâm của mắt đã phát hiện
            eye_center_x = detected_eye[0] + detected_eye[2] // 2
            eye_center_y = detected_eye[1] + detected_eye[3] // 2

            # Xác định mắt phát hiện được là mắt trái hay mắt phải
            if eye_center_x < img_w // 2:  # Nếu mắt ở nửa trái của ảnh
                left_eye_pos = (eye_center_x, eye_center_y)
                # Ước tính vị trí mắt phải dựa trên khoảng cách trung bình giữa hai mắt
                eye_distance = int(img_w * 0.4)  # Khoảng 40% chiều rộng khuôn mặt
                right_eye_pos = (min(eye_center_x + eye_distance, img_w - 10), eye_center_y)
                print(f"[detect_landmarks] Đã phát hiện mắt trái, ước tính mắt phải tại {right_eye_pos}")
            else:  # Nếu mắt ở nửa phải của ảnh
                right_eye_pos = (eye_center_x, eye_center_y)
                # Ước tính vị trí mắt trái
                eye_distance = int(img_w * 0.4)
                left_eye_pos = (max(eye_center_x - eye_distance, 10), eye_center_y)
                print(f"[detect_landmarks] Đã phát hiện mắt phải, ước tính mắt trái tại {left_eye_pos}")

        # Trường hợp 4: Không phát hiện được mắt nào
        else:
            print("[detect_landmarks] Không phát hiện được mắt nào, ước tính vị trí")
            # Ước tính vị trí hai mắt dựa trên kích thước ảnh
            left_eye_pos = (int(img_w * 0.3), int(img_h * 0.4))
            right_eye_pos = (int(img_w * 0.7), int(img_h * 0.4))

        # Ước tính vị trí mũi dựa trên vị trí hai mắt
        if left_eye_pos and right_eye_pos:
            # Mũi thường nằm dưới điểm giữa hai mắt
            center_x = (left_eye_pos[0] + right_eye_pos[0]) // 2
            # Mũi thường nằm dưới mắt khoảng 40% khoảng cách giữa mắt và cằm
            eye_y = (left_eye_pos[1] + right_eye_pos[1]) // 2
            nose_y = eye_y + int((img_h - eye_y) * 0.4)
            nose_pos = (center_x, nose_y)
            print(f"[detect_landmarks] Ước tính vị trí mũi tại {nose_pos}")
        else:
            # Trường hợp thiếu cả hai mắt
            nose_pos = (img_w // 2, int(img_h * 0.6))
            print(f"[detect_landmarks] Không có thông tin mắt, ước tính mũi tại {nose_pos}")

        # Đảm bảo rằng chúng ta có đủ 3 điểm để trả về
        landmarks = []

        # Thêm mắt trái (đảm bảo luôn có)
        if left_eye_pos:
            landmarks.append(left_eye_pos)
        else:
            estimated_left_eye = (int(img_w * 0.3), int(img_h * 0.4))
            landmarks.append(estimated_left_eye)
            print(f"[detect_landmarks] Không có mắt trái, ước tính tại {estimated_left_eye}")

        # Thêm mắt phải (đảm bảo luôn có)
        if right_eye_pos:
            landmarks.append(right_eye_pos)
        else:
            estimated_right_eye = (int(img_w * 0.7), int(img_h * 0.4))
            landmarks.append(estimated_right_eye)
            print(f"[detect_landmarks] Không có mắt phải, ước tính tại {estimated_right_eye}")

        # Thêm mũi (đảm bảo luôn có)
        if nose_pos:
            landmarks.append(nose_pos)
        else:
            estimated_nose = (img_w // 2, int(img_h * 0.6))
            landmarks.append(estimated_nose)
            print(f"[detect_landmarks] Không có mũi, ước tính tại {estimated_nose}")

        # Đảm bảo đúng 3 điểm đặc trưng
        assert len(landmarks) == 3, f"Phải trả về đúng 3 landmarks nhưng có {len(landmarks)}"

        return landmarks

    except Exception as e:
        print(f"[detect_landmarks] Lỗi phát hiện landmarks: {e}")
        # Trong trường hợp lỗi, trả về landmarks mặc định
        img_h, img_w = face_img.shape[:2] if face_img is not None and face_img.size > 0 else (100, 100)
        default_landmarks = [
            (int(img_w * 0.3), int(img_h * 0.4)),  # Mắt trái
            (int(img_w * 0.7), int(img_h * 0.4)),  # Mắt phải
            (int(img_w * 0.5), int(img_h * 0.6))  # Mũi
        ]
        print(f"[detect_landmarks] Trả về landmarks mặc định: {default_landmarks}")
        return default_landmarks
