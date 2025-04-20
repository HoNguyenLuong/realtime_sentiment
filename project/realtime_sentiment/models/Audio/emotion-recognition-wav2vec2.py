# Import module đã được cập nhật
from speechbrain.inference.interfaces import foreign_class

# Tải mô hình từ Hugging Face
classifier = foreign_class(
    source="speechbrain/emotion-recognition-wav2vec2-IEMOCAP",
    pymodule_file="custom_interface.py",
    classname="CustomEncoderWav2vec2Classifier"
)


# Đường dẫn đến file audio của bạn
audio_file = "../Sample/Ses01F_impro01.wav"  # Thay bằng đường dẫn thực tế

# Dự đoán cảm xúc từ file audio
out_prob, score, index, text_lab = classifier.classify_file(audio_file)

# In kết quả dự đoán
print(f"Cảm xúc dự đoán: {text_lab}")
