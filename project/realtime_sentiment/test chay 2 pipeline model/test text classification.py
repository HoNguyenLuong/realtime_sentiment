# main_pipeline.py
# from speech_to_text import load_asr_pipeline, audio_to_text
from models.cardiffnlp_twitter_roberta import load_sentiment_model, analyze_sentiment
from models.openai_whisper import load_asr_pipeline, audio_to_text
from speechbrain.inference.interfaces import foreign_class


def main():
    # Tải các mô hình
    asr_pipe = load_asr_pipeline()
    sentiment_model, sentiment_tokenizer, labels = load_sentiment_model()

    # Xử lý audio
    audio_path = "Sample/Ses01F_impro01.wav"
    text = audio_to_text(asr_pipe, audio_path)
    print(f"Recognized Text: {text}")

    # Load emotion recognition model
    emotion_classifier = foreign_class(
        source="speechbrain/emotion-recognition-wav2vec2-IEMOCAP",
        pymodule_file="custom_interface.py",
        classname="CustomEncoderWav2vec2Classifier"
    )

    # Phân tích sentiment....
    processed_text = analyze_sentiment(text, sentiment_model, sentiment_tokenizer, labels)
    print("\nSentiment Scores:")
    for label, score in processed_text:
        print(f"  {label}: {score:.4f}")

    # Analyze emotion
    out_prob, score, index, text_lab = emotion_classifier.classify_file(audio_path)
    print(f"\nPredicted Emotion: {text_lab}")

if __name__ == "__main__":
    main()