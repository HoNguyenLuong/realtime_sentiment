
from project.realtime_sentiment.models.text_classification.cardiffnlp_twitter_roberta import load_sentiment_model, analyze_sentiment
from project.realtime_sentiment.models.text_classification.openai_whisper import load_asr_pipeline, audio_to_text

def main():
    # Tải các mô hình
    asr_pipe = load_asr_pipeline()
    sentiment_model, sentiment_tokenizer, labels = load_sentiment_model()

    # Xử lý audio
    audio_path = "Sample/angry_037.wav"
    text = audio_to_text(asr_pipe, audio_path)
    print(f"Recognized Text: {text}")

    # Phân tích sentiment
    label, confidence = analyze_sentiment(text, sentiment_model, sentiment_tokenizer, labels)
    print(f"\nSentiment: {label} (Confidence: {confidence:.4f})")


if __name__ == "__main__":
    main()