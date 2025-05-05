import librosa
import soundfile as sf
import os
import tempfile
from src.audio_sentiment.cardiffnlp_twitter_roberta import load_sentiment_model, analyze_sentiment
from src.audio_sentiment.openai_whisper import load_asr_pipeline, audio_to_text
from src.audio_sentiment.emotion_recognition_wav2vec2 import load_emotion_classifier, classify_emotion

def split_audio(audio_path, chunk_length):
    """
    Split audio file into chunks of specified length (in seconds).
    Returns a list of temporary audio file paths for each chunk.
    """
    y, sr = librosa.load(audio_path, sr=None)
    chunk_samples = int(chunk_length * sr)
    chunks = []
    temp_files = []

    for start in range(0, len(y), chunk_samples):
        end = min(start + chunk_samples, len(y))
        chunk = y[start:end]

        # Create temporary file for the chunk
        temp_fd, temp_path = tempfile.mkstemp(suffix=".wav")
        os.close(temp_fd)
        sf.write(temp_path, chunk, sr)
        chunks.append(temp_path)
        temp_files.append(temp_path)

    return chunks, temp_files

def main():
    # Load models
    asr_pipe = load_asr_pipeline()
    sentiment_model, sentiment_tokenizer, labels = load_sentiment_model()
    emotion_classifier = load_emotion_classifier()

    # Input audio file
    audio_path = "Sample/Ses01F_impro01.wav"
    chunk_length = 30  # Set chunk length to 60 seconds

    # Split audio into chunks
    chunk_files, temp_files = split_audio(audio_path, chunk_length=chunk_length)

    # Process each chunk
    results = []
    for i, chunk_path in enumerate(chunk_files):
        print(f"\nProcessing chunk {i + 1} (seconds {i * chunk_length} to {(i + 1) * chunk_length})")

        # Pipeline 1: Audio -> Text -> Sentiment
        text = audio_to_text(asr_pipe, chunk_path)
        print(f"  Recognized Text: {text}")

        sentiment_scores = analyze_sentiment(text, sentiment_model, sentiment_tokenizer, labels)
        print("  Sentiment Scores:")
        for label, score in sentiment_scores:
            print(f"    {label}: {score:.4f}")

        # Pipeline 2: Audio -> Emotion
        predicted_emotion = classify_emotion(emotion_classifier, chunk_path)
        print(f"  Predicted Emotion: {predicted_emotion}")

        # Store results
        results.append({
            "chunk": i + 1,
            "start_time": i * chunk_length,
            "end_time": (i + 1) * chunk_length,
            "text": text,
            "sentiment": {label: score for label, score in sentiment_scores},
            "emotion": predicted_emotion
        })

    # Clean up temporary files
    for temp_file in temp_files:
        os.remove(temp_file)

    # Print summary
    print("\nSummary of Results:")
    for result in results:
        print(f"\nChunk {result['chunk']} ({result['start_time']}s - {result['end_time']}s):")
        print(f"  Text: {result['text']}")
        print(f"  Sentiment: {result['sentiment']}")
        print(f"  Emotion: {result['emotion']}")

if __name__ == "__main__":
    main()