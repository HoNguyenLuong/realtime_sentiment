# Real-time Sentiment Analysis of Social Media Data
![Screenshot from 2025-05-31 20-56-58](https://github.com/user-attachments/assets/7c07a95c-9be5-4eee-a605-75b100fa9c82)
![Screenshot from 2025-05-31 20-58-13](https://github.com/user-attachments/assets/91b65b80-9244-45ea-9be4-6f76daa2bf0b)
![Screenshot from 2025-05-31 21-03-16](https://github.com/user-attachments/assets/816df303-d791-478b-b5df-c2863909afd7)


## 📌 Introduction
This project builds a real-time, multimodal sentiment analysis system that processes data from social media platforms such as YouTube (TikTok). Unlike traditional sentiment analysis methods that focus solely on text, this system integrates visual, audio, and textual data to provide comprehensive sentiment evaluation. The results are stored and visualized on a real-time dashboard for immediate insight into public sentiment trends.

## 🎯 Features
- 📥**Multimodal Data Collection from YouTube**: videos, audio, and comments.
- 🎞️**Visual Sentiment Analysis** using face detection and emotion recognition (DeepFace).
- 🔊**Audio Emotion Recognition** using Wav2Vec2 and Whisper + RoBERTa.
- 💬**Text Sentiment Analysis** from user comments with emoji and language model fusion.
- 🧠**Cross-modal Label Fusion** using LLM (e.g., GPT).
- 📊**Real-time Dashboard** for sentiment monitoring.
- ☁️**Scalable Big Data Architecture** using Kafka, Spark, and MinIO.

## 🛠️ System Architecture

![image](https://github.com/user-attachments/assets/e1118e67-7e3b-4658-8b05-b5e48f95e3db)

1. **Data Source**: YouTube.
2. **Data Ingestion**: yt-dlp, FFmpeg, Kafka.
3. **Data Transformation**: Apache Spark for parallel processing.
4. **Data Storage & Visuallization**: MinIO, real-time dashboard.

## 🚀 Technologies Used

| Component        | Description |
|------------------|-------------|
| `yt-dlp`         | Download videos, audio, and comments from YouTube. |
| `FFmpeg`         | Extract frames and audio chunks from video streams. |
| `Apache Kafka`   | Stream multimodal data to processing layers. |
| `Apache Spark`   | Real-time processing and ML-based sentiment classification. |
| `MinIO`          | Distributed object storage. |
| `DeepFace`       | Facial emotion recognition from video frames. |
| `Whisper`        | Transcribe audio to text. |
| `RoBERTa`        | Sentiment classification on text and transcribed speech. |
| `Wav2Vec2`       | Audio-based emotion recognition. |

## 🧪 Modal-Specific Analysis

- **Visual**:
  - HaarCascade face detection
  - Face alignment
  - Emotion recognition using DeepFace (FER2013 dataset)

- **Audio**:
  - Whisper (speech-to-text) + RoBERTa for sentiment
  - Wav2Vec2 for raw audio emotion classification
  
- **Text**:
  - Emoji-based emotion pre-analysis
  - Sentiment classification via RoBERTa (twitter-roberta-base-sentiment-latest)

- **Fusion**:
  - Unified sentiment label via LLM (e.g., GPT-based fusion strategy)
  - Confidence scoring
 
## Get Started
> Ensure Docker are installed.
### 1. Clone repository
```bash
git clone https://github.com/HoNguyenLuong/realtime_sentiment.git
```
### 2. Configure environment variables
> You must have a LLM key if you want to have results of fusion processing.
```bash
cd realtime_sentiment
echo "OPENAI_KEY_MAIN=sk-<your-key>" > .env
```
### 3. Run project
Don't worry If you don't have a LLM key, we can run project without fusion processing by this command.
```bash
docker compose up --build
```
> You need to wait about ten minuites or more for it to load the necessary settings. The model taken from HuggingFace are quite heavy so you need to wait patiently and make sure your network is stable.
> You need to pay attention to your Docker version in order to use suitable command.
> If you get an error about "env", you can try removing line 83, 84 in docker-compose.yml .

## 📄 Document

## 👥 Authors
- Hồ Nguyên Lượng
- Lê Hồng Triệu
- Nguyễn Duy Anh
