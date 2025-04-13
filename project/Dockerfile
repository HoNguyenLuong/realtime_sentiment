FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y ffmpeg libgl1-mesa-glx git curl netcat-openbsd && \
    apt-get clean

RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp

WORKDIR /app
COPY . .
    
    # QUAN TRỌNG: Đặt PYTHONPATH bao gồm cả thư mục gốc
ENV PYTHONPATH="${PYTHONPATH}:/app:/app/project"
    
RUN pip install --no-cache-dir -r requirements.txt
    
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]    