FROM python:3.9-slim

# Cài các phụ thuộc hệ thống: ffmpeg, git, curl, netcat + thêm Java + procps
RUN apt-get update && \
    apt-get install -y ffmpeg libgl1-mesa-glx git curl netcat-openbsd openjdk-17-jdk procps gcc g++ && \
    apt-get clean

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Thiết lập biến môi trường cho PySpark
ENV PYSPARK_PYTHON=python3.9
ENV PYSPARK_DRIVER_PYTHON=python3.9

# Cài đặt yt-dlp
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rx /usr/local/bin/yt-dlp

# Thiết lập thư mục làm việc
WORKDIR /app

# Thiết lập PYTHONPATH để hỗ trợ imports
ENV PYTHONPATH="${PYTHONPATH}:/app:/app/src"

# Copy requirements vào container
COPY requirement.txt .

# Cài đặt các phụ thuộc Python
RUN pip install --no-cache-dir -r requirement.txt

# Copy toàn bộ project vào container
COPY . .

COPY wait-for-kafka.sh /app/wait-for-kafka.sh
RUN chmod +x /app/wait-for-kafka.sh

# Mở port
EXPOSE 8000

# Chạy ứng dụng
CMD ["/app/wait-for-kafka.sh"]