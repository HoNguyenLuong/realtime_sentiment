#!/bin/bash

echo "🔄 Waiting for Kafka to be ready at $KAFKA_SERVERS..."

# Lấy host và port từ biến môi trường
HOST=$(echo $KAFKA_SERVERS | cut -d: -f1)
PORT=$(echo $KAFKA_SERVERS | cut -d: -f2)

# Nếu không có port thì dùng mặc định 9092
if [ -z "$PORT" ]; then
  PORT=9092
fi

# Chờ đến khi có thể kết nối đến Kafka
while ! nc -z $HOST $PORT; do
  echo "⏳ Kafka ($HOST:$PORT) not ready yet..."
  sleep 2
done

echo "✅ Kafka is up! Starting app..."
exec python main.py
