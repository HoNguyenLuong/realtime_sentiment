#!/bin/bash

# Sử dụng KAFKA_BOOTSTRAP_SERVERS thay vì KAFKA_SERVERS
KAFKA_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}

echo "🔄 Waiting for Kafka to be ready at $KAFKA_SERVERS..."

HOST=$(echo "$KAFKA_SERVERS" | cut -d: -f1)
PORT=$(echo "$KAFKA_SERVERS" | cut -d: -f2)

# Mặc định port nếu thiếu
if [ -z "$PORT" ]; then
  PORT=9092
fi

# Debug thêm:
echo "📡 Host: $HOST | Port: $PORT"

while ! nc -z "$HOST" "$PORT"; do
  echo "⏳ Kafka ($HOST:$PORT) not ready yet..."
  sleep 2
done

echo "✅ Kafka is up! Starting app..."
exec uvicorn main:app --host 0.0.0.0 --port 8000

