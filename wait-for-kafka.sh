#!/bin/bash

# Sử dụng KAFKA_BOOTSTRAP_SERVERS từ biến môi trường
KAFKA_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka1:9092,kafka2:9094,kafka3:9096}

echo "🔄 Waiting for Kafka brokers to be ready..."

# Phân tách chuỗi các server bằng dấu phẩy
IFS=',' read -ra SERVERS <<< "$KAFKA_SERVERS"

# Kiểm tra từng server một
for SERVER in "${SERVERS[@]}"; do
  # Phân tách host và port
  HOST=$(echo "$SERVER" | cut -d: -f1)
  PORT=$(echo "$SERVER" | cut -d: -f2)

  # Mặc định port nếu thiếu
  if [ -z "$PORT" ]; then
    PORT=9092
  fi

  echo "📡 Checking Kafka broker: $HOST:$PORT"

  # Đợi cho đến khi kết nối được
  CONNECTED=false
  RETRY_COUNT=0
  MAX_RETRIES=30

  while [ $RETRY_COUNT -lt $MAX_RETRIES ] && [ "$CONNECTED" = false ]; do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then
      echo "✅ Kafka broker $HOST:$PORT is ready!"
      CONNECTED=true
    else
      echo "⏳ Kafka broker $HOST:$PORT not ready yet... (Attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"
      RETRY_COUNT=$((RETRY_COUNT+1))
      sleep 2
    fi
  done

  if [ "$CONNECTED" = false ]; then
    echo "❌ Failed to connect to Kafka broker $HOST:$PORT after $MAX_RETRIES attempts"
    echo "⚠️ Continuing anyway, will try to connect to other brokers if available..."
  fi
done

echo "✅ Kafka connection check completed! Starting application..."
exec uvicorn main:app --host 0.0.0.0 --port 8000