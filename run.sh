#!/bin/bash

# Đặt cluster ID cố định (nên dùng uuidgen nếu cần tạo mới mỗi lần)
CLUSTER_ID="b56a00fa-67dc-4c17-8c1f-b706m002b9e2"

# Format volumes cho các Kafka brokers
echo "🔧 Formatting Kafka volumes with cluster ID: $CLUSTER_ID ..."
for BROKER in 1 2 3; do
  docker run --rm \
    -v kafka${BROKER}-data:/var/lib/kafka/data \
    apache/kafka:4.0.0 \
    kafka-storage.sh format --ignore-formatted \
      --cluster-id $CLUSTER_ID \
      --config /opt/kafka/config/kraft/server.properties
done

# Khởi động Docker Compose
echo "🚀 Building and starting services with Docker Compose..."
docker-compose up --build
