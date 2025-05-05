#!/bin/bash

# Đặt cluster ID cho Kafka
CLUSTER_ID="b56a00fa-67dc-4c17-8c1f-b706m002b9e2"

# Format volume Kafka (đảm bảo volumes đã được tạo từ trước)
echo "Formatting Kafka volumes..."
for BROKER in 1 2 3; do
  docker run --rm \
    -v kafka${BROKER}-data:/var/lib/kafka/data \
    apache/kafka:4.0.0 \
    kafka-storage.sh format --ignore-formatted \
      --cluster-id $CLUSTER_ID \
      --config /opt/kafka/config/kraft/server.properties
done

# Sau khi format xong, chạy docker-compose lên với build
echo "Building and starting Kafka cluster with docker-compose..."
sudo docker-compose up --build
