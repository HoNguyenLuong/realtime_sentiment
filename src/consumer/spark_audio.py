from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField, ArrayType
from src.consumer.common import logger, AUDIO_TOPIC
from src.producer.config import CONFIG, minio_client
from src.audio_sentiment.openai_whisper import load_asr_pipeline, audio_to_text
from src.audio_sentiment.cardiffnlp_twitter_roberta import load_sentiment_model, analyze_sentiment
from src.audio_sentiment.emotion_recognition_wav2vec2 import load_emotion_classifier, classify_emotion
from src.producer.kafka_sender import producer, send_metadata
from datetime import datetime
from io import BytesIO
import json
import os
import numpy as np

# Load models once at driver level
asr_pipe = load_asr_pipeline()
sentiment_model, sentiment_tokenizer, labels = load_sentiment_model()
emotion_classifier = load_emotion_classifier()


def run():
    try:
        # Định nghĩa tên topic cho kết quả xử lý audio
        audio_results_topic = "audio_results"

        # Cấu hình bucket MinIO để lưu kết quả audio
        audio_results_bucket = "audio-results"

        # Đảm bảo bucket tồn tại
        if not minio_client.bucket_exists(audio_results_bucket):
            minio_client.make_bucket(audio_results_bucket)
            logger.info(f"Created new bucket: {audio_results_bucket}")

        # Khởi tạo Spark session kết nối với Spark master bên ngoài
        spark = SparkSession.builder \
            .appName("AudioStreamProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/audio-checkpoint") \
            .config("spark.driver.extraJavaOptions", "-Dkafka.bootstrap.servers=kafka:9092") \
            .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
            .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Schema dữ liệu Kafka gửi tới - Cập nhật theo send_audio_file
        schema = StructType() \
            .add("video_id", StringType()) \
            .add("chunk_id", StringType()) \
            .add("type", StringType()) \
            .add("format", StringType()) \
            .add("bucket_name", StringType()) \
            .add("object_name", StringType()) \
            .add("timestamp", StringType())

        # Đọc từ Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['kafka']['bootstrap_servers']) \
            .option("subscribe", AUDIO_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

# Giải mã và parse JSON
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # Sử dụng foreachBatch để xử lý từng batch
        def process_audio_batch(batch_df, batch_id):
            if not batch_df.isEmpty():
                # Convert DataFrame to pandas để xử lý từng hàng
                pandas_df = batch_df.toPandas()

                # Xử lý từng hàng
                results = []
                for _, row in pandas_df.iterrows():
                    try:
                        if hasattr(row, 'asDict'):
                            metadata = row.asDict()
                        else:
                            metadata = row.to_dict()

                        bucket_name = metadata["bucket_name"]
                        object_name = metadata["object_name"]
                        chunk_id = metadata["chunk_id"]
                        video_id = metadata["video_id"]  # Sử dụng video_id thay cho audio_id

                        # Fetch audio from MinIO
                        audio_data = minio_client.get_object(bucket_name, object_name).read()

                        # Save to temp file
                        temp_audio_path = f"/tmp/{chunk_id}_audio.wav"
                        with open(temp_audio_path, "wb") as f:
                            f.write(audio_data)

                        # Run ASR
                        text = audio_to_text(asr_pipe, temp_audio_path)

                        # Sentiment - Chuyển đổi numpy types thành Python types
                        sentiment_scores = analyze_sentiment(text, sentiment_model, sentiment_tokenizer, labels)

                        # Chuyển đổi điểm sentiment thành kiểu dữ liệu Python chuẩn
                        sentiment_dict = {}
                        for label, score in sentiment_scores:
                            # Chuyển đổi numpy types sang Python native types
                            if hasattr(score, "item"):  # Kiểm tra nếu là numpy type có phương thức item()
                                sentiment_dict[label] = score.item()
                            else:
                                sentiment_dict[label] = score

                        # Emotion - Đảm bảo chuyển đổi tất cả giá trị numpy
                        emotion_result = classify_emotion(emotion_classifier, temp_audio_path)

                        # Đảm bảo emotion có dạng Python types tiêu chuẩn
                        if isinstance(emotion_result, dict):
                            emotion = {}
                            for k, v in emotion_result.items():
                                if hasattr(v, "item"):  # Kiểm tra nếu là numpy type
                                    emotion[k] = v.item()
                                else:
                                    emotion[k] = v
                        elif hasattr(emotion_result, "item"):
                            emotion = emotion_result.item()
                        else:
                            emotion = emotion_result

                        # Tạo đối tượng kết quả với các kiểu dữ liệu Python chuẩn
                        result_obj = {
                            "chunk_id": chunk_id,
                            "video_id": video_id,  # Thay đổi từ audio_id sang video_id
                            "timestamp": metadata["timestamp"],
                            "bucket_name": metadata["bucket_name"],
                            "object_name": metadata["object_name"],
                            "text": text,
                            "sentiment": sentiment_dict,
                            "emotion": emotion,
                            "processed_at": datetime.now().isoformat()
                        }

                        # Kiểm tra và chuyển đổi lần cuối để đảm bảo không còn numpy types
                        result_obj_serializable = convert_numpy_to_python(result_obj)

                        results.append(result_obj_serializable)

                        # Lưu kết quả vào MinIO
                        try:
                            # Tạo tên đối tượng dựa trên video_id và chunk_id
                            result_object_name = f"{video_id}/{chunk_id}_audio_result.json"

                            # Chuyển đổi kết quả thành JSON string
                            result_json = json.dumps(result_obj_serializable)
                            result_bytes = result_json.encode("utf-8")
                            result_stream = BytesIO(result_bytes)

                            minio_client.put_object(
                                bucket_name=audio_results_bucket,
                                object_name=result_object_name,
                                data=result_stream,
                                length=len(result_bytes),
                                content_type="application/json"
                            )

                            logger.info(f"Saved audio result to MinIO: {audio_results_bucket}/{result_object_name}")
                        except Exception as e:
                            logger.error(f"Error saving to MinIO: {e}")

                        # Cleanup temp file
                        os.remove(temp_audio_path)

                    except Exception as e:
                        logger.error(f"Error processing audio row: {e}")

                # Tạo Schema kết quả và gửi ra Kafka
                if results:
                    # Gửi kết quả đến Kafka sử dụng send_metadata
                    for result in results:
                        send_metadata(audio_results_topic, result)

                    logger.info(
                        f"Successfully processed batch {batch_id} and sent {len(results)} results to topic {audio_results_topic}")

        # Hàm đệ quy để chuyển đổi tất cả numpy types trong đối tượng phức tạp
        def convert_numpy_to_python(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy_to_python(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_to_python(item) for item in obj]
            elif isinstance(obj, (np.integer, np.int64, np.int32)):
                return int(obj)
            elif isinstance(obj, (np.floating, np.float32, np.float64)):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif hasattr(obj, 'item'):  # Bắt tất cả các loại numpy scalar còn lại
                return obj.item()
            else:
                return obj

        # Attach processing logic to stream
        query = parsed_df.writeStream \
            .foreachBatch(process_audio_batch) \
            .start()

        # Chờ stream kết thúc
        query.awaitTermination()

    except Exception as e:
        logger.error(f"[spark_audio.run] Error: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()