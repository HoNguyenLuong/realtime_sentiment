from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField
from src.consumer.common import logger, AUDIO_TOPIC
from src.producer.config import CONFIG, minio_client
from src.audio_sentiment.openai_whisper import load_asr_pipeline, audio_to_text
from src.audio_sentiment.cardiffnlp_twitter_roberta import load_sentiment_model, analyze_sentiment
from src.audio_sentiment.emotion_recognition_wav2vec2 import load_emotion_classifier, classify_emotion
from src.producer.kafka_sender import producer
from datetime import datetime
from io import BytesIO
import json
import os

# Load models once at driver level
asr_pipe = load_asr_pipeline()
sentiment_model, sentiment_tokenizer, labels = load_sentiment_model()
emotion_classifier = load_emotion_classifier()

def run():
    try:
        # Define MinIO bucket and Kafka topic for results
        audio_results_bucket = "audio-results"
        audio_results_topic = "audio_results"

        # Ensure bucket exists
        if not minio_client.bucket_exists(audio_results_bucket):
            minio_client.make_bucket(audio_results_bucket)
            logger.info(f"Created new bucket: {audio_results_bucket}")

        # Spark session setup
        spark = SparkSession.builder \
            .appName("AudioStreamProcessor") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/audio-checkpoint") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # Kafka message schema
        schema = StructType() \
            .add("audio_id", StringType()) \
            .add("chunk_id", StringType()) \
            .add("type", StringType()) \
            .add("bucket_name", StringType()) \
            .add("object_name", StringType()) \
            .add("timestamp", StringType())

        # Read from Kafka
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", CONFIG['kafka']['bootstrap_servers']) \
            .option("subscribe", AUDIO_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON
        parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # foreachBatch for processing
        def process_audio_batch(batch_df, batch_id):
            if not batch_df.isEmpty():
                # Convert DataFrame to pandas for row-wise processing
                pandas_df = batch_df.toPandas()

                results = []
                for _, row in pandas_df.iterrows():
                    try:
                        metadata = row.to_dict()
                        bucket_name = metadata["bucket_name"]
                        object_name = metadata["object_name"]
                        chunk_id = metadata["chunk_id"]

                        # Fetch audio from MinIO
                        audio_data = minio_client.get_object(bucket_name, object_name).read()

                        # Save to temp file
                        temp_audio_path = f"/tmp/{chunk_id}_audio.wav"
                        with open(temp_audio_path, "wb") as f:
                            f.write(audio_data)

                        # Run ASR
                        text = audio_to_text(asr_pipe, temp_audio_path)

                        # Sentiment
                        sentiment_scores = analyze_sentiment(text, sentiment_model, sentiment_tokenizer, labels)

                        # Emotion
                        emotion = classify_emotion(emotion_classifier, temp_audio_path)

                        # Result object
                        result_obj = {
                            "chunk_id": chunk_id,
                            "audio_id": metadata["audio_id"],
                            "timestamp": metadata["timestamp"],
                            "text": text,
                            "sentiment": {label: score for label, score in sentiment_scores},
                            "emotion": emotion,
                            "processed_at": datetime.now().isoformat()
                        }

                        # Save to MinIO
                        result_object_name = f"{metadata['audio_id']}/{chunk_id}_audio_result.json"
                        result_json = json.dumps(result_obj)
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

                        # Add result to list for Kafka
                        results.append(result_obj)

                        # Cleanup temp file
                        os.remove(temp_audio_path)

                    except Exception as e:
                        logger.error(f"Error processing audio row: {e}")

                # Send results to Kafka
                for result in results:
                    producer.send(audio_results_topic, value=result)

                producer.flush()
                logger.info(f"Processed batch {batch_id} with {len(results)} results sent to Kafka")

        # Attach processing logic to stream
        query = parsed_df.writeStream \
            .foreachBatch(process_audio_batch) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error(f"[spark_audio.run] Error: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()