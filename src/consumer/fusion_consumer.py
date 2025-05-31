import os
import time
import json
from kafka import KafkaConsumer
from src.utils.fusion_utils import run_fusion_analysis
from src.producer.config import CONFIG
import traceback

# Kafka topics để lắng nghe
COMMENT_TOPIC = "video_comments"
VIDEO_TOPIC = "video_frames"
AUDIO_TOPIC = "audio_stream"

# Cache theo dõi dữ liệu theo URL/video_id
video_data_tracking = {}

# Cấu trúc dữ liệu cho mỗi video_id:
# {
#    "video_id": {
#        "video_complete": False,
#        "audio_complete": False,
#        "video_frames": 0,
#        "audio_chunks": 0,
#        "comments": 0,
#        "last_update": timestamp
#    }
# }

# Thời gian tối đa (giây) để đợi dữ liệu mới trước khi coi là hoàn thành
MAX_WAIT_TIME = 300  # 2 phút
# Thời gian định kỳ dọn dẹp dữ liệu cũ (giây)
CLEANUP_INTERVAL = 420  # 5 phút


def check_completed_videos():
    """Kiểm tra video nào đã hoàn thành xử lý cả video và audio"""
    current_time = time.time()
    completed_videos = []

    # Thiết lập ngưỡng số lượng tối thiểu cho mỗi loại dữ liệu
    MIN_VIDEO_FRAMES = 5  # Số lượng frame tối thiểu
    MIN_AUDIO_CHUNKS = 1  # Số lượng audio chunk tối thiểu
    MIN_COMMENTS = 0  # Số lượng comment tối thiểu (có thể 0 nếu video không có comment)

    for video_id, data in list(video_data_tracking.items()):
        time_since_update = current_time - data["last_update"]

        # Điều kiện hoàn thành:
        # 1. Dữ liệu video đã đánh dấu hoàn thành HOẶC nhận đủ số lượng frame tối thiểu
        # 2. Dữ liệu audio đã đánh dấu hoàn thành HOẶC nhận đủ số lượng chunk tối thiểu
        # 3. Đã nhận đủ số lượng comment tối thiểu
        # 4. Thời gian từ lần cập nhật cuối cùng vượt quá ngưỡng đợi

        video_condition = data["video_complete"] or (data["video_frames"] >= MIN_VIDEO_FRAMES)
        audio_condition = data["audio_complete"] or (data["audio_chunks"] >= MIN_AUDIO_CHUNKS)
        comment_condition = data["comments"] >= MIN_COMMENTS
        timeout_condition = time_since_update > MAX_WAIT_TIME

        # Một video chỉ được coi là hoàn thành khi đã nhận đủ dữ liệu từ cả 3 nguồn hoặc hết thời gian đợi
        if (video_condition and audio_condition and comment_condition) or timeout_condition:
            # Log trạng thái
            if timeout_condition and not (video_condition and audio_condition and comment_condition):
                print(f"⏱️ Đã hết thời gian đợi cho video_id: {video_id} ({time_since_update:.1f}s)")
                if not video_condition:
                    print(f"⚠️ Video stream chưa đủ dữ liệu cho video_id: {video_id} ({data['video_frames']} frames)")
                if not audio_condition:
                    print(f"⚠️ Audio stream chưa đủ dữ liệu cho video_id: {video_id} ({data['audio_chunks']} chunks)")
                if not comment_condition:
                    print(f"⚠️ Chưa đủ comments cho video_id: {video_id} ({data['comments']} comments)")
            else:
                print(
                    f"✅ Đã nhận đủ dữ liệu cho video_id: {video_id} (V:{data['video_frames']}, A:{data['audio_chunks']}, C:{data['comments']})")

            completed_videos.append(video_id)

    return completed_videos


def process_message(message):
    """Xử lý message từ Kafka và cập nhật trạng thái theo video_id"""
    try:
        # Parse nội dung message
        data = json.loads(message.value)

        # Ưu tiên lấy url_id, nếu không có thì lấy video_id
        video_id = data.get("url_id") or data.get("video_id") or data.get("content_id")

        if not video_id:
            print(f"⚠️ Message không có video identifier (topic: {message.topic}), bỏ qua")
            return None

        # Khởi tạo tracking cho video mới
        if video_id not in video_data_tracking:
            video_data_tracking[video_id] = {
                "video_complete": False,
                "audio_complete": False,
                "video_frames": 0,
                "audio_chunks": 0,
                "comments": 0,
                "last_update": time.time()
            }

        # Cập nhật bộ đếm và trạng thái theo loại message
        video_data = video_data_tracking[video_id]
        video_data["last_update"] = time.time()

        if message.topic == VIDEO_TOPIC:
            video_data["video_frames"] += 1
            # Kiểm tra nếu đây là frame cuối cùng
            if data.get("is_last_frame", False):
                video_data["video_complete"] = True
                print(f"✅ Hoàn thành video stream cho video_id: {video_id} ({video_data['video_frames']} frames)")

        elif message.topic == AUDIO_TOPIC:
            video_data["audio_chunks"] += 1
            # Kiểm tra nếu đây là audio chunk cuối cùng
            if data.get("is_last_chunk", False):
                video_data["audio_complete"] = True
                print(f"✅ Hoàn thành audio stream cho video_id: {video_id} ({video_data['audio_chunks']} chunks)")

        elif message.topic == COMMENT_TOPIC:
            video_data["comments"] += 1
            # Log mỗi 10 comments
            if video_data["comments"] % 10 == 0:
                print(f"📝 Đã nhận {video_data['comments']} comments cho video_id: {video_id}")

        return video_id

    except json.JSONDecodeError:
        print(f"⚠️ Không thể parse JSON từ message: {message.value}")
        return None
    except Exception as e:
        print(f"⚠️ Lỗi khi xử lý message: {str(e)}")
        traceback.print_exc()
        return None


def run_fusion_for_video(video_id):
    """Chạy fusion analysis cho một video_id cụ thể"""
    if video_id not in video_data_tracking:
        print(f"⚠️ Không tìm thấy dữ liệu tracking cho video_id: {video_id}")
        return

    data = video_data_tracking[video_id]
    print(f"🔄 Chạy fusion analysis cho video_id {video_id}:")
    print(f"   - Video frames: {data['video_frames']}")
    print(f"   - Audio chunks: {data['audio_chunks']}")
    print(f"   - Comments: {data['comments']}")

    # Kiểm tra xem có đủ dữ liệu từ mỗi nguồn không
    if data['video_frames'] == 0:
        print(f"⚠️ Không có dữ liệu video cho video_id: {video_id}, bỏ qua fusion analysis")
        return

    if data['audio_chunks'] == 0:
        print(f"⚠️ Không có dữ liệu audio cho video_id: {video_id}, bỏ qua fusion analysis")
        return

    try:
        # Trước khi chạy fusion, đảm bảo các nguồn dữ liệu đều được lọc theo video_id cụ thể
        print(f"📊 Chuẩn bị phân tích cảm xúc cho video_id: {video_id}")

        # Chạy fusion analysis với video_id cụ thể
        run_fusion_analysis(url_id=video_id)
        print(f"✅ Đã hoàn thành fusion analysis cho video_id: {video_id}")

        # Xóa video đã xử lý khỏi tracking để tránh xử lý lặp lại
        del video_data_tracking[video_id]

    except Exception as e:
        print(f"❌ Lỗi khi chạy fusion analysis cho {video_id}: {str(e)}")
        traceback.print_exc()


def cleanup_old_videos():
    """Xóa các video quá hạn khỏi tracking để giải phóng bộ nhớ"""
    current_time = time.time()
    expired_threshold = current_time - (MAX_WAIT_TIME * 3)  # Thời gian hết hạn lâu hơn

    for video_id in list(video_data_tracking.keys()):
        if video_data_tracking[video_id]["last_update"] < expired_threshold:
            print(
                f"🧹 Xóa video_id hết hạn: {video_id} (không hoạt động trong {(current_time - video_data_tracking[video_id]['last_update']):.1f}s)")
            del video_data_tracking[video_id]


def run():
    """Consumer lắng nghe các topic và trigger fusion khi một video đã hoàn thành xử lý"""
    print("▶️ Khởi động fusion consumer...")
    print(f"⚙️ Cấu hình: MAX_WAIT_TIME={MAX_WAIT_TIME}s, CLEANUP_INTERVAL={CLEANUP_INTERVAL}s")

    # Tạo group_id với prefix từ môi trường hoặc default
    group_id = f"{os.getenv('KAFKA_GROUP_ID', 'sentiment-group')}_fusion"

    consumer = KafkaConsumer(
        COMMENT_TOPIC, VIDEO_TOPIC, AUDIO_TOPIC,
        bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
        group_id=group_id,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8')
    )

    last_cleanup = time.time()

    try:
        print("👂 Đang lắng nghe các topic Kafka...")

        while True:
            # Poll để lấy message từ Kafka với timeout 1 giây
            message_pack = consumer.poll(timeout_ms=1000, max_records=10)

            # Xử lý các message nhận được
            for topic_partition, messages in message_pack.items():
                for message in messages:
                    # Xử lý message và lấy video_id
                    video_id = process_message(message)

            # Kiểm tra video nào đã hoàn thành cả hai luồng
            completed_videos = check_completed_videos()
            for video_id in completed_videos:
                # Chạy fusion analysis cho từng video đã hoàn thành
                run_fusion_for_video(video_id)

            # Dọn dẹp định kỳ để giải phóng bộ nhớ
            current_time = time.time()
            if current_time - last_cleanup > CLEANUP_INTERVAL:
                cleanup_old_videos()
                last_cleanup = current_time

                # Hiển thị số lượng video đang theo dõi
                if video_data_tracking:
                    print(f"📊 Đang theo dõi {len(video_data_tracking)} video_id")

    except KeyboardInterrupt:
        print("🛑 Dừng fusion consumer theo yêu cầu người dùng...")
    except Exception as e:
        print(f"❌ Lỗi không xử lý được: {str(e)}")
        traceback.print_exc()
    finally:
        print("👋 Đóng Kafka consumer...")
        consumer.close()

# import os
# import threading
# import time
# import json
# from kafka import KafkaConsumer
# from src.utils.fusion_utils import run_fusion_analysis
# from src.producer.config import CONFIG
# import traceback
#
# # Kafka topics để lắng nghe
# COMMENT_TOPIC = "video_comments"
# VIDEO_TOPIC = "video_frames"
# AUDIO_TOPIC = "audio_stream"
#
# # Cache cho theo dõi số lượng dữ liệu mới
# data_counters = {
#     "comments": 0,
#     "video_frames": 0,
#     "audio_chunks": 0
# }
#
# # Ngưỡng để kích hoạt phân tích fusion
# THRESHOLD = {
#     "comments": 0,
#     "video_frames": 5,
#     "audio_chunks": 1
# }
#
# # Flag kiểm soát trạng thái fusion
# fusion_running = False
# fusion_last_run = 0
# MIN_INTERVAL = 60  # giây
#
# def should_run_fusion():
#     """Kiểm tra điều kiện chạy fusion analysis"""
#     global fusion_last_run
#     current_time = time.time()
#     if current_time - fusion_last_run < MIN_INTERVAL:
#         return False
#
#     if (data_counters["comments"] >= THRESHOLD["comments"] and
#         data_counters["video_frames"] >= THRESHOLD["video_frames"] and
#         data_counters["audio_chunks"] >= THRESHOLD["audio_chunks"]):
#         return True
#
#     return False
#
# def reset_counters():
#     """Reset bộ đếm"""
#     global data_counters
#     data_counters = {
#         "comments": 0,
#         "video_frames": 0,
#         "audio_chunks": 0
#     }
#
# def process_message(topic):
#     """Tăng bộ đếm theo topic"""
#     if topic == COMMENT_TOPIC:
#         data_counters["comments"] += 1
#     elif topic == VIDEO_TOPIC:
#         data_counters["video_frames"] += 1
#     elif topic == AUDIO_TOPIC:
#         data_counters["audio_chunks"] += 1
#
# def run():
#     """Consumer lắng nghe các topic và trigger fusion khi đủ dữ liệu"""
#     print("▶️ Starting fusion consumer...")
#
#     group_id = f"{os.getenv('KAFKA_GROUP_ID', 'default-group')}_fusion"
#
#     consumer = KafkaConsumer(
#         COMMENT_TOPIC, VIDEO_TOPIC, AUDIO_TOPIC,
#         bootstrap_servers=CONFIG['kafka']['bootstrap_servers'],
#         group_id=group_id,
#         auto_offset_reset='latest',
#         enable_auto_commit=True,
#         value_deserializer=lambda m: m.decode('utf-8')
#     )
#
#     global fusion_running, fusion_last_run
#
#     try:
#         for message in consumer:
#             topic = message.topic
#             process_message(topic)
#
#             if not fusion_running and should_run_fusion():
#                 fusion_running = True
#                 print(f"🔄 Running fusion analysis with new data: {data_counters}")
#
#                 try:
#                     run_fusion_analysis()
#                     fusion_last_run = time.time()
#                     reset_counters()
#                 except Exception as e:
#                     print(f"❌ Error running fusion analysis: {str(e)}")
#                     traceback.print_exc()
#                 finally:
#                     fusion_running = False
#
#     except KeyboardInterrupt:
#         print("🛑 Stopping fusion consumer...")
#     finally:
#         consumer.close()
