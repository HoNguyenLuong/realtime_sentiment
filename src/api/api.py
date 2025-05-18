from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from src.producer.controller import process_url  # Import từ producer
from src.utils.image_utils import get_sentiment_results
from src.utils.audio_utils import get_audio_sentiment_results
from src.utils.fusion_utils import get_fusion_sentiment_results, run_fusion_analysis, get_fusion_component_results

# API routes cho ứng dụng Flask
app = Flask(__name__)
app.secret_key = "dm"

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        youtube_url = request.form.get("youtube_url")
        if youtube_url:
            try:
                process_url(youtube_url)
                flash("Processing started for the provided YouTube URL!", "success")
            except Exception as e:
                flash(f"Error processing URL: {str(e)}", "danger")
        else:
            flash("Please provide a valid YouTube URL.", "warning")
        return redirect(url_for("index"))
    return render_template("index.html")

@app.route("/get_video_sentiments", methods=["GET"])
def get_results():
    try:
        results = get_sentiment_results("emotion_results")
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_audio_results", methods=["GET"])
def get_audio_results():
    try:
        results = get_audio_sentiment_results("audio_results")
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/get_fusion_sentiment", methods=["GET"])
def get_fusion_sentiment():
    """API endpoint để lấy kết quả phân tích cảm xúc tổng hợp (fusion)"""
    try:
        result = get_fusion_sentiment_results()
        if not result:
            return jsonify({"error": "Fusion sentiment results not found. Run analysis first."}), 404
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/run_fusion_analysis", methods=["POST"])
def api_run_fusion_analysis():
    """API endpoint để chạy phân tích cảm xúc tổng hợp (fusion)"""
    try:
        fusion_result = run_fusion_analysis()
        return jsonify(fusion_result)
    except Exception as e:
        return jsonify({"error": f"Lỗi khi chạy phân tích fusion: {str(e)}"}), 500

@app.route("/api/get_fusion_components", methods=["GET"])
def get_fusion_components():
    """API endpoint để lấy kết quả thành phần chi tiết cho fusion"""
    try:
        component_results = get_fusion_component_results()
        return jsonify(component_results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500