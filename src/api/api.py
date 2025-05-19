from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from src.producer.controller import process_url  # Import từ producer
from src.utils.image_utils import get_sentiment_results
from src.utils.audio_utils import get_audio_sentiment_results
from src.utils.fusion_utils import get_fusion_sentiment_results, run_fusion_analysis, get_fusion_component_results
from src.utils.comment_utils import get_sentiment_results as get_comment_sentiment_results
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
        # Get video_id from query parameters if available
        video_id = request.args.get('video_id')

        result = get_fusion_sentiment_results(video_id=video_id)
        if not result:
            return jsonify({"error": "Fusion sentiment results not found. Run analysis first."}), 404
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/run_fusion_analysis", methods=["POST"])
def api_run_fusion_analysis():
    """API endpoint để chạy phân tích cảm xúc tổng hợp (fusion)"""
    try:
        # Get video_id from request data
        data = request.get_json(silent=True) or {}
        video_id = data.get('video_id')

        fusion_result = run_fusion_analysis(url_id=video_id)
        return jsonify(fusion_result)
    except Exception as e:
        return jsonify({"error": f"Lỗi khi chạy phân tích fusion: {str(e)}"}), 500


@app.route("/api/get_fusion_components", methods=["GET"])
def get_fusion_components():
    """API endpoint để lấy kết quả thành phần chi tiết cho fusion"""
    try:
        # Get video_id from query parameters if available
        video_id = request.args.get('video_id')

        # If your get_fusion_component_results doesn't yet support video_id,
        # you might need to modify that function too
        component_results = get_fusion_component_results(video_id=video_id)
        return jsonify(component_results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get_comment_sentiments", methods=["GET"])
def get_comment_sentiments():
    try:
        video_id = request.args.get('video_id')
        results = get_comment_sentiment_results("comment_sentiment_results")

        # Lọc theo video_id nếu có
        if video_id:
            results = [r for r in results if r.get("content_id") == video_id]

        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/comment_summary", methods=["GET"])
def get_comment_summary():
    try:
        video_id = request.args.get('video_id')
        results = get_comment_sentiment_results("comment_sentiment_results")

        # Lọc theo video_id nếu có
        if video_id:
            results = [r for r in results if r.get("content_id") == video_id]

        # Tạo tổng hợp
        summary = {"positive": 0, "negative": 0, "neutral": 0}
        languages = {}

        for result in results:
            sentiment = result.get("sentiment", "neutral")
            if sentiment in summary:
                summary[sentiment] += 1

            # Ghi nhận ngôn ngữ
            language = result.get("language", "unknown")
            if language in languages:
                languages[language] += 1
            else:
                languages[language] = 1

        return jsonify({
            "sentiment_summary": summary,
            "languages": languages,
            "total_comments": len(results)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/dashboard/comments", methods=["GET"])
def comment_dashboard():
    try:
        video_id = request.args.get('video_id')
        return render_template("comment_dashboard.html", video_id=video_id)
    except Exception as e:
        flash(f"Error loading comment dashboard: {str(e)}", "danger")
        return redirect(url_for("index"))