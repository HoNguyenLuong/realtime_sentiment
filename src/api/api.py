from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
# from src.consumer.controller import get_sentiment_results  # Import từ consumer
from src.producer.controller import process_url  # Import từ producer
from src.utils.image_utils import get_sentiment_results
from src.utils.audio_utils import get_audio_sentiment_results

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

@app.route("/get_results", methods=["GET"])
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


@app.route("/comment", methods=["GET"])
def get_comment_results():
    try:
        results = get_comment_results("audio_results")
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500