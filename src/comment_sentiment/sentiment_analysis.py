import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import numpy as np


class SentimentAnalyzer:
    """
    Performs sentiment analysis on comments using pre-trained models.
    Supports multiple languages and handles various text formats.
    """

    def __init__(self, model_name="cardiffnlp/twitter-xlm-roberta-base-sentiment"):
        """
        Initialize the sentiment analyzer with a multilingual model
        """
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.labels = ["negative", "neutral", "positive"]

    def analyze(self, text):
        """
        Analyze the sentiment of a given text.

        Args:
            text (str): The comment text to analyze

        Returns:
            dict: A dictionary containing sentiment labels and confidence scores
        """
        # Clean the text
        cleaned_text = self._preprocess_text(text)

        # Skip empty text
        if not cleaned_text:
            return {
                "sentiment": "neutral",
                "confidence": {
                    "negative": 0.0,
                    "neutral": 1.0,
                    "positive": 0.0
                }
            }

        # Tokenize and get sentiment
        inputs = self.tokenizer(cleaned_text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad():
            outputs = self.model(**inputs)

        # Get scores and convert to probabilities
        scores = torch.nn.functional.softmax(outputs.logits, dim=1)[0].numpy()

        # Create result dictionary
        sentiment_idx = np.argmax(scores)
        result = {
            "sentiment": self.labels[sentiment_idx],
            "confidence": {
                self.labels[i]: float(scores[i]) for i in range(len(self.labels))
            }
        }

        return result

    def _preprocess_text(self, text):
        """
        Preprocess the text before analysis.

        Args:
            text (str): Raw text

        Returns:
            str: Cleaned text
        """
        if not text or not isinstance(text, str):
            return ""

        # Basic preprocessing
        text = text.strip()

        # You can add more preprocessing steps here

        return text