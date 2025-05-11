import emoji
import re

class EmojiAnalyzer:
    """
    Analyzes emojis in comments to extract sentiment signals.
    """

    def __init__(self):
        """
        Initialize the emoji analyzer with sentiment mappings.
        """
        # Simple emoji sentiment dictionary (can be expanded)
        self.emoji_sentiment = {
            # Positive emojis
            "😊": 1.0, "😁": 1.0, "😄": 1.0, "😍": 1.0, "❤️": 1.0,
            "👍": 0.8, "🙂": 0.6, "😎": 0.7, "🔥": 0.7, "✨": 0.6,
            # Neutral emojis
            "😐": 0.0, "🤔": 0.0, "😶": 0.0, "🙄": -0.2,
            # Negative emojis
            "😠": -0.8, "😡": -1.0, "😢": -0.7, "😭": -0.8, "👎": -0.8,
            "😔": -0.6, "😞": -0.7, "😒": -0.5, "💔": -0.9
        }

    def extract_emojis(self, text):
        """
        Extract emojis from text.

        Args:
            text (str): Input text

        Returns:
            list: List of emojis found in the text
        """
        if not text or not isinstance(text, str):
            return []

        return [c for c in text if c in emoji.EMOJI_DATA]

    def analyze(self, text):
        """
        Analyze emojis in the text for sentiment.

        Args:
            text (str): Input text containing emojis

        Returns:
            dict: Sentiment analysis based on emojis
        """
        if not text or not isinstance(text, str):
            return {
                "emoji_sentiment": "neutral",
                "emoji_score": 0.0,
                "emojis_found": []
            }

        emojis = self.extract_emojis(text)

        if not emojis:
            return {
                "emoji_sentiment": "neutral",
                "emoji_score": 0.0,
                "emojis_found": []
            }

        # Calculate average sentiment score from emojis
        total_score = 0
        scored_emojis = 0

        for e in emojis:
            if e in self.emoji_sentiment:
                total_score += self.emoji_sentiment[e]
                scored_emojis += 1

        # Determine overall sentiment
        if scored_emojis > 0:
            avg_score = total_score / scored_emojis
        else:
            avg_score = 0.0

        # Map score to sentiment label
        if avg_score > 0.3:
            sentiment = "positive"
        elif avg_score < -0.3:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        return {
            "emoji_sentiment": sentiment,
            "emoji_score": avg_score,
            "emojis_found": emojis
        }
