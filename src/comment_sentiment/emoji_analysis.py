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
            "😊": 1.0, "😁": 1.0, "😄": 1.0, "😍": 1.0, "❤️": 1.0, "❤": 1.0,
            "👍": 0.8, "🙂": 0.6, "😎": 0.7, "🔥": 0.7, "✨": 0.6,
            "👏": 0.8, "🥰": 1.0, "😘": 0.9, "🙏": 0.7, "👌": 0.7,
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
            
        # Phương pháp cải tiến để tìm emoji
        emojis_list = []
        
        # Sử dụng thư viện emoji để phát hiện
        for char in text:
            if char in emoji.EMOJI_DATA:
                emojis_list.append(char)
        
        # Nếu không tìm thấy emoji bằng phương pháp trên, thử dùng regex
        if not emojis_list:
            # Mẫu regex cho emoji cơ bản và emoji kết hợp
            emoji_pattern = re.compile("["
                                       u"\U0001F600-\U0001F64F"  # emoticons
                                       u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                       u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                       u"\U0001F700-\U0001F77F"  # alchemical symbols
                                       u"\U0001F780-\U0001F7FF"  # Geometric Shapes
                                       u"\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
                                       u"\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
                                       u"\U0001FA00-\U0001FA6F"  # Chess Symbols
                                       u"\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
                                       u"\U00002702-\U000027B0"  # Dingbats
                                       u"\U000024C2-\U0001F251" 
                                       "]+", flags=re.UNICODE)
            
            matches = emoji_pattern.findall(text)
            for match in matches:
                # Thêm từng ký tự trong match vào danh sách emoji
                for char in match:
                    emojis_list.append(char)
                    
        # Loại bỏ các ký tự trùng lặp nhưng giữ thứ tự
        seen = set()
        unique_emojis = []
        for emoji_char in emojis_list:
            if emoji_char not in seen:
                seen.add(emoji_char)
                unique_emojis.append(emoji_char)
                
        return unique_emojis

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
        
        # Log found emojis for debugging
        if emojis:
            print(f"Found emojis in text: {emojis}")

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
