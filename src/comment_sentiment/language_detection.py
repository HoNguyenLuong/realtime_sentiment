from langdetect import detect, LangDetectException


class LanguageDetector:
    """
    Detects the language of comment text.
    """

    def detect_language(self, text):
        """
        Detect the language of a given text.

        Args:
            text (str): Input text

        Returns:
            str: ISO language code (e.g., 'en', 'vi', 'ja')
        """
        if not text or not isinstance(text, str) or len(text.strip()) < 3:
            return "unknown"

        try:
            return detect(text)
        except LangDetectException:
            return "unknown"
