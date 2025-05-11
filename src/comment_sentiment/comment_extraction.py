import json
from datetime import datetime


class CommentExtractor:
    """
    Extracts comments from JSON data and prepares them for sentiment analysis.
    """

    def extract_comments(self, json_data):
        """
        Extract comments from the JSON data structure.

        Args:
            json_data (dict): The JSON data containing comments

        Returns:
            list: A list of extracted comment objects
        """
        if not json_data or "comments" not in json_data:
            return []

        comments = []

        # Process all comments
        for i, comment in enumerate(json_data["comments"]):
            if "text" not in comment or not comment["text"]:
                continue

            comment_obj = {
                "id": comment.get("id", f"unknown_{i}"),
                "text": comment["text"],
                "author": comment.get("author", "unknown"),
                "timestamp": comment.get("timestamp", None),
                "parent_id": comment.get("parent", "root"),
                "likes": comment.get("like_count", 0)
            }

            # Convert timestamp if present
            if comment_obj["timestamp"] and isinstance(comment_obj["timestamp"], (int, float)):
                try:
                    comment_obj["timestamp_iso"] = datetime.fromtimestamp(
                        comment_obj["timestamp"]
                    ).isoformat()
                except (ValueError, OverflowError):
                    comment_obj["timestamp_iso"] = None

            comments.append(comment_obj)

        return comments
