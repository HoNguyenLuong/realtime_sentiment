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
            - Could be {"comments": [comment]} format (from get_comments_from_minio)
            - Or a single comment object
            
    Returns:
        list: A list of extracted comment objects
    """
    # Nếu json_data là None
    if not json_data:
        return []
        
    comments = []
    
    # Nếu json_data có khóa "comments" (định dạng cũ)
    if "comments" in json_data:
        raw_comments = json_data["comments"]
        # Nếu là list comments
        if isinstance(raw_comments, list):
            for comment in raw_comments:
                if isinstance(comment, dict) and "text" in comment:
                    comments.append(self._process_comment(comment))
        # Nếu có 1 comment trong comments
        elif isinstance(raw_comments, dict) and "text" in raw_comments:
            comments.append(self._process_comment(raw_comments))
    
    # Nếu json_data có vẻ là một comment riêng lẻ
    elif isinstance(json_data, dict) and "text" in json_data:
        comments.append(self._process_comment(json_data))
        
    return comments
    
def _process_comment(self, comment):
    """Process a single comment object"""
    if not comment or "text" not in comment or not comment["text"]:
        return None
        
    comment_obj = {
        "id": comment.get("id", "unknown"),
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
            
    return comment_obj
