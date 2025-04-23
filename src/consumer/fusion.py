import numpy as np


# Function to combine sentiment labels using Weighted Average
def combine_weighted_average(audio_label, audio_score, video_label, video_score, audio_weight=0.5):
    """
    Combine sentiment labels using weighted average of confidence scores.

    Parameters:
    - audio_label: str, sentiment label from audio ('positive', 'negative', 'neutral')
    - audio_score: float, confidence score for audio label (0 to 1)
    - video_label: str, sentiment label from video ('positive', 'negative', 'neutral')
    - video_score: float, confidence score for video label (0 to 1)
    - audio_weight: float, weight for audio prediction (default 0.5)

    Returns:
    - str, combined sentiment label ('positive', 'negative', 'neutral')
    """
    # Map labels to numeric scores
    label_to_score = {"positive": 1, "negative": -1, "neutral": 0}
    audio_numeric = label_to_score.get(audio_label, 0)
    video_numeric = label_to_score.get(video_label, 0)

    # Calculate weighted score
    combined_score = audio_weight * audio_score * audio_numeric + (1 - audio_weight) * video_score * video_numeric

    # Thresholds for classification
    if combined_score > 0.2:
        return "positive"
    elif combined_score < -0.2:
        return "negative"
    else:
        return "neutral"


# Function to combine sentiment labels using Majority Voting
def combine_majority_voting(audio_label, audio_score, video_label, video_score):
    """
    Combine sentiment labels using majority voting or confidence-based priority.

    Parameters:
    - audio_label: str, sentiment label from audio ('positive', 'negative', 'neutral')
    - audio_score: float, confidence score for audio label (0 to 1)
    - video_label: str, sentiment label from video ('positive', 'negative', 'neutral')
    - video_score: float, confidence score for video label (0 to 1)

    Returns:
    - str, combined sentiment label ('positive', 'negative', 'neutral')
    """
    if audio_score > video_score:
        return audio_label
    elif video_score > audio_score:
        return video_label
    else:
        # If scores are equal, prioritize audio (arbitrary choice, can be adjusted)
        return audio_label


# Main function to process and combine sentiments
def combine_sentiments(audio_label, audio_score, video_label, video_score, method="weighted_average", audio_weight=0.5):
    """
    Main function to combine sentiments using specified method.

    Parameters:
    - audio_label, audio_score, video_label, video_score: as described above
    - method: str, either 'weighted_average' or 'majority_voting'
    - audio_weight: float, weight for audio in weighted_average method

    Returns:
    - str, final sentiment label
    """
    if method == "weighted_average":
        return combine_weighted_average(audio_label, audio_score, video_label, video_score, audio_weight)
    elif method == "majority_voting":
        return combine_majority_voting(audio_label, audio_score, video_label, video_score)
    else:
        raise ValueError("Invalid method. Choose 'weighted_average' or 'majority_voting'.")


# Example usage
def main():
    # Example inputs (replace with your actual labels and scores)
    audio_label = "positive"
    audio_score = 0.8
    video_label = "neutral"
    video_score = 0.6

    # Combine using Weighted Average
    result_weighted = combine_sentiments(audio_label, audio_score, video_label, video_score, method="weighted_average")
    print(f"Weighted Average Result: {result_weighted}")

    # Combine using Majority Voting
    result_voting = combine_sentiments(audio_label, audio_score, video_label, video_score, method="majority_voting")
    print(f"Majority Voting Result: {result_voting}")


if __name__ == "__main__":
    main()