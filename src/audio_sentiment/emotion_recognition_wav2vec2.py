from speechbrain.inference.interfaces import foreign_class

def load_emotion_classifier():
    """
    Load the emotion recognition model.
    """
    return foreign_class(
        source="speechbrain/emotion-recognition-wav2vec2-IEMOCAP",
        pymodule_file="custom_interface.py",
        classname="CustomEncoderWav2vec2Classifier"
    )

def classify_emotion(classifier, audio_file):
    """
    Classify emotion from an audio file.
    """
    out_prob, score, index, text_lab = classifier.classify_file(audio_file)
    return text_lab