# text_sentiment.py
import torch
import numpy as np
from scipy.special import softmax
from transformers import AutoModelForSequenceClassification, AutoTokenizer

def load_sentiment_model():
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
    model = AutoModelForSequenceClassification.from_pretrained(model_name).to(device)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    labels = ["Negative", "Neutral", "Positive"]
    return model, tokenizer, labels

def preprocess_text(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def analyze_sentiment(text, model, tokenizer, labels):
    processed_text = preprocess_text(text)
    encoded_input = tokenizer(processed_text, return_tensors="pt").to(model.device)
    output = model(**encoded_input)
    scores = softmax(output.logits[0].detach().cpu().numpy())
    idx = np.argmax(scores)
    return labels[idx], scores[idx]