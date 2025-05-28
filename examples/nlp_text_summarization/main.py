import nltk
import numpy as np
from nltk.probability import FreqDist
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

nltk.download("punkt_tab", quiet=True)
nltk.download("stopwords", quiet=True)


# Step 1: Text Tokenization
def tokenize_text(text):
    from nltk.corpus import stopwords

    words = word_tokenize(text.lower())
    stop_words = set(stopwords.words("english"))
    return [word for word in words if word.isalpha() and word not in stop_words]


# Step 2: Keyword Extraction
def extract_keywords(tokens):
    freq_dist = FreqDist(tokens)
    common_keywords = freq_dist.most_common(5)
    return [word for word, _ in common_keywords]


# Step 3: Summary Generation
def generate_summary(text, keywords):
    sentences = sent_tokenize(text)
    important_sentences = [
        sentence
        for sentence in sentences
        if any(keyword in sentence.lower() for keyword in keywords)
    ]
    return " ".join(important_sentences[:2])  # Return the first two important sentences


# Step 4: Sentiment Analysis
def analyze_sentiment(summary) -> str:
    # Simplified sentiment analysis: More positive words = Positive sentiment
    positive_words = {"good", "great", "excellent", "positive", "fortunate"}
    negative_words = {"bad", "terrible", "poor", "negative", "unfortunate"}
    words = set(summary.lower().split())
    sentiment_score = len(words & positive_words) - len(words & negative_words)
    if sentiment_score > 0:
        return "Positive"
    if sentiment_score < 0:
        return "Negative"
    return "Neutral"


# Step 5: Summarization Result Aggregation
def aggregate_summarization(sentiment):
    # Convert the sentiment masked array to a list
    sentiment_list = np.array(sentiment).tolist()

    # Count occurrences of each sentiment type
    positive_count = sentiment_list.count("Positive")
    negative_count = sentiment_list.count("Negative")
    neutral_count = sentiment_list.count("Neutral")

    return {
        "Positive": positive_count,
        "Negative": negative_count,
        "Neutral": neutral_count,
    }
