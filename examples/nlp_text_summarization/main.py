import nltk
import numpy as np
from nltk.probability import FreqDist
from nltk.tokenize import sent_tokenize
from nltk.tokenize import word_tokenize

nltk.download("punkt", quiet=True)
nltk.download("stopwords", quiet=True)


# Step 1: Text Tokenization
def tokenize_text(text: str) -> list[str]:
    """
    Tokenizes the input text into a list of words.

    This function converts the text to lowercase, splits it into individual words,
    and filters out common English "stop words" (e.g., 'the', 'a', 'is') and
    any tokens that are not purely alphabetic.

    Args:
        text: The original text to be summarized.

    Returns:
        A list of cleaned, significant words from the text.
    """
    from nltk.corpus import stopwords

    words = word_tokenize(text.lower())
    stop_words = set(stopwords.words("english"))
    return [word for word in words if word.isalpha() and word not in stop_words]


# Step 2: Keyword Extraction
def extract_keywords(tokens: list[str]) -> list[str]:
    """
    Identifies the most frequent words in a list of tokens to serve as keywords.

    This function calculates the frequency of each word and returns the top 5
    most common words, which are considered the primary keywords of the text.

    Args:
        tokens: A list of words, typically from the `tokenize_text` function.

    Returns:
        A list containing the 5 most common keywords.
    """
    freq_dist = FreqDist(tokens)
    common_keywords = freq_dist.most_common(5)
    return [word for word, _ in common_keywords]


# Step 3: Summary Generation
def generate_summary(text: str, keywords: list[str]) -> str:
    """
    Creates a brief summary of the text based on keyword relevance.

    The function splits the original text into sentences and selects those that
    contain any of the specified keywords. It then joins the first two of these
    "important" sentences to form the summary.

    Args:
        text: The original text to be summarized.
        keywords: A list of important keywords to look for in the sentences.

    Returns:
        A string containing a concise, keyword-based summary.
    """
    sentences = sent_tokenize(text)
    important_sentences = [
        sentence
        for sentence in sentences
        if any(keyword in sentence.lower() for keyword in keywords)
    ]
    return " ".join(important_sentences[:2])  # Return the first two important sentences


# Step 4: Sentiment Analysis
def analyze_sentiment(summary: str) -> str:
    """
    Performs a simplified sentiment analysis on the summary text.

    It determines the sentiment by counting the presence of predefined positive and
    negative words. The sentiment is classified as "Positive", "Negative", or
    "Neutral" based on whether positive or negative words are more frequent.

    Args:
        summary: The summary text to be analyzed.

    Returns:
        A string indicating the sentiment: "Positive", "Negative", or "Neutral".
    """
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
def aggregate_summarization(sentiment: np.ndarray) -> dict[str, float]:
    """
    Aggregates sentiment analysis results into a summary dictionary.

    This function takes one or more sentiment labels and counts the occurrences
    of each type ("Positive", "Negative", "Neutral"), returning the final counts.

    Args:
        sentiment: A single sentiment string or a list of sentiment strings.

    Returns:
        A dictionary with the counts for each sentiment category.
    """
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
