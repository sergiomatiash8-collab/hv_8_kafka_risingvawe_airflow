from textblob import TextBlob
from config import settings
from src.domain.consumer_models import TweetSentiment

class SentimentAnalyzer:
    def __init__(self):
        self.threshold = settings.SENTIMENT_THRESHOLD

    def analyze(self, tweet_data: dict) -> TweetSentiment:
        """
        Accepts a dictionary from Kafka, analyzes the text,
        and returns a TweetSentiment object.
        """
        text = tweet_data.get("text", "")
        
        
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        
        
        if polarity > self.threshold:
            sentiment = "positive"
        elif polarity < -self.threshold:
            sentiment = "negative"
        else:
            sentiment = "neutral"

       
        return TweetSentiment(
            tweet_id=str(tweet_data.get("tweet_id")),
            author=str(tweet_data.get("author", "unknown")),
            text=text,
            sentiment=sentiment,
            polarity=polarity
        )