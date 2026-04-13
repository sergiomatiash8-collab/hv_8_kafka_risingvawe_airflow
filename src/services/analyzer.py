from textblob import TextBlob
from config import settings
from src.domain.consumer_models import TweetSentiment

class SentimentAnalyzer:
    def __init__(self):
        # Ми виносимо поріг чутливості в налаштування
        self.threshold = settings.SENTIMENT_THRESHOLD

    def analyze(self, tweet_data: dict) -> TweetSentiment:
        """
        Приймає словник із Kafka, аналізує текст 
        і повертає готовий об'єкт TweetSentiment.
        """
        text = tweet_data.get("text", "")
        
        # Використовуємо TextBlob для аналізу
        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        
        # Визначаємо категорію настрою
        if polarity > self.threshold:
            sentiment = "positive"
        elif polarity < -self.threshold:
            sentiment = "negative"
        else:
            sentiment = "neutral"

        # Створюємо об'єкт моделі, яку ми підготували раніше
        return TweetSentiment(
            tweet_id=str(tweet_data.get("tweet_id")),
            author=str(tweet_data.get("author", "unknown")),
            text=text,
            sentiment=sentiment,
            polarity=polarity
        )