import psycopg2
from config import settings
from src.domain.consumer_models import TweetSentiment

class PostgresRepository:
    def __init__(self):
        """Ініціалізуємо налаштування підключення"""
        self.connection_params = {
            "user": settings.DB_USER,
            "password": settings.DB_PASSWORD,
            "host": settings.DB_HOST,
            "port": settings.DB_PORT,
            "database": settings.DB_NAME
        }

    def create_table(self):
        """Створюємо таблицю, якщо її ще немає"""
        query = """
        CREATE TABLE IF NOT EXISTS tweet_sentiments (
            tweet_id TEXT PRIMARY KEY,
            author TEXT,
            text TEXT,
            sentiment TEXT,
            polarity FLOAT
        )
        """
        with psycopg2.connect(**self.connection_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                conn.commit()

    def save_sentiment(self, tweet_sentiment: TweetSentiment):
        """Зберігаємо об'єкт TweetSentiment в базу"""
        query = """
            INSERT INTO tweet_sentiments (tweet_id, author, text, sentiment, polarity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tweet_id) DO NOTHING
        """
        with psycopg2.connect(**self.connection_params) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, tweet_sentiment.to_tuple())
                conn.commit()