import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME = "tweets"


CSV_FILE_PATH = "data/twcs.csv"
STREAMING_DELAY = 1 / 12  
BATCH_LIMIT = 1000


LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5454))
DB_NAME = os.getenv("DB_NAME", "twitter_sentiment")


SENTIMENT_THRESHOLD = 0.1