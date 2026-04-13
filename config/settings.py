import os

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME = "tweets"

# Data settings
CSV_FILE_PATH = "data/twcs.csv"
STREAMING_DELAY = 1 / 12  # приблизно 0.083s для 12 msg/sec
BATCH_LIMIT = 1000

# Logging format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Налаштування Postgres (RisingWave використовує той самий протокол)
DB_USER = os.getenv("DB_USER", "admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", 5454))
DB_NAME = os.getenv("DB_NAME", "twitter_sentiment")

# Налаштування аналізатора
SENTIMENT_THRESHOLD = 0.1