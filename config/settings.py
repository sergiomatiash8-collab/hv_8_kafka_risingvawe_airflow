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