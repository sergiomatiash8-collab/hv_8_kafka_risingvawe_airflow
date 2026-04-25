import json
from kafka import KafkaConsumer
from config import settings

class KafkaConsumerClient:
    def __init__(self):
        """Initialize Kafka client for reading raw bytes"""
        self.consumer = KafkaConsumer(
            settings.KAFKA_TOPIC_NAME,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            # New group to avoid processing old faulty messages
            group_id="sentiment-group-v3",
            # We DO NOT use value_deserializer here to avoid crashes
        )

    def get_messages(self):
        """Generator that yields raw messages"""
        for message in self.consumer:
            yield message.value

    def close(self):
        """Close Kafka consumer connection"""
        self.consumer.close()