import json
from kafka import KafkaConsumer
from config import settings

class KafkaConsumerClient:
    def __init__(self):
        """Ініціалізація клієнта Kafka для читання"""
        self.consumer = KafkaConsumer(
            settings.KAFKA_TOPIC_NAME,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="sentiment-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="ignore"))
        )

    def get_messages(self):
        """Генератор, який повертає повідомлення по одному"""
        for message in self.consumer:
            yield message.value

    def close(self):
        self.consumer.close()