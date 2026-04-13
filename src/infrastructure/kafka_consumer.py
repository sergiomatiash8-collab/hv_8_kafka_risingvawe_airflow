import json
from kafka import KafkaConsumer
from config import settings

class KafkaConsumerClient:
    def __init__(self):
        """Ініціалізація клієнта Kafka для читання сирих байтів"""
        self.consumer = KafkaConsumer(
            settings.KAFKA_TOPIC_NAME,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            # Нова група, щоб точно проскочити старі помилки
            group_id="sentiment-group-v3",
            # Ми НЕ використовуємо value_deserializer тут, щоб уникнути вильоту
        )

    def get_messages(self):
        """Генератор, який повертає сирі повідомлення"""
        for message in self.consumer:
            yield message.value

    def close(self):
        self.consumer.close()