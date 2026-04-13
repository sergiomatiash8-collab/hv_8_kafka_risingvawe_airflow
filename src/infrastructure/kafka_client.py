import json
from kafka import KafkaProducer

class KafkaMessagingService:
    def __init__(self, bootstrap_servers: str):
        """
        Ініціалізуємо реальний клієнт Kafka.
        Ми ховаємо логіку налаштування (serializer) всередині цього класу.
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )

    def send_message(self, topic: str, message: dict):
        """
        Метод для відправки. Якщо ми змінимо бібліотеку, 
        назва цього методу залишиться такою самою для інших частин програми.
        """
        try:
            self.producer.send(topic, value=message)
        except Exception as e:
            print(f"❌ [Kafka Error]: {e}")

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()