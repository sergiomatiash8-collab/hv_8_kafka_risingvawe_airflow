import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Налаштування логера
logger = logging.getLogger("KafkaMessagingService")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class KafkaMessagingService:
    def __init__(self, bootstrap_servers: str):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                retries=5,
                retry_backoff_ms=1000 
            )
            logger.info(f"✅ Підключено до Kafka: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"🚨 Критична помилка ініціалізації Kafka: {e}")
            raise

    def send_message(self, topic: str, message: dict):
        """
        Відправка повідомлення з використанням універсальних callback-ів.
        """
        try:
            future = self.producer.send(topic, value=message)
            
            # Передаємо topic, але обробляємо його через *args
            future.add_callback(self._on_success, topic)
            future.add_errback(self._on_error, topic, message)
            
        except KafkaError as ke:
            logger.warning(f"⚠️ Тимчасова помилка Kafka (Recovery state): {ke}")
        except Exception as e:
            logger.error(f"❌ Непередбачувана помилка при відправці: {e}")

    def _on_success(self, *args, **kwargs):
        """
        Універсальний обробник успіху. Ігнорує порядок аргументів від бібліотеки.
        """
        logger.debug("✅ Повідомлення успішно доставлено в Kafka")

    def _on_error(self, topic, message, exc, *args, **kwargs):
        """
        Універсальний обробник помилки.
        """
        logger.error(f"🚨 Помилка доставки в топік {topic}: {exc}")

    def flush(self):
        logger.info("⏳ Очищення буфера Kafka (Flush)...")
        self.producer.flush()

    def close(self):
        logger.info("🔌 Закриття з'єднання з Kafka.")
        self.producer.close()