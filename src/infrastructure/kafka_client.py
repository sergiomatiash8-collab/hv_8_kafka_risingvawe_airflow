import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Logger configuration
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
            logger.info(f"Connected to Kafka: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Critical Kafka initialization error: {e}")
            raise

    def send_message(self, topic: str, message: dict):
        """
        Send message using universal callbacks.
        """
        try:
            future = self.producer.send(topic, value=message)
            
            # Pass topic, handled via *args in callback
            future.add_callback(self._on_success, topic)
            future.add_errback(self._on_error, topic, message)
            
        except KafkaError as ke:
            logger.warning(f"Temporary Kafka error (Recovery state): {ke}")
        except Exception as e:
            logger.error(f"Unexpected error during send: {e}")

    def _on_success(self, *args, **kwargs):
        """
        Universal success handler. Ignores argument order from library.
        """
        logger.debug("Message successfully delivered to Kafka")

    def _on_error(self, topic, message, exc, *args, **kwargs):
        """
        Universal error handler.
        """
        logger.error(f"Delivery error to topic {topic}: {exc}")

    def flush(self):
        logger.info("Flushing Kafka buffer...")
        self.producer.flush()

    def close(self):
        logger.info("Closing Kafka connection.")
        self.producer.close()