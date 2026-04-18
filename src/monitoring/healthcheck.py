import sys
import logging
from kafka import KafkaProducer
from config import settings

# Налаштовуємо мінімальне логування для консолі Docker
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("Healthcheck")

def check_kafka_status():
    """
    Перевіряє, чи може додаток підключитися до брокера.
    """
    try:
        # Створюємо тимчасовий клієнт з дуже коротким таймаутом
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=1000,
            connections_max_idle_ms=5000
        )
        
        # Перевіряємо, чи є з'єднання хоча б з одним вузлом
        if producer.bootstrap_connected():
            producer.close()
            return True
        
        return False
    except Exception as e:
        # Ми не хочемо детальних помилок у логах докера щоразу, 
        # тому просто повертаємо False
        return False

if __name__ == "__main__":
    if check_kafka_status():
        sys.exit(0)  # Стан: Healthy
    else:
        sys.exit(1)  # Стан: Unhealthy