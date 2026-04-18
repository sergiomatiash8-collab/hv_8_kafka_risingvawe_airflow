import logging
import signal
import sys
from config import settings
from src.infrastructure.kafka_client import KafkaMessagingService
from src.services.producer_service import TweetStreamingService

def main():
    # 1. Налаштовуємо логування
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info("🔧 Ініціалізація системи...")

    # Ініціалізуємо змінні заздалегідь, щоб вони були доступні у finally
    kafka_client = None
    streamer = None

    def handle_exit(sig, frame):
        """Обробник сигналів для м'якої зупинки"""
        logger.info(f"📥 Отримано сигнал зупинки ({sig}). Закриваємося...")
        if streamer:
            streamer.stop() # Кажемо циклу в сервісі зупинитися

    # Реєструємо обробники для Ctrl+C (SIGINT) та Docker Stop (SIGTERM)
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        # 2. Створюємо інфраструктурний об'єкт
        kafka_client = KafkaMessagingService(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )

        # 3. Створюємо сервіс і передаємо йому клієнт
        streamer = TweetStreamingService(messaging_service=kafka_client)

        # 4. Запуск!
        streamer.start_streaming()

    except Exception as e:
        logger.error(f"💥 Критична помилка: {e}")
    finally:
        # 5. Завжди закриваємо з'єднання
        if kafka_client:
            kafka_client.close()
            logger.info("🔌 З'єднання з Kafka закрито.")
        sys.exit(0)

if __name__ == "__main__":
    main()