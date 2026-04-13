import logging
from config import settings
from src.infrastructure.kafka_client import KafkaMessagingService
from src.services.producer_service import TweetStreamingService

def main():
    # 1. Налаштовуємо логування (професійний замінник print)
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info("🔧 Ініціалізація системи...")

    try:
        # 2. Створюємо інфраструктурний об'єкт
        kafka_client = KafkaMessagingService(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )

        # 3. Створюємо сервіс і передаємо йому клієнт (Dependency Injection)
        streamer = TweetStreamingService(messaging_service=kafka_client)

        # 4. Запуск!
        streamer.start_streaming()

    except KeyboardInterrupt:
        logger.info("🛑 Стрімінг зупинено користувачем.")
    except Exception as e:
        logger.error(f"💥 Критична помилка: {e}")
    finally:
        # 5. Завжди закриваємо з'єднання
        if 'kafka_client' in locals():
            kafka_client.close()
            logger.info("🔌 З'єднання з Kafka закрито.")

if __name__ == "__main__":
    main()