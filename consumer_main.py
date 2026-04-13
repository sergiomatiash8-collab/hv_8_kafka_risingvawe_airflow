import logging
from config import settings
from src.infrastructure.kafka_consumer import KafkaConsumerClient
from src.infrastructure.db_repository import PostgresRepository
from src.services.analyzer import SentimentAnalyzer
from src.services.consumer_service import TweetConsumerService

def main():
    # 1. Налаштовуємо логування
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info("📡 Запуск Tweet Sentiment Consumer...")

    try:
        # 2. Ініціалізуємо інфраструктуру
        consumer_client = KafkaConsumerClient()
        repository = PostgresRepository()
        
        # 3. Ініціалізуємо логіку (Аналізатор)
        analyzer = SentimentAnalyzer()

        # 4. Збираємо сервіс (Dependency Injection)
        consumer_service = TweetConsumerService(
            consumer_client=consumer_client,
            repository=repository,
            analyzer=analyzer
        )

        # 5. Поїхали!
        consumer_service.run()

    except KeyboardInterrupt:
        logger.info("🛑 Консюмер зупинений користувачем.")
    except Exception as e:
        logger.error(f"💥 Критична помилка в консюмері: {e}")
    finally:
        if 'consumer_client' in locals():
            consumer_client.close()
            logger.info("🔌 З'єднання з Kafka закрито.")

if __name__ == "__main__":
    main()