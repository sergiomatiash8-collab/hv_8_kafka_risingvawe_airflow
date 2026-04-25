import logging
import signal
import sys
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

    # Ініціалізація змінних для доступу у finally
    consumer_client = None
    consumer_service = None

    def handle_exit(sig, frame):
        """Обробник сигналів для м'якої зупинки Docker контейнера"""
        logger.info(f"📥 Отримано сигнал зупинки ({sig}). Завершуємо роботу...")
        if consumer_service:
            consumer_service.stop() # Вимикаємо прапорець роботи в циклі

    # Реєструємо сигнали зупинки
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        # 2. Ініціалізуємо інфраструктуру
        # Переконайся, що в класах прописані параметри підключення з settings
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
        # Цей метод має містити цикл "while self._is_running"
        consumer_service.run()

    except Exception as e:
        logger.error(f"💥 Критична помилка в консюмері: {e}")
    finally:
        # 6. Чисте закриття ресурсів
        if consumer_client:
            consumer_client.close()
            logger.info("🔌 З'єднання з Kafka закрито.")
        
        # Якщо в репозиторії є пул з'єднань, його теж варто закрити тут
        # if repository: repository.close() 
        
        sys.exit(0)

if __name__ == "__main__":
    main()