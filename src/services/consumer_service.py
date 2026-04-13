import json
import logging
from src.infrastructure.kafka_consumer import KafkaConsumerClient
from src.infrastructure.db_repository import PostgresRepository
from src.services.analyzer import SentimentAnalyzer

logger = logging.getLogger(__name__)

class TweetConsumerService:
    def __init__(self, consumer_client: KafkaConsumerClient, repository: PostgresRepository, analyzer: SentimentAnalyzer):
        """Впровадження залежностей"""
        self.consumer_client = consumer_client
        self.repository = repository
        self.analyzer = analyzer

    def run(self):
        """Запуск нескінченного циклу обробки з ручним парсингом JSON"""
        logger.info("🛠️ Підготовка бази даних...")
        self.repository.create_table()

        logger.info("🎧 Очікування повідомлень із Kafka...")
        
        try:
            for raw_data in self.consumer_client.get_messages():
                if not raw_data:
                    continue
                
                try:
                    # 1. Пробуємо перетворити байти на словник (JSON)
                    # errors='ignore' допоможе, якщо в тексті є "криві" символи
                    decoded_data = raw_data.decode("utf-8", errors="ignore")
                    tweet_dict = json.loads(decoded_data)
                    
                    # 2. Аналіз настрою через наш сервіс
                    sentiment_data = self.analyzer.analyze(tweet_dict)
                    
                    # 3. Збереження в базу через репозиторій
                    self.repository.save_sentiment(sentiment_data)
                    
                    logger.info(f"✅ Оброблено твіт {sentiment_data.tweet_id}: {sentiment_data.sentiment}")
                
                except json.JSONDecodeError:
                    logger.warning("⚠️ Пропущено повідомлення: невірний формат JSON (сміття в Kafka)")
                    continue
                except Exception as msg_error:
                    logger.warning(f"⚠️ Помилка при обробці окремого повідомлення: {msg_error}")
                    continue
        
        except Exception as e:
            logger.error(f"❌ Критична помилка сервісу: {e}")