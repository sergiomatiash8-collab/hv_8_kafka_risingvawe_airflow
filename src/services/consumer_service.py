import logging
from src.infrastructure.kafka_consumer import KafkaConsumerClient
from src.infrastructure.db_repository import PostgresRepository
from src.services.analyzer import SentimentAnalyzer

logger = logging.getLogger(__name__)

class TweetConsumerService:
    def __init__(self, consumer_client: KafkaConsumerClient, repository: PostgresRepository, analyzer: SentimentAnalyzer):
        self.consumer_client = consumer_client
        self.repository = repository
        self.analyzer = analyzer

    def run(self):
        """Запуск нескінченного циклу обробки"""
        logger.info("🛠️ Підготовка бази даних...")
        self.repository.create_table()

        logger.info("🎧 Очікування повідомлень із Kafka...")
        
        # Зовнішній try ловить критичні помилки (наприклад, розрив з'єднання)
        try:
            for raw_tweet in self.consumer_client.get_messages():
                if not raw_tweet:
                    continue
                
                # Внутрішній try захищає від помилок в окремих повідомленнях
                try:
                    # 1. Аналіз настрою
                    sentiment_data = self.analyzer.analyze(raw_tweet)
                    
                    # 2. Збереження в базу
                    self.repository.save_sentiment(sentiment_data)
                    
                    logger.info(f"✅ Оброблено твіт {sentiment_data.tweet_id}: {sentiment_data.sentiment}")
                
                except Exception as msg_error:
                    logger.warning(f"⚠️ Пропущено повідомлення через помилку парсингу: {msg_error}")
                    continue # Йдемо далі, не зупиняючи весь сервіс
        
        except Exception as e:
            logger.error(f"❌ Критична помилка сервісу: {e}")