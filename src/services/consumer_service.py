import json
import logging
from src.infrastructure.kafka_consumer import KafkaConsumerClient
from src.infrastructure.db_repository import PostgresRepository
from src.services.analyzer import SentimentAnalyzer

logger = logging.getLogger(__name__)

class TweetConsumerService:
    def __init__(self, consumer_client: KafkaConsumerClient, repository: PostgresRepository, analyzer: SentimentAnalyzer):
        """Dependency injection"""
        self.consumer_client = consumer_client
        self.repository = repository
        self.analyzer = analyzer

    def run(self):
        """Run infinite processing loop with manual JSON parsing"""
        logger.info("Preparing database...")
        self.repository.create_table()

        logger.info("Waiting for Kafka messages...")
        
        try:
            for raw_data in self.consumer_client.get_messages():
                if not raw_data:
                    continue
                
                try:
                    
                    decoded_data = raw_data.decode("utf-8", errors="ignore")
                    tweet_dict = json.loads(decoded_data)
                    
                   
                    sentiment_data = self.analyzer.analyze(tweet_dict)
                    
                    
                    self.repository.save_sentiment(sentiment_data)
                    
                    logger.info(f"Processed tweet {sentiment_data.tweet_id}: {sentiment_data.sentiment}")
                
                except json.JSONDecodeError:
                    logger.warning("Skipped message: invalid JSON format (corrupted Kafka message)")
                    continue
                except Exception as msg_error:
                    logger.warning(f"Error processing single message: {msg_error}")
                    continue
        
        except Exception as e:
            logger.error(f"Critical service error: {e}")