import logging
import signal
import sys
from config import settings
from src.infrastructure.kafka_consumer import KafkaConsumerClient
from src.infrastructure.db_repository import PostgresRepository
from src.services.analyzer import SentimentAnalyzer
from src.services.consumer_service import TweetConsumerService

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info("Starting Tweet Sentiment Consumer...")

    # Initialize variables for safe cleanup in finally block
    consumer_client = None
    consumer_service = None

    def handle_exit(sig, frame):
        """Signal handler for graceful Docker shutdown"""
        logger.info(f"Received shutdown signal ({sig}). Stopping service...")
        if consumer_service:
            consumer_service.stop()  # Stop main processing loop

    # Register shutdown signals
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        # Initialize infrastructure layer
        consumer_client = KafkaConsumerClient()
        repository = PostgresRepository()

        # Initialize business logic layer
        analyzer = SentimentAnalyzer()

        # Dependency injection setup
        consumer_service = TweetConsumerService(
            consumer_client=consumer_client,
            repository=repository,
            analyzer=analyzer
        )

        # Start processing loop
        consumer_service.run()

    except Exception as e:
        logger.error(f"Critical consumer error: {e}")
    finally:
        # Graceful resource cleanup
        if consumer_client:
            consumer_client.close()
            logger.info("Kafka connection closed.")

        # If repository has connection pool, close it here
        # if repository: repository.close()

        sys.exit(0)

if __name__ == "__main__":
    main()