import logging
import signal
import sys
from config import settings
from src.infrastructure.kafka_client import KafkaMessagingService
from src.services.producer_service import TweetStreamingService

def main():
    # Configure logging
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info("System initialization started")

    # Initialize variables for safe cleanup
    kafka_client = None
    streamer = None

    def handle_exit(sig, frame):
        """Graceful shutdown signal handler"""
        logger.info(f"Shutdown signal received ({sig}). Stopping service...")
        if streamer:
            streamer.stop()  # Stop streaming loop

    # Register signal handlers for Ctrl+C and Docker stop
    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        # Initialize infrastructure layer
        kafka_client = KafkaMessagingService(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS
        )

        # Initialize streaming service
        streamer = TweetStreamingService(messaging_service=kafka_client)

        # Start streaming process
        streamer.start_streaming()

    except Exception as e:
        logger.error(f"Critical error occurred: {e}")
    finally:
        # Ensure Kafka connection is always closed
        if kafka_client:
            kafka_client.close()
            logger.info("Kafka connection closed.")

        sys.exit(0)

if __name__ == "__main__":
    main()