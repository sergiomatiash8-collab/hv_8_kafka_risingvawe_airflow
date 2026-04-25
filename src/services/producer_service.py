import csv
import time
import logging
from src.services.transformers import transform_row_to_tweet
from config import settings

logger = logging.getLogger("TweetStreamingService")

class TweetStreamingService:
    def __init__(self, messaging_service):
        self.messaging_service = messaging_service
        self._is_running = True

    def start_streaming(self):
        logger.info(f"Starting streaming from file: {settings.CSV_FILE_PATH}")
        
        try:
            with open(settings.CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0
                
                for row in reader:
                    if not self._is_running:
                        break

                    
                    tweet_obj = transform_row_to_tweet(row)
                    if not tweet_obj:
                        logger.warning("Skipped row due to transformation error.")
                        continue

                    
                    try:
                        self.messaging_service.send_message(
                            topic=settings.KAFKA_TOPIC_NAME,
                            message=tweet_obj.to_dict()
                        )
                    except Exception as e:
                        
                        logger.error(f"Error sending tweet {tweet_obj.tweet_id}: {e}")
                        continue
                    
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"Progress: {count} messages sent...")

                    
                    time.sleep(settings.STREAMING_DELAY)

                    
                    if settings.BATCH_LIMIT and count >= settings.BATCH_LIMIT:
                        break

        except FileNotFoundError:
            logger.error(f"File not found at path: {settings.CSV_FILE_PATH}")
        except Exception as e:
            logger.critical(f"Critical streaming error: {e}")
        finally:
            self._shutdown(count)

    def _shutdown(self, final_count):
        logger.info("Shutting down: flushing buffers...")
        self.messaging_service.flush()
        self.messaging_service.close()
        logger.info(f"Streaming finished. Total successfully processed: {final_count}")

    def stop(self):
        """Graceful shutdown method (e.g., triggered by Docker signal)"""
        self._is_running = False