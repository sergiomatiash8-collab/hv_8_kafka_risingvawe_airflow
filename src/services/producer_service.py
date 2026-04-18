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
        logger.info(f"🚀 Починаємо стрімінг із файлу: {settings.CSV_FILE_PATH}")
        
        try:
            with open(settings.CSV_FILE_PATH, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0
                
                for row in reader:
                    if not self._is_running:
                        break

                    # 1. Трансформація з обробкою None
                    tweet_obj = transform_row_to_tweet(row)
                    if not tweet_obj:
                        logger.warning(f"⚠️ Пропущено рядок через помилку трансформації.")
                        continue

                    # 2. Відправка
                    try:
                        self.messaging_service.send_message(
                            topic=settings.KAFKA_TOPIC_NAME,
                            message=tweet_obj.to_dict()
                        )
                    except Exception as e:
                        # Recovery state: якщо не вдалося відправити, пробуємо наступний
                        logger.error(f"❌ Помилка відправки твіта {tweet_obj.tweet_id}: {e}")
                        continue
                    
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"📊 Прогрес: відправлено {count} повідомлень...")

                    # 3. Контроль швидкості (Rate Limiting)
                    time.sleep(settings.STREAMING_DELAY)

                    # Обмеження для тестів (Batch Limit)
                    if settings.BATCH_LIMIT and count >= settings.BATCH_LIMIT:
                        break

        except FileNotFoundError:
            logger.error(f"🚨 Файл не знайдено за шляхом: {settings.CSV_FILE_PATH}")
        except Exception as e:
            logger.critical(f"💥 Критична помилка стрімінгу: {e}")
        finally:
            self._shutdown(count)

    def _shutdown(self, final_count):
        logger.info("⏳ Завершення роботи: очищення буферів...")
        self.messaging_service.flush()
        self.messaging_service.close()
        logger.info(f"🏁 Стрімінг завершено. Всього успішно оброблено: {final_count}")

    def stop(self):
        """Метод для м'якої зупинки (наприклад, по сигналу від Docker)"""
        self._is_running = False