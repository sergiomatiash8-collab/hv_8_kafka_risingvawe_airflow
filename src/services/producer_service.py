import pandas as pd
import time
from src.services.transformers import transform_row_to_tweet
from config import settings

class TweetStreamingService:
    def __init__(self, messaging_service):
        """
        Ми передаємо (inject) сервіс повідомлень. 
        Це і є Dependency Injection.
        """
        self.messaging_service = messaging_service

    def start_streaming(self):
        print(f"🚀 Починаємо стрімінг із файлу: {settings.CSV_FILE_PATH}")
        
        # Читаємо дані (використовуємо налаштування з config)
        df = pd.read_csv(settings.CSV_FILE_PATH).fillna("").head(settings.BATCH_LIMIT)
        
        count = 0
        for _, row in df.iterrows():
            # 1. Трансформація
            tweet_obj = transform_row_to_tweet(row)
            
            # 2. Відправка (через наш інтерфейс)
            self.messaging_service.send_message(
                topic=settings.KAFKA_TOPIC_NAME,
                message=tweet_obj.to_dict()
            )
            
            count += 1
            if count % 100 == 0:
                print(f"✅ Відправлено {count} твітів...")

            # 3. Затримка
            time.sleep(settings.STREAMING_DELAY)

        self.messaging_service.flush()
        print(f"🏁 Стрімінг завершено. Всього: {count}")