import logging
from datetime import datetime
from src.domain.producer_models import Tweet

# Ініціалізуємо логер для цього модуля
logger = logging.getLogger("TransformerService")

def transform_row_to_tweet(row: dict) -> Tweet:
    """
    Перетворює словник (рядок з CSV) в об'єкт Tweet з валідацією та логуванням.
    """
    if not row:
        logger.warning("⚠️ Отримано порожній рядок (row) для трансформації.")
        return None

    try:
        # Основна трансформація
        tweet = Tweet(
            tweet_id=str(row.get("tweet_id")),
            author=str(row.get("author_id", "unknown")),
            text=str(row.get("text", "")).strip(),
            # Для безпечного перетворення рядків "True"/"False" у булеві значення
            inbound=str(row.get("inbound")).lower() == 'true',
            created_at=datetime.now().isoformat()
        )
        
        # Це DEBUG рівень, щоб не спамити в основні логи, 
        # але мати можливість перевірити дані
        logger.debug(f"✅ Трансформовано твіт: {tweet.tweet_id}")
        return tweet

    except KeyError as e:
        logger.error(f"❌ Відсутнє обов'язкове поле у вхідних даних: {e}")
    except Exception as e:
        # Стан Error: ми не зупиняємо програму, але чітко фіксуємо проблему
        logger.error(f"🚨 Непередбачувана помилка трансформації рядка {row.get('tweet_id')}: {e}")
    
    return None