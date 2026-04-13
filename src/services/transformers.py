from datetime import datetime
from src.domain.models import Tweet

def transform_row_to_tweet(row: dict) -> Tweet:
    """
    Перетворює словник (рядок з CSV) в об'єкт Tweet.
    Тут ми концентруємо всю логіку 'чистки' даних.
    """
    return Tweet(
        tweet_id=str(row.get("tweet_id")),
        author=str(row.get("author_id", "unknown")),
        text=str(row.get("text", "")).strip(),
        inbound=bool(row.get("inbound")),
        created_at=datetime.now().isoformat()
    )