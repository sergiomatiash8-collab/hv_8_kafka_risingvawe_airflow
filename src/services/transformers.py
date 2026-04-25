import logging
from datetime import datetime
from src.domain.producer_models import Tweet


logger = logging.getLogger("TransformerService")

def transform_row_to_tweet(row: dict) -> Tweet:
    """
    Transforms a CSV row dictionary into a Tweet object with validation and logging.
    """
    if not row:
        logger.warning("Received empty row for transformation.")
        return None

    try:
        
        tweet = Tweet(
            tweet_id=str(row.get("tweet_id")),
            author=str(row.get("author_id", "unknown")),
            text=str(row.get("text", "")).strip(),
            # Safe conversion of "True"/"False" strings to boolean
            inbound=str(row.get("inbound")).lower() == 'true',
            created_at=datetime.now().isoformat()
        )
        
        
        logger.debug(f"Transformed tweet: {tweet.tweet_id}")
        return tweet

    except KeyError as e:
        logger.error(f"Missing required field in input data: {e}")
    except Exception as e:
        
        logger.error(f"Unexpected transformation error for row {row.get('tweet_id')}: {e}")
    
    return None