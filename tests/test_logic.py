import pytest
from src.services.transformers import transform_row_to_tweet
from src.domain.models import Tweet

def test_transform_row_to_tweet_valid():
    
    raw_data = {
        "tweet_id": 123, 
        "author_id": "UserABC", 
        "text": "  Hello Kafka!  ", 
        "inbound": 1
    }
    
    
    result = transform_row_to_tweet(raw_data)
    
    
    assert isinstance(result, Tweet) 
    assert result.tweet_id == "123"
    assert result.author == "UserABC" 
    assert result.text == "Hello Kafka!"
    assert result.inbound is True

def test_transform_row_to_tweet_missing_data():
    raw_data = {} 
    result = transform_row_to_tweet(raw_data)
    
    assert result.tweet_id == "None"
    assert result.author == "unknown"
    assert result.text == ""
    assert result.inbound is False