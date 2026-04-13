import pytest

# Функція, яку ми тестуємо (потім ми її імпортуємо з твого файлу)
def preprocess_tweet(row):
    return {
        "tweet_id": str(row.get("tweet_id")),
        "author": row.get("author", "unknown").lower(),
        "text": row.get("text", "").strip(),
        "inbound": bool(row.get("inbound"))
    }

# Сам тест
def test_preprocess_tweet_valid_data():
    raw_data = {"tweet_id": 123, "author": "UserABC", "text": "  Hello Kafka!  ", "inbound": 1}
    result = preprocess_tweet(raw_data)
    
    assert result["tweet_id"] == "123"      # Перевіряємо, чи стало рядком
    assert result["author"] == "userabc"    # Перевіряємо, чи став нижній регістр
    assert result["text"] == "Hello Kafka!" # Перевіряємо, чи прибрались пробіли
    assert result["inbound"] is True        # Перевіряємо перетворення в bool

def test_preprocess_tweet_missing_data():
    # Ситуація: прийшов порожній об'єкт
    raw_data = {}
    result = preprocess_tweet(raw_data)
    
    assert result["tweet_id"] == "None"      # Перевіряємо, що не вибило помилку
    assert result["author"] == "unknown"     # Значення за замовчуванням
    assert result["text"] == ""              # Порожній рядок замість помилки
    assert result["inbound"] is False        # False за замовчуванням