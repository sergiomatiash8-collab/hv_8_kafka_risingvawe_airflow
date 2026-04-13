import json
import psycopg2
from kafka import KafkaConsumer
from textblob import TextBlob
import sys

# =========================
# KAFKA
# =========================
consumer = KafkaConsumer(
    "tweets",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sentiment-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8", errors="ignore"))
)

# =========================
# POSTGRES (ПОВНИЙ ФІКС)
# =========================
try:
    # Використовуємо порт 5454
    conn = psycopg2.connect(
        user="admin",
        password="admin123",
        host="127.0.0.1",
        port=5454, 
        database="twitter_sentiment"
    )
    conn.set_client_encoding('UTF8')
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweet_sentiments (
        tweet_id TEXT PRIMARY KEY,
        author TEXT,
        text TEXT,
        sentiment TEXT,
        polarity FLOAT
    )
    """)
    conn.commit()
    print("🚀 Consumer та Postgres готові!")

except Exception as e:
    # Цей блок виведе помилку, навіть якщо Windows шле бінарне сміття
    print("❌ Помилка підключення!")
    try:
        print(f"Текст помилки: {str(e).encode('utf-8', 'replace').decode('utf-8')}")
    except:
        print("Не вдалося розшифрувати текст помилки.")
    sys.exit(1)

# =========================
# LOOP
# =========================
for msg in consumer:
    try:
        tweet = msg.value
        text = tweet.get("text", "")
        if not text: continue

        polarity = TextBlob(text).sentiment.polarity
        sentiment = "positive" if polarity > 0.1 else "negative" if polarity < -0.1 else "neutral"

        cursor.execute("""
            INSERT INTO tweet_sentiments (tweet_id, author, text, sentiment, polarity)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tweet_id) DO NOTHING
        """, (str(tweet.get("tweet_id", "")), str(tweet.get("author", "")), text, sentiment, polarity))
        conn.commit()
        print(f"✅ {sentiment}: {text[:50]}...")
    except Exception as e:
        print(f"❌ Помилка: {e}")