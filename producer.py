from kafka import KafkaProducer
import pandas as pd
import json
import time

# =========================
# KAFKA PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# =========================
# LOAD DATASET
# =========================
print("Завантажую твіти...")

df = pd.read_csv('data/twcs.csv')

# 🔥 прибираємо NaN, щоб нічого не ламалось
df = df.fillna("")

print(f"Знайдено {len(df)} твітів")

# =========================
# SEND TO KAFKA
# =========================
count = 0

for _, row in df.head(1000).iterrows():

    text = str(row['text']).strip()

    # пропускаємо пусті твіти
    if not text:
        continue

    tweet = {
        "tweet_id": str(row["tweet_id"]),
        "author": str(row["author_id"]),
        "text": text,
        "inbound": bool(row["inbound"])
    }

    try:
        producer.send("tweets", value=tweet)
        count += 1

        if count % 100 == 0:
            print(f"Відправлено {count} твіти")

        time.sleep(0.01)

    except Exception as e:
        print(f"❌ Помилка відправки: {e}")

# =========================
# FLUSH
# =========================
producer.flush()
producer.close()

print(f"Готово! Відправлено {count} твіти в Kafka")