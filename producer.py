from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime

# =========================
# KAFKA PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

print("Завантажую твіти...")
df = pd.read_csv('data/twcs.csv')
df = df.fillna("")

# Визначаємо затримку для 12 повідомлень на секунду (1 / 12 ≈ 0.083s)
# Це відповідає вимозі 10-15 msg/sec 
msg_per_second = 12
delay = 1 / msg_per_second

count = 0

# Для тесту беремо head(1000) або прибираємо для повного стріму [cite: 86]
for _, row in df.head(1000).iterrows():
    text = str(row['text']).strip()
    if not text:
        continue

    # Крок 4: Заміна таймштампу на поточний 
    # Використовуємо ISO формат, який легко читається в RisingWave/Postgres
    current_time = datetime.now().isoformat()

    tweet = {
        "tweet_id": str(row["tweet_id"]),
        "author": str(row["author_id"]),
        "text": text,
        "inbound": bool(row["inbound"]),
        "created_at": current_time  # Додано нове поле [cite: 58]
    }

    try:
        producer.send("tweets", value=tweet)
        count += 1

        if count % 50 == 0:
            print(f"[{current_time}] Відправлено {count} твітів...")

        # Точне керування швидкістю стрімінгу 
        time.sleep(delay)

    except Exception as e:
        print(f"❌ Помилка відправки: {e}")

producer.flush()
producer.close()
print(f"Готово! Відправлено {count} твітів")