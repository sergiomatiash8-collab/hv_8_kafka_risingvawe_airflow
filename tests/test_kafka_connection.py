import pytest
from confluent_kafka import Producer

def test_kafka_connectivity():
    # Налаштування для підключення до твого Docker-контейнера
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'test-producer'
    }
    
    producer = Producer(conf)
    
    # Функція зворотного зв'язку (callback)
    report = {"status": None}
    
    def delivery_report(err, msg):
        if err is not None:
            report["status"] = f"Error: {err}"
        else:
            report["status"] = "Success"

    # Відправляємо тестове повідомлення в топік 'tweets' (він є у твоєму docker-compose)
    producer.produce('tweets', key='test', value='test_message', callback=delivery_report)
    
    # Чекаємо відповіді від Kafka (максимум 5 секунд)
    producer.flush(5)
    
    # Головна перевірка: статус має бути Success
    assert report["status"] == "Success", f"Kafka connection failed: {report['status']}"