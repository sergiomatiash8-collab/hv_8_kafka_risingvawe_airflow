import pytest
from confluent_kafka import Producer

def test_kafka_connectivity():
    
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'test-producer'
    }
    
    producer = Producer(conf)
    
    
    report = {"status": None}
    
    def delivery_report(err, msg):
        if err is not None:
            report["status"] = f"Error: {err}"
        else:
            report["status"] = "Success"

    
    producer.produce('tweets', key='test', value='test_message', callback=delivery_report)
    
    
    producer.flush(5)
    
    
    assert report["status"] == "Success", f"Kafka connection failed: {report['status']}"