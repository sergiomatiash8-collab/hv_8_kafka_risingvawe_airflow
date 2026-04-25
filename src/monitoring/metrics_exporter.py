import time
import logging
import os
import sys
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from kafka import KafkaConsumer, TopicPartition
import psycopg2
from config import settings

# Налаштування логування
logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
logger = logging.getLogger("MetricsExporter")

# --- ВИЗНАЧЕННЯ МЕТРИК PROMETHUES ---
MESSAGES_PROCESSED = Counter(
    'kafka_messages_processed_total', 
    'Загальна кількість оброблених повідомлень з Kafka'
)
PROCESSING_TIME = Histogram(
    'message_processing_seconds', 
    'Час витрачений на обробку одного повідомлення'
)
POSTGRES_RECORDS = Gauge(
    'postgres_total_records', 
    'Загальна кількість записів у таблиці tweets'
)
KAFKA_LAG = Gauge(
    'kafka_consumer_lag', 
    'Відставання (lag) консюмера в топіку Kafka',
    ['topic', 'partition']
)

class MetricsCollector:
    """Клас для періодичного збору метрик системи"""
    
    def __init__(self, port=8000):
        self.port = port

    def collect_postgres_metrics(self):
        """Збір даних з PostgreSQL"""
        conn = None
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="twitter_sentiment",
                user="admin",
                password=os.getenv("POSTGRES_PASSWORD", "admin123"),
                connect_timeout=5
            )
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM tweets;")
                count = cursor.fetchone()[0]
                POSTGRES_RECORDS.set(count)
            logger.debug(f"📊 Postgres records: {count}")
        except Exception as e:
            logger.error(f"❌ Помилка збору метрик Postgres: {e}")
        finally:
            if conn:
                conn.close()

    def collect_kafka_metrics(self):
        """Збір даних про лаг у Kafka"""
        consumer = None
        topic = settings.KAFKA_TOPIC_NAME
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                enable_auto_commit=False
            )
            
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                for p_id in partitions:
                    tp = TopicPartition(topic, p_id)
                    consumer.assign([tp])
                    
                    # Отримуємо останній зміщений офсет та поточний зафіксований
                    committed = consumer.committed(tp) or 0
                    end_offset = consumer.end_offsets([tp])[tp]
                    
                    lag = end_offset - committed
                    KAFKA_LAG.labels(topic=topic, partition=p_id).set(lag)
            
            logger.debug(f"📊 Kafka lag collected for {topic}")
        except Exception as e:
            logger.error(f"❌ Помилка збору метрик Kafka: {e}")
        finally:
            if consumer:
                consumer.close()

    def run(self):
        """Запуск HTTP-сервера та нескінченного циклу оновлення"""
        try:
            start_http_server(self.port)
            logger.info(f"🚀 Metrics server started on port {self.port}")
            
            while True:
                self.collect_postgres_metrics()
                self.collect_kafka_metrics()
                time.sleep(30) # Оновлюємо кожні 30 секунд
        except KeyboardInterrupt:
            logger.info("🛑 Metrics exporter зупинено.")
            sys.exit(0)

if __name__ == "__main__":
    collector = MetricsCollector(port=8000)
    collector.run()