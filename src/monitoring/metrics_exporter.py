import time
import logging
import os
import sys
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from kafka import KafkaConsumer, TopicPartition
import psycopg2
from config import settings

# Logging configuration
logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
logger = logging.getLogger("MetricsExporter")

# --- PROMETHEUS METRICS DEFINITIONS ---
MESSAGES_PROCESSED = Counter(
    'kafka_messages_processed_total', 
    'Total number of processed Kafka messages'
)
PROCESSING_TIME = Histogram(
    'message_processing_seconds', 
    'Time spent processing a single message'
)
POSTGRES_RECORDS = Gauge(
    'postgres_total_records', 
    'Total number of records in the system (aggregated from materialized view)'
)
KAFKA_LAG = Gauge(
    'kafka_consumer_lag', 
    'Kafka consumer lag per topic partition',
    ['topic', 'partition']
)

class MetricsCollector:
    """Class for periodic system metrics collection"""
    
    def __init__(self, port=8000):
        self.port = port

    def collect_postgres_metrics(self):
        """Collect data from RisingWave (instead of direct Postgres)"""
        conn = None
        try:
            # Using connection parameters from settings/environment
            conn = psycopg2.connect(
                host="risingwave",  # Connecting to RisingWave where the materialized view exists
                port=4566,
                database="dev",
                user="root",
                password="",
                connect_timeout=5
            )
            with conn.cursor() as cursor:
                # Query: sum tweet counts from materialized view
                cursor.execute("SELECT SUM(tweet_count) FROM tweets_per_minute;")
                result = cursor.fetchone()[0]
                count = int(result) if result else 0
                
                POSTGRES_RECORDS.set(count)
                logger.info(f"Metric updated: total_records = {count}")
        except Exception as e:
            logger.error(f"Error collecting database metrics: {e}")
        finally:
            if conn:
                conn.close()

    def collect_kafka_metrics(self):
        """Collect Kafka consumer lag metrics"""
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
                    
                    committed = consumer.committed(tp) or 0
                    end_offset = consumer.end_offsets([tp])[tp]
                    
                    lag = end_offset - committed
                    KAFKA_LAG.labels(topic=topic, partition=p_id).set(lag)
            
            logger.debug(f"Kafka lag collected for {topic}")
        except Exception as e:
            logger.error(f"Error collecting Kafka metrics: {e}")
        finally:
            if consumer:
                consumer.close()

    def run(self):
        """Start HTTP server and collection loop"""
        try:
            # Start Prometheus HTTP server on port 8000
            start_http_server(self.port)
            logger.info(f"Metrics server started on port {self.port}")
            
            while True:
                self.collect_postgres_metrics()
                self.collect_kafka_metrics()
                time.sleep(15)  # Reduced to 15 seconds for faster updates
        except KeyboardInterrupt:
            logger.info("Metrics exporter stopped.")
            sys.exit(0)

if __name__ == "__main__":
    collector = MetricsCollector(port=8000)
    collector.run()