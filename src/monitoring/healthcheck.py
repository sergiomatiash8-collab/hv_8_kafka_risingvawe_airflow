import sys
import socket
import logging
import os
from kafka import KafkaProducer
import psycopg2
from config import settings

# Налаштування логування для Docker logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("HealthCheck")

def check_kafka():
    """Перевірка доступності Kafka через спробу підключення"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000
        )
        producer.close()
        logger.info("✅ Kafka: Healthy")
        return True
    except Exception as e:
        logger.error(f"❌ Kafka: Unhealthy ({e})")
        return False

def check_port(host, port, service_name):
    """Перевірка, чи відкритий TCP порт сервісу"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(3)
            result = sock.connect_ex((host, int(port)))
            if result == 0:
                logger.info(f"✅ {service_name} (port {port}): Open")
                return True
            else:
                logger.error(f"❌ {service_name} (port {port}): Closed")
                return False
    except Exception as e:
        logger.error(f"❌ {service_name} check failed: {e}")
        return False

def check_postgres():
    """Перевірка підключення до PostgreSQL"""
    try:
        # Беремо дані з твого об'єкту settings
        conn = psycopg2.connect(
            host="postgres",  # Назва сервісу в docker-compose
            port=5432,
            database="twitter_sentiment",
            user="admin",
            # В Docker пароль зазвичай у файлі або в ENV
            password=os.getenv("POSTGRES_PASSWORD", "admin123"), 
            connect_timeout=3
        )
        conn.close()
        logger.info("✅ PostgreSQL: Healthy")
        return True
    except Exception as e:
        logger.error(f"❌ PostgreSQL: Unhealthy ({e})")
        return False

def main():
    """Збірний запуск усіх перевірок"""
    logger.info("🔍 Starting system health check...")
    
    # Виконуємо всі перевірки
    results = [
        check_kafka(),
        check_port("zookeeper", 2181, "Zookeeper"),
        check_postgres()
    ]
    
    if all(results):
        logger.info("🟢 ALL SYSTEMS GO")
        sys.exit(0)
    else:
        logger.error("🔴 SYSTEM UNHEALTHY")
        sys.exit(1)

if __name__ == "__main__":
    main()