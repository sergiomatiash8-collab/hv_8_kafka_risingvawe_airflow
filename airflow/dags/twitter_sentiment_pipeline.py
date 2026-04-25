"""
Apache Airflow DAG для управління Twitter Sentiment Pipeline.
Це головний файл-інструкція для диригента (Airflow).
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator

# Налаштування логування всередині Airflow
logger = logging.getLogger(__name__)

# 1. Налаштування логіки за замовчуванням для всіх завдань (Tasks)
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,      # Чи залежить сьогоднішній запуск від вчорашнього
    'email_on_failure': False,     # Чи слати імейл при помилці
    'retries': 1,                  # Скільки разів пробувати перездати, якщо впало
    'retry_delay': timedelta(minutes=5),
}

# --- ФУНКЦІЇ-ОПЕРАТОРИ (ЛОГІКА ЗАВДАНЬ) ---

def check_system_health(**context):
    """
    Крок 1: Санітарна перевірка.
    Ми перевіряємо чи 'живі' Kafka та Postgres перед стартом.
    """
    import socket
    from kafka import KafkaProducer
    import psycopg2
    
    # Перевірка Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            request_timeout_ms=5000
        )
        producer.close()
        logger.info("✅ Kafka healthy")
    except Exception as e:
        logger.error(f"❌ Kafka failed: {e}")
        raise  # Якщо помилка - Airflow зупинить цей запуск

    # Перевірка PostgreSQL
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="twitter_sentiment",
            user="admin",
            password="admin123", # Потім замінимо на секрет
            connect_timeout=5
        )
        conn.close()
        logger.info("✅ PostgreSQL healthy")
    except Exception as e:
        logger.error(f"❌ PostgreSQL failed: {e}")
        raise

def validate_data_quality(**context):
    """
    Крок 3: Перевірка якості даних.
    Заходимо в базу і дивимось, чи не налетіло сміття.
    """
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", database="twitter_sentiment",
        user="admin", password="admin123"
    )
    cursor = conn.cursor()
    
    # Рахуємо кількість дублікатів по tweet_id
    cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT tweet_id) FROM tweets;")
    duplicates = cursor.fetchone()[0]
    
    conn.close()
    
    if duplicates > 100:
        raise Exception(f"🚨 Забагато дублікатів: {duplicates}!")
    logger.info(f"✅ Якість даних в нормі. Дублікатів: {duplicates}")

# --- ВИЗНАЧЕННЯ ПАЙПЛАЙНУ (DAG) ---

with DAG(
    'twitter_sentiment_pipeline',
    default_args=default_args,
    description='Конвеєр аналізу твітів: Kafka -> RisingWave -> Postgres',
    schedule_interval='@hourly',      # Запуск кожну годину
    start_date=datetime(2026, 4, 1),  # Починаємо з квітня 2026 року
    catchup=False,                    # Не 'доганяти' минулі дати
    tags=['production', 'twitter'],
) as dag:

    # Завдання 1: Перевірка підключень
    t1_health = PythonOperator(
        task_id='system_health_check',
        python_callable=check_system_health
    )

    # Завдання 2: Запуск Продюсера (Docker в Docker)
    # Ця команда створює окремий контейнер для відправки порції твітів
    t2_run_producer = DockerOperator(
        task_id='start_kafka_producer',
        image='producer_app:latest',   # Твій зібраний образ
        api_version='auto',
        auto_remove=True,              # Видалити контейнер після виконання
        network_mode='streaming_net',  # Та сама мережа, що й Kafka
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'BATCH_LIMIT': '500'       # Відправити лише 500 штук за раз
        },
        docker_url='unix://var/run/docker.sock',
    )

    # Завдання 3: Валідація результатів у базі
    t3_validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality
    )

    # ЛАНЦЮЖОК: t1 спочатку, потім t2, потім t3
    t1_health >> t2_run_producer >> t3_validate