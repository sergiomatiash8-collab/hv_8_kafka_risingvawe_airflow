
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator


logger = logging.getLogger(__name__)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,      
    'email_on_failure': False,     
    'retries': 1,                  
    'retry_delay': timedelta(minutes=5),
}


def check_system_health(**context):
    """
    Kafka and Postgres avalability check
    """
    import socket
    from kafka import KafkaProducer
    import psycopg2
    
    
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            request_timeout_ms=5000
        )
        producer.close()
        logger.info(" Kafka healthy")
    except Exception as e:
        logger.error(f" Kafka failed: {e}")
        raise  

    
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="twitter_sentiment",
            user="admin",
            password="admin123", # Потім замінимо на секрет
            connect_timeout=5
        )
        conn.close()
        logger.info(" PostgreSQL healthy")
    except Exception as e:
        logger.error(f" PostgreSQL failed: {e}")
        raise

def validate_data_quality(**context):
    """
    DQ
    """
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", database="twitter_sentiment",
        user="admin", password="admin123"
    )
    cursor = conn.cursor()
    
    
    cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT tweet_id) FROM tweets;")
    duplicates = cursor.fetchone()[0]
    
    conn.close()
    
    if duplicates > 100:
        raise Exception(f"Many duplicates: {duplicates}!")
    logger.info(f"dq ok. Duplicates: {duplicates}")



with DAG(
    'twitter_sentiment_pipeline',
    default_args=default_args,
    description='Conveyer analysis: Kafka -> RisingWave -> Postgres',
    schedule_interval='@hourly',      
    start_date=datetime(2026, 4, 1),  
    catchup=False,                    
    tags=['production', 'twitter'],
) as dag:

    
    t1_health = PythonOperator(
        task_id='system_health_check',
        python_callable=check_system_health
    )

    
    t2_run_producer = DockerOperator(
        task_id='start_kafka_producer',
        image='producer_app:latest',   
        api_version='auto',
        auto_remove=True,              
        network_mode='streaming_net',  
        environment={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'BATCH_LIMIT': '500'       
        },
        docker_url='unix://var/run/docker.sock',
    )

   
    t3_validate = PythonOperator(
        task_id='validate_data_quality',
        python_callable=validate_data_quality
    )

    
    t1_health >> t2_run_producer >> t3_validate