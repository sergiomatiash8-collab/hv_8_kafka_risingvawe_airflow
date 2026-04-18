# 1. Базовий образ
FROM python:3.10-slim

# 2. Системні залежності
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

# 3. Робоча директорія
WORKDIR /app

# 4. Встановлення бібліотек
RUN pip install --no-cache-dir \
    pandas \
    kafka-python \
    python-dotenv

# 5. Копіювання файлів (враховуючи твою структуру на скріншоті)
COPY ./src /app/src
COPY ./config.py /app/config.py
COPY ./producer_main.py /app/producer_main.py
COPY ./tweets.csv /app/tweets.csv

# 6. Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python /app/src/monitoring/healthcheck.py || exit 1

# 7. Запуск (змінено на твій реальний файл)
CMD ["python", "producer_main.py"]