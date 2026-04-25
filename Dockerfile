# 1. Базовий образ
FROM python:3.10-slim

# 2. Системні залежності
# Встановлюємо інструменти для роботи з мережею та компіляції драйверів БД
RUN apt-get update && apt-get install -y \
    netcat-openbsd \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 3. Робоча директорія
WORKDIR /app

# 4. Встановлення бібліотек
# Включаємо все: від обробки даних до аналізу тексту та метрик
RUN pip install --no-cache-dir \
    pandas \
    kafka-python \
    python-dotenv \
    psycopg2-binary \
    prometheus-client \
    textblob

# 5. Копіювання всього проекту
# Копіюємо всі файли (src, config, data тощо) всередину образу
COPY . .

# 6. Healthcheck
# Використовуємо твій скрипт для моніторингу стану контейнера
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python /app/src/monitoring/healthcheck.py || exit 1

# 7. Запуск за замовчуванням
# Ця команда буде виконана, якщо інша не вказана в docker-compose
CMD ["python", "producer_main.py"]