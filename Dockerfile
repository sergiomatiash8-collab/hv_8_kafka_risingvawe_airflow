# 1. Базовий образ
FROM python:3.10-slim

# 2. Системні залежності
RUN apt-get update && apt-get install -y \
    netcat-openbsd \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 3. Робоча директорія
WORKDIR /app

# 4. Встановлення бібліотек
# Додано psycopg2-binary та prometheus-client для моніторингу
RUN pip install --no-cache-dir \
    pandas \
    kafka-python \
    python-dotenv \
    psycopg2-binary \
    prometheus-client

# 5. Копіювання всього проекту
# Ми копіюємо корінь, щоб були доступні і producer, і consumer
COPY . .

# 6. Healthcheck (залишаємо твій комплексний варіант)
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python /app/src/monitoring/healthcheck.py || exit 1

# 7. Запуск за замовчуванням (можна перевизначити в docker-compose)
CMD ["python", "producer_main.py"]