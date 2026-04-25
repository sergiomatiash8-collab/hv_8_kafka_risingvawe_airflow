
FROM python:3.10-slim


RUN apt-get update && apt-get install -y \
    netcat-openbsd \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app


RUN pip install --no-cache-dir \
    pandas \
    kafka-python \
    python-dotenv \
    psycopg2-binary \
    prometheus-client \
    textblob


COPY . .


HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python /app/src/monitoring/healthcheck.py || exit 1


CMD ["python", "producer_main.py"]