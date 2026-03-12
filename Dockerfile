FROM python:3.11

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY migrate_notification_log_errors.py ./
COPY utils ./utils/

CMD ["python", "migrate_notification_log_errors.py"]
