FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DB_PATH=/tmp/trading_bot.db \
    SESSION_PATH=/data/session_name \
    SQLITE_JOURNAL_MODE=DELETE \
    SQLITE_BUSY_TIMEOUT_MS=30000 \
    SQLITE_LOCK_RETRY_SEC=180

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY app ./app
COPY src ./src

VOLUME ["/data"]

CMD ["python", "-m", "app.main"]
