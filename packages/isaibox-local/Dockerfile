FROM python:3.10-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ISAIBOX_HOST=0.0.0.0 \
    ISAIBOX_PORT=8000 \
    ISAIBOX_LOG_LEVEL=info \
    ISAIBOX_LOCAL_MODE=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-local.txt ./
RUN pip install --no-cache-dir -r requirements-local.txt

COPY app/ ./

RUN mkdir -p .cache/audio exports data

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD curl -fsS http://127.0.0.1:8000/api/health || exit 1

CMD ["python", "app.py"]
