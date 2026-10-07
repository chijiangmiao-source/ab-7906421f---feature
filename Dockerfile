FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/handoff.db \
    QUIET=1

WORKDIR /app

COPY app/ ./app/
COPY tests/ ./tests/
COPY verify.py ./verify.py

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=3s --timeout=2s --start-period=2s --retries=10 \
    CMD ["python", "-m", "app.healthcheck"]

CMD ["python", "-m", "app.server"]
