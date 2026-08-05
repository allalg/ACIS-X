# ──────────────────────────────────────────────────────────────────────────────
# ACIS-X API BFF (Backend-For-Frontend)
# ──────────────────────────────────────────────────────────────────────────────
# FastAPI gateway that serves the React frontend via REST + SSE.
# Reads from acis.db (shared volume) and streams Kafka events to the browser.
#
# Build: docker build -t acis-bff .
# Run:   docker run --env-file .env -p 8000:8000 acis-bff
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# librdkafka not needed for BFF (uses aiokafka which is pure Python)
WORKDIR /app

# Install Python dependencies first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

ENV PYTHONUNBUFFERED=1
ENV ACIS_BFF_HOST=0.0.0.0
ENV ACIS_BFF_PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health')"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
