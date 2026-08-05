# ──────────────────────────────────────────────────────────────────────────────
# ACIS-X Core Agent Engine
# ──────────────────────────────────────────────────────────────────────────────
# Runs run_acis.py which spawns all multi-agent processes (DBAgent,
# ScenarioGenerator, PaymentPrediction, etc.) in a supervised loop.
#
# Build: docker build -t acis-engine .
# Run:   docker run --env-file .env --network host acis-engine
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

# System dependencies for confluent-kafka (librdkafka) and lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create directory for SQLite database (volume mount point)
RUN mkdir -p /data

ENV ACIS_DB_PATH=/app/acis.db
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import os, sys; p = os.getenv('ACIS_DB_PATH', '/app/acis.db'); sys.exit(0 if os.path.exists(p) else 1)"

CMD ["python", "run_acis.py"]
