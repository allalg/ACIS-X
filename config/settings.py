import os


# =========================================================
# Kafka Configuration
# =========================================================

ACIS_KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "ACIS_KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

ACIS_KAFKA_BACKEND = os.getenv(
    "ACIS_KAFKA_BACKEND",
    "confluent",
)


# =========================================================
# Agent Configuration
# =========================================================

DEFAULT_MAX_REPLICAS = int(
    os.getenv("ACIS_DEFAULT_MAX_REPLICAS", "3")
)

DECISION_INTERVAL = int(
    os.getenv("ACIS_DECISION_INTERVAL", "15")
)


# =========================================================
# Self-Healing / Monitoring
# =========================================================

HEARTBEAT_INTERVAL = int(
    os.getenv("ACIS_HEARTBEAT_INTERVAL", "5")
)

HEALTH_SCORE_THRESHOLD = float(
    os.getenv("ACIS_HEALTH_SCORE_THRESHOLD", "0.6")
)


# =========================================================
# Logging
# =========================================================

LOG_LEVEL = os.getenv(
    "ACIS_LOG_LEVEL",
    "INFO",
)