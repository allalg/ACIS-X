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
    os.getenv("ACIS_HEALTH_SCORE_THRESHOLD", "0.8")
)

# Self-Healing cooldown periods (seconds)
RESTART_COOLDOWN = int(
    os.getenv("ACIS_RESTART_COOLDOWN", "120")
)

SCALE_COOLDOWN = int(
    os.getenv("ACIS_SCALE_COOLDOWN", "180")
)

SPAWN_COOLDOWN = int(
    os.getenv("ACIS_SPAWN_COOLDOWN", "180")
)

FALLBACK_COOLDOWN = int(
    os.getenv("ACIS_FALLBACK_COOLDOWN", "120")
)

RECOVERY_EVENT_COOLDOWN = int(
    os.getenv("ACIS_RECOVERY_EVENT_COOLDOWN", "60")
)

PLACEMENT_REQUEST_COOLDOWN = int(
    os.getenv("ACIS_PLACEMENT_REQUEST_COOLDOWN", "180")
)

# Self-Healing thresholds
DEGRADED_RESTART_DELAY = int(
    os.getenv("ACIS_DEGRADED_RESTART_DELAY", "30")
)

LAG_SCALE_THRESHOLD = int(
    os.getenv("ACIS_LAG_SCALE_THRESHOLD", "50")    # FIX 3: was 5000 – unreachable in single-process sim
)

CRITICAL_LAG_THRESHOLD = int(
    os.getenv("ACIS_CRITICAL_LAG_THRESHOLD", "200")  # FIX 3: was 10000 – unreachable in single-process sim
)


# =========================================================
# Logging
# =========================================================

LOG_LEVEL = os.getenv(
    "ACIS_LOG_LEVEL",
    "INFO",
)