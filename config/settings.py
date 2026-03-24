import os

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_BACKEND = os.getenv("KAFKA_BACKEND", "kafka")

# Agent Configuration
DEFAULT_MAX_REPLICAS = int(os.getenv("DEFAULT_MAX_REPLICAS", "3"))
DECISION_INTERVAL = int(os.getenv("DECISION_INTERVAL", "15"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
