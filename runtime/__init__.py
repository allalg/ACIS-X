"""Runtime infrastructure for ACIS-X."""

from runtime.kafka_client import (
    KafkaClient,
    KafkaConfig,
    KafkaMessage,
    SerializationFormat,
)
from runtime.retry_strategy import (
    RetryStrategy,
    RetryConfig,
    retry_with_backoff,
    calculate_backoff_delay,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)
from runtime.topic_manager import (
    TopicAdmin,
    TopicConfig,
    ACIS_TOPIC_CONFIGS,
)
# PlacementEngine and RuntimeManager are not exported here to avoid circular imports
# Import them directly: from runtime.placement_engine import PlacementEngine

__all__ = [
    # Kafka client
    "KafkaClient",
    "KafkaConfig",
    "KafkaMessage",
    "SerializationFormat",
    # Retry
    "RetryStrategy",
    "RetryConfig",
    "retry_with_backoff",
    "calculate_backoff_delay",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    # Topics
    "TopicAdmin",
    "TopicConfig",
    "ACIS_TOPIC_CONFIGS",
]
