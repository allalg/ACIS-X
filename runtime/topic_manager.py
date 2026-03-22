"""
Kafka topic management utilities for ACIS-X.

Provides topic creation, configuration, and admin operations.
"""

import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# =============================================================================
# Topic Configuration
# =============================================================================

@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""

    name: str
    partitions: int = 3
    replication_factor: int = 1
    retention_ms: Optional[int] = None  # None = infinite
    retention_bytes: Optional[int] = None
    cleanup_policy: str = "delete"  # delete or compact
    compression_type: str = "gzip"
    min_insync_replicas: int = 1

    # Additional topic configs
    segment_ms: Optional[int] = None
    segment_bytes: Optional[int] = None
    max_message_bytes: Optional[int] = None

    def to_config_dict(self) -> Dict[str, str]:
        """Convert to Kafka topic config dictionary."""
        config = {
            "cleanup.policy": self.cleanup_policy,
            "compression.type": self.compression_type,
            "min.insync.replicas": str(self.min_insync_replicas),
        }

        if self.retention_ms is not None:
            config["retention.ms"] = str(self.retention_ms)
        if self.retention_bytes is not None:
            config["retention.bytes"] = str(self.retention_bytes)
        if self.segment_ms is not None:
            config["segment.ms"] = str(self.segment_ms)
        if self.segment_bytes is not None:
            config["segment.bytes"] = str(self.segment_bytes)
        if self.max_message_bytes is not None:
            config["max.message.bytes"] = str(self.max_message_bytes)

        return config


# =============================================================================
# ACIS-X Topic Definitions
# =============================================================================

# Standard topic configurations for ACIS-X
ACIS_TOPIC_CONFIGS = {
    # Business event topics (high volume, shorter retention)
    "acis.invoices": TopicConfig(
        name="acis.invoices",
        partitions=6,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,  # 7 days
        cleanup_policy="delete",
    ),
    "acis.payments": TopicConfig(
        name="acis.payments",
        partitions=6,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.customers": TopicConfig(
        name="acis.customers",
        partitions=3,
        replication_factor=3,
        retention_ms=30 * 24 * 3600 * 1000,  # 30 days
        cleanup_policy="compact",  # Latest customer state only
    ),
    "acis.risk": TopicConfig(
        name="acis.risk",
        partitions=4,
        replication_factor=3,
        retention_ms=14 * 24 * 3600 * 1000,  # 14 days
        cleanup_policy="delete",
    ),
    "acis.policy": TopicConfig(
        name="acis.policy",
        partitions=2,
        replication_factor=3,
        retention_ms=30 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.external": TopicConfig(
        name="acis.external",
        partitions=3,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.commands": TopicConfig(
        name="acis.commands",
        partitions=3,
        replication_factor=3,
        retention_ms=1 * 24 * 3600 * 1000,  # 1 day (commands are ephemeral)
        cleanup_policy="delete",
    ),

    # System topics (longer retention)
    "acis.system": TopicConfig(
        name="acis.system",
        partitions=4,
        replication_factor=3,
        retention_ms=30 * 24 * 3600 * 1000,  # 30 days
        cleanup_policy="delete",
    ),
    "acis.agent.health": TopicConfig(
        name="acis.agent.health",
        partitions=2,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.registry": TopicConfig(
        name="acis.registry",
        partitions=1,
        replication_factor=3,
        retention_ms=-1,  # Infinite retention
        cleanup_policy="compact",  # Keep latest agent state
    ),

    # DLQ topics (very long retention)
    "acis.invoices.dlq": TopicConfig(
        name="acis.invoices.dlq",
        partitions=1,
        replication_factor=3,
        retention_ms=90 * 24 * 3600 * 1000,  # 90 days
        cleanup_policy="delete",
    ),
    "acis.payments.dlq": TopicConfig(
        name="acis.payments.dlq",
        partitions=1,
        replication_factor=3,
        retention_ms=90 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.risk.dlq": TopicConfig(
        name="acis.risk.dlq",
        partitions=1,
        replication_factor=3,
        retention_ms=90 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
    "acis.system.dlq": TopicConfig(
        name="acis.system.dlq",
        partitions=1,
        replication_factor=3,
        retention_ms=90 * 24 * 3600 * 1000,
        cleanup_policy="delete",
    ),
}


# =============================================================================
# Topic Admin Operations
# =============================================================================

class TopicAdmin:
    """Admin operations for Kafka topics."""

    def __init__(self, bootstrap_servers: List[str], backend: str = "confluent"):
        """
        Initialize topic admin client.

        Args:
            bootstrap_servers: Kafka broker addresses
            backend: Kafka library (confluent or kafka-python)
        """
        self.bootstrap_servers = bootstrap_servers
        self.backend = backend
        self._admin_client: Optional[Any] = None

        if backend == "confluent":
            self._init_confluent_admin()
        elif backend == "kafka-python":
            self._init_kafka_python_admin()

    def _init_confluent_admin(self) -> None:
        """Initialize confluent-kafka admin client."""
        try:
            from confluent_kafka.admin import AdminClient

            config = {
                "bootstrap.servers": ",".join(self.bootstrap_servers),
            }
            self._admin_client = AdminClient(config)
            logger.info("Confluent Kafka admin client initialized")

        except ImportError:
            logger.error("confluent-kafka-python not installed")
            raise

    def _init_kafka_python_admin(self) -> None:
        """Initialize kafka-python admin client."""
        try:
            from kafka import KafkaAdminClient

            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
            )
            logger.info("kafka-python admin client initialized")

        except ImportError:
            logger.error("kafka-python not installed")
            raise

    def create_topic(self, topic_config: TopicConfig) -> bool:
        """
        Create a Kafka topic.

        Args:
            topic_config: Topic configuration

        Returns:
            True if created successfully, False otherwise
        """
        try:
            if self.backend == "confluent":
                return self._create_topic_confluent(topic_config)
            elif self.backend == "kafka-python":
                return self._create_topic_kafka_python(topic_config)
            return False

        except Exception as e:
            logger.error(f"Failed to create topic {topic_config.name}: {e}")
            return False

    def _create_topic_confluent(self, topic_config: TopicConfig) -> bool:
        """Create topic using confluent-kafka."""
        from confluent_kafka.admin import NewTopic

        new_topic = NewTopic(
            topic=topic_config.name,
            num_partitions=topic_config.partitions,
            replication_factor=topic_config.replication_factor,
            config=topic_config.to_config_dict(),
        )

        futures = self._admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            try:
                future.result()  # Block until done
                logger.info(f"Topic created: {topic}")
                return True
            except Exception as e:
                if "TopicExistsError" in str(e):
                    logger.info(f"Topic already exists: {topic}")
                    return True
                logger.error(f"Failed to create topic {topic}: {e}")
                return False
        return False

    def _create_topic_kafka_python(self, topic_config: TopicConfig) -> bool:
        """Create topic using kafka-python."""
        from kafka.admin import NewTopic

        new_topic = NewTopic(
            name=topic_config.name,
            num_partitions=topic_config.partitions,
            replication_factor=topic_config.replication_factor,
            topic_configs=topic_config.to_config_dict(),
        )

        try:
            self._admin_client.create_topics([new_topic])
            logger.info(f"Topic created: {topic_config.name}")
            return True
        except Exception as e:
            if "TopicExistsError" in str(e):
                logger.info(f"Topic already exists: {topic_config.name}")
                return True
            logger.error(f"Failed to create topic: {e}")
            return False

    def list_topics(self) -> List[str]:
        """List all topics."""
        try:
            if self.backend == "confluent":
                metadata = self._admin_client.list_topics(timeout=10)
                return list(metadata.topics.keys())
            elif self.backend == "kafka-python":
                return list(self._admin_client.list_topics())
            return []

        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []

    def delete_topic(self, topic: str) -> bool:
        """
        Delete a topic.

        Args:
            topic: Topic name

        Returns:
            True if deleted successfully
        """
        try:
            if self.backend == "confluent":
                futures = self._admin_client.delete_topics([topic])
                for t, future in futures.items():
                    try:
                        future.result()
                        logger.info(f"Topic deleted: {t}")
                        return True
                    except Exception as e:
                        logger.error(f"Failed to delete topic {t}: {e}")
                        return False

            elif self.backend == "kafka-python":
                self._admin_client.delete_topics([topic])
                logger.info(f"Topic deleted: {topic}")
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to delete topic {topic}: {e}")
            return False

    def create_all_acis_topics(self) -> Dict[str, bool]:
        """
        Create all standard ACIS-X topics.

        Returns:
            Dictionary mapping topic name to success status
        """
        results = {}
        for topic_name, topic_config in ACIS_TOPIC_CONFIGS.items():
            results[topic_name] = self.create_topic(topic_config)
        return results

    def close(self) -> None:
        """Close admin client."""
        if self._admin_client:
            if self.backend == "kafka-python":
                self._admin_client.close()
            logger.info("Topic admin client closed")
