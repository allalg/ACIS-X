"""
KafkaClient - Production-ready Kafka abstraction for ACIS-X.

Provides unified interface for Kafka producer/consumer operations with:
- Event schema integration
- Automatic serialization/deserialization
- Partition key support
- Correlation ID propagation
- Header support
- DLQ handling
- Retry logic
- Offset management
- Consumer lag tracking

Designed to work with confluent-kafka-python or kafka-python.
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set
from enum import Enum

from pydantic import ValidationError

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

class SerializationFormat(str, Enum):
    """Serialization format for Kafka messages."""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"


@dataclass
class KafkaConfig:
    """Configuration for KafkaClient."""

    # Broker connection
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None

    # Producer settings
    producer_acks: str = "all"  # all, 1, 0
    producer_retries: int = 3
    producer_max_in_flight: int = 5
    producer_compression: str = "gzip"  # none, gzip, snappy, lz4, zstd
    producer_batch_size: int = 16384
    producer_linger_ms: int = 10

    # Consumer settings
    consumer_auto_offset_reset: str = "earliest"  # earliest, latest, none
    consumer_enable_auto_commit: bool = False  # Manual commit for reliability
    consumer_max_poll_records: int = 500
    consumer_max_poll_interval_ms: int = 300000  # 5 minutes
    consumer_session_timeout_ms: int = 30000  # 30 seconds
    consumer_heartbeat_interval_ms: int = 3000  # 3 seconds
    consumer_fetch_min_bytes: int = 1
    consumer_fetch_max_wait_ms: int = 500

    # Serialization
    serialization_format: SerializationFormat = SerializationFormat.JSON
    schema_registry_url: Optional[str] = None

    # Retry and DLQ
    max_retries: int = 3
    retry_backoff_ms: int = 100
    enable_dlq: bool = True
    dlq_suffix: str = ".dlq"

    # Monitoring
    enable_metrics: bool = True
    metrics_interval_seconds: int = 60

    # Client ID
    client_id: Optional[str] = None

    def __post_init__(self):
        """Generate client_id if not provided."""
        if self.client_id is None:
            self.client_id = f"acis_client_{uuid.uuid4().hex[:8]}"


# =============================================================================
# Kafka Message Wrapper
# =============================================================================

@dataclass
class KafkaMessage:
    """Wrapper for Kafka messages with metadata."""

    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: datetime
    high_watermark: Optional[int] = None

    def get_lag(self) -> Optional[int]:
        """Calculate lag if high watermark is available."""
        if self.high_watermark is None:
            return None
        return self.high_watermark - self.offset


# =============================================================================
# KafkaClient
# =============================================================================

class KafkaClient:
    """
    Production Kafka client for ACIS-X.

    Wraps confluent-kafka-python or kafka-python with:
    - Automatic Event schema validation
    - Partition key extraction
    - Correlation ID header propagation
    - DLQ handling
    - Retry logic
    - Consumer lag tracking
    """

    def __init__(
        self,
        config: KafkaConfig,
        backend: str = "confluent",  # confluent or kafka-python
    ):
        """
        Initialize KafkaClient.

        Args:
            config: Kafka configuration
            backend: Kafka library to use (confluent or kafka-python)
        """
        self.config = config
        self.backend = backend

        # Producer and consumer instances (lazy init)
        self._producer: Optional[Any] = None
        self._consumer: Optional[Any] = None
        self._subscribed_topics: List[str] = []
        self._group_id: Optional[str] = None

        # Consumer callback
        self._message_handler: Optional[Callable[[KafkaMessage], None]] = None

        # Metrics
        self._messages_published = 0
        self._messages_consumed = 0
        self._publish_errors = 0
        self._consume_errors = 0
        self._dlq_events = 0

        # Consumer position tracking
        self._topic_partition_offsets: Dict[str, Dict[int, int]] = {}
        self._topic_partition_watermarks: Dict[str, Dict[int, int]] = {}

        # Initialize producer
        self._init_producer()

        logger.info(f"KafkaClient initialized (backend: {backend}, client_id: {config.client_id})")

    # -------------------------------------------------------------------------
    # Initialization
    # -------------------------------------------------------------------------

    def _init_producer(self) -> None:
        """Initialize Kafka producer based on backend."""
        if self.backend == "confluent":
            self._init_confluent_producer()
        elif self.backend == "kafka-python":
            self._init_kafka_python_producer()
        else:
            raise ValueError(f"Unknown backend: {self.backend}")

    def _init_confluent_producer(self) -> None:
        """Initialize confluent-kafka producer."""
        try:
            from confluent_kafka import Producer

            producer_config = {
                "bootstrap.servers": ",".join(self.config.bootstrap_servers),
                "client.id": self.config.client_id,
                "acks": self.config.producer_acks,
                "retries": self.config.producer_retries,
                "max.in.flight.requests.per.connection": self.config.producer_max_in_flight,
                "compression.type": self.config.producer_compression,
                "batch.size": self.config.producer_batch_size,
                "linger.ms": self.config.producer_linger_ms,
            }

            # Add security settings if configured
            if self.config.security_protocol != "PLAINTEXT":
                producer_config["security.protocol"] = self.config.security_protocol
                if self.config.sasl_mechanism:
                    producer_config["sasl.mechanism"] = self.config.sasl_mechanism
                    producer_config["sasl.username"] = self.config.sasl_username
                    producer_config["sasl.password"] = self.config.sasl_password

            self._producer = Producer(producer_config)
            logger.info("Confluent Kafka producer initialized")

        except ImportError:
            logger.error("confluent-kafka-python not installed")
            raise

    def _init_kafka_python_producer(self) -> None:
        """Initialize kafka-python producer."""
        try:
            from kafka import KafkaProducer

            producer_config = {
                "bootstrap_servers": self.config.bootstrap_servers,
                "client_id": self.config.client_id,
                "acks": self.config.producer_acks,
                "retries": self.config.producer_retries,
                "max_in_flight_requests_per_connection": self.config.producer_max_in_flight,
                "compression_type": self.config.producer_compression,
                "batch_size": self.config.producer_batch_size,
                "linger_ms": self.config.producer_linger_ms,
                "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
                "key_serializer": lambda k: k.encode("utf-8") if k else None,
            }

            # Add security settings if configured
            if self.config.security_protocol != "PLAINTEXT":
                producer_config["security_protocol"] = self.config.security_protocol
                if self.config.sasl_mechanism:
                    producer_config["sasl_mechanism"] = self.config.sasl_mechanism
                    producer_config["sasl_plain_username"] = self.config.sasl_username
                    producer_config["sasl_plain_password"] = self.config.sasl_password

            self._producer = KafkaProducer(**producer_config)
            logger.info("kafka-python producer initialized")

        except ImportError:
            logger.error("kafka-python not installed")
            raise

    def _init_consumer(self, group_id: str) -> None:
        """Initialize Kafka consumer based on backend."""
        if self.backend == "confluent":
            self._init_confluent_consumer(group_id)
        elif self.backend == "kafka-python":
            self._init_kafka_python_consumer(group_id)

    def _init_confluent_consumer(self, group_id: str) -> None:
        """Initialize confluent-kafka consumer."""
        try:
            from confluent_kafka import Consumer

            consumer_config = {
                "bootstrap.servers": ",".join(self.config.bootstrap_servers),
                "client.id": self.config.client_id,
                "group.id": group_id,
                "auto.offset.reset": self.config.consumer_auto_offset_reset,
                "enable.auto.commit": self.config.consumer_enable_auto_commit,
                "max.poll.interval.ms": self.config.consumer_max_poll_interval_ms,
                "session.timeout.ms": self.config.consumer_session_timeout_ms,
                "heartbeat.interval.ms": self.config.consumer_heartbeat_interval_ms,
                "fetch.min.bytes": self.config.consumer_fetch_min_bytes,
                "fetch.max.wait.ms": self.config.consumer_fetch_max_wait_ms,
            }

            # Add security settings
            if self.config.security_protocol != "PLAINTEXT":
                consumer_config["security.protocol"] = self.config.security_protocol
                if self.config.sasl_mechanism:
                    consumer_config["sasl.mechanism"] = self.config.sasl_mechanism
                    consumer_config["sasl.username"] = self.config.sasl_username
                    consumer_config["sasl.password"] = self.config.sasl_password

            self._consumer = Consumer(consumer_config)
            self._group_id = group_id
            logger.info(f"Confluent Kafka consumer initialized (group: {group_id})")

        except ImportError:
            logger.error("confluent-kafka-python not installed")
            raise

    def _init_kafka_python_consumer(self, group_id: str) -> None:
        """Initialize kafka-python consumer."""
        try:
            from kafka import KafkaConsumer

            consumer_config = {
                "bootstrap_servers": self.config.bootstrap_servers,
                "client_id": self.config.client_id,
                "group_id": group_id,
                "auto_offset_reset": self.config.consumer_auto_offset_reset,
                "enable_auto_commit": self.config.consumer_enable_auto_commit,
                "max_poll_records": self.config.consumer_max_poll_records,
                "max_poll_interval_ms": self.config.consumer_max_poll_interval_ms,
                "session_timeout_ms": self.config.consumer_session_timeout_ms,
                "heartbeat_interval_ms": self.config.consumer_heartbeat_interval_ms,
                "fetch_min_bytes": self.config.consumer_fetch_min_bytes,
                "fetch_max_wait_ms": self.config.consumer_fetch_max_wait_ms,
                "value_deserializer": lambda m: json.loads(m.decode("utf-8")),
                "key_deserializer": lambda k: k.decode("utf-8") if k else None,
            }

            # Add security settings
            if self.config.security_protocol != "PLAINTEXT":
                consumer_config["security_protocol"] = self.config.security_protocol
                if self.config.sasl_mechanism:
                    consumer_config["sasl_mechanism"] = self.config.sasl_mechanism
                    consumer_config["sasl_plain_username"] = self.config.sasl_username
                    consumer_config["sasl_plain_password"] = self.config.sasl_password

            self._consumer = KafkaConsumer(**consumer_config)
            self._group_id = group_id
            logger.info(f"kafka-python consumer initialized (group: {group_id})")

        except ImportError:
            logger.error("kafka-python not installed")
            raise

    # -------------------------------------------------------------------------
    # Producer methods
    # -------------------------------------------------------------------------

    def publish(
        self,
        topic: str,
        event: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
    ) -> None:
        """
        Publish an event to a Kafka topic.

        Args:
            topic: Target topic
            event: Event dictionary (from Event.model_dump())
            key: Optional partition key (defaults to entity_id from event)
            headers: Optional headers (merged with correlation_id)
            partition: Optional specific partition (otherwise uses key hash)
        """
        if self._producer is None:
            raise RuntimeError("Producer not initialized")

        # Extract partition key from event if not provided
        if key is None:
            key = event.get("entity_id", str(uuid.uuid4()))

        # Build headers with correlation_id
        kafka_headers = headers or {}
        if "correlation_id" not in kafka_headers and "correlation_id" in event:
            kafka_headers["correlation_id"] = event["correlation_id"]

        # Add event metadata to headers
        kafka_headers["event_type"] = event.get("event_type", "unknown")
        kafka_headers["event_source"] = event.get("event_source", "unknown")
        kafka_headers["schema_version"] = event.get("schema_version", "1.0")

        # Serialize event
        serialized_event = self._serialize(event)

        # Publish based on backend
        try:
            if self.backend == "confluent":
                self._publish_confluent(topic, key, serialized_event, kafka_headers, partition)
            elif self.backend == "kafka-python":
                self._publish_kafka_python(topic, key, serialized_event, kafka_headers, partition)

            self._messages_published += 1
            logger.debug(f"Published event to {topic} (key: {key})")

        except Exception as e:
            self._publish_errors += 1
            logger.error(f"Failed to publish to {topic}: {e}")
            raise

    def _publish_confluent(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Dict[str, str],
        partition: Optional[int],
    ) -> None:
        """Publish using confluent-kafka."""
        # Convert headers to list of tuples
        header_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Publish
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=value,
            headers=header_list,
            partition=partition if partition is not None else -1,
            callback=self._delivery_callback,
        )

        # Flush periodically to ensure delivery
        self._producer.poll(0)

    def _publish_kafka_python(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: Dict[str, str],
        partition: Optional[int],
    ) -> None:
        """Publish using kafka-python."""
        # Convert headers to list of tuples
        header_list = [(k, v.encode("utf-8")) for k, v in headers.items()]

        # Publish
        kwargs = {
            "key": key.encode("utf-8") if key else None,
            "value": value,
            "headers": header_list,
        }
        if partition is not None:
            kwargs["partition"] = partition

        future = self._producer.send(topic, **kwargs)
        # Optional: can call future.get(timeout=30) for synchronous publishing

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback for confluent-kafka delivery reports."""
        if err:
            logger.error(f"Message delivery failed: {err}")
            self._publish_errors += 1
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending producer messages."""
        if self._producer is None:
            return

        if self.backend == "confluent":
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(f"{remaining} messages failed to flush")
        elif self.backend == "kafka-python":
            self._producer.flush(timeout=timeout)

    # -------------------------------------------------------------------------
    # Consumer methods
    # -------------------------------------------------------------------------

    def subscribe(
        self,
        topics: List[str],
        group_id: str,
        handler: Optional[Callable[[KafkaMessage], None]] = None,
    ) -> None:
        """
        Subscribe to Kafka topics.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            handler: Optional message handler callback
        """
        if self._consumer is None:
            self._init_consumer(group_id)

        self._subscribed_topics = topics
        self._message_handler = handler

        if self.backend == "confluent":
            self._consumer.subscribe(topics)
        elif self.backend == "kafka-python":
            self._consumer.subscribe(topics=topics)

        logger.info(f"Subscribed to topics: {topics} (group: {group_id})")

    def poll(
        self,
        timeout_ms: int = 1000,
        max_messages: Optional[int] = None,
    ) -> List[KafkaMessage]:
        """
        Poll for messages from subscribed topics.

        Args:
            timeout_ms: Poll timeout in milliseconds
            max_messages: Maximum messages to return (None = unlimited)

        Returns:
            List of KafkaMessage objects
        """
        if self._consumer is None:
            raise RuntimeError("Consumer not initialized - call subscribe() first")

        messages = []

        try:
            if self.backend == "confluent":
                messages = self._poll_confluent(timeout_ms, max_messages)
            elif self.backend == "kafka-python":
                messages = self._poll_kafka_python(timeout_ms, max_messages)

            self._messages_consumed += len(messages)

            # Update offset tracking for lag calculation
            for msg in messages:
                self._update_offset_tracking(msg.topic, msg.partition, msg.offset, msg.high_watermark)

        except Exception as e:
            self._consume_errors += 1
            logger.error(f"Error polling messages: {e}")
            raise

        return messages

    def _poll_confluent(
        self,
        timeout_ms: int,
        max_messages: Optional[int],
    ) -> List[KafkaMessage]:
        """Poll using confluent-kafka."""
        messages = []
        max_count = max_messages or self.config.consumer_max_poll_records

        # confluent-kafka polls one message at a time
        for _ in range(max_count):
            msg = self._consumer.poll(timeout=timeout_ms / 1000.0)
            if msg is None:
                break
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # Parse message
            kafka_msg = self._parse_confluent_message(msg)
            if kafka_msg:
                messages.append(kafka_msg)

            # Break on first message if quick polling
            if timeout_ms < 100:
                break

        return messages

    def _poll_kafka_python(
        self,
        timeout_ms: int,
        max_messages: Optional[int],
    ) -> List[KafkaMessage]:
        """Poll using kafka-python."""
        messages = []

        # kafka-python poll returns dict: TopicPartition -> list of records
        records = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)

        for topic_partition, msgs in records.items():
            for msg in msgs:
                kafka_msg = self._parse_kafka_python_message(msg, topic_partition)
                if kafka_msg:
                    messages.append(kafka_msg)

        return messages

    def _parse_confluent_message(self, msg: Any) -> Optional[KafkaMessage]:
        """Parse confluent-kafka message."""
        try:
            # Deserialize value
            value = json.loads(msg.value().decode("utf-8"))

            # Parse headers
            headers = {}
            if msg.headers():
                headers = {k: v.decode("utf-8") for k, v in msg.headers()}

            # Get high watermark
            watermark = None
            try:
                low, high = self._consumer.get_watermark_offsets(
                    msg.topic(),
                    msg.partition(),
                    timeout=1.0
                )
                watermark = high
            except Exception:
                pass

            return KafkaMessage(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key(),
                value=value,
                headers=headers,
                timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000.0),
                high_watermark=watermark,
            )

        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return None

    def _parse_kafka_python_message(self, msg: Any, topic_partition: Any) -> Optional[KafkaMessage]:
        """Parse kafka-python message."""
        try:
            # Value already deserialized by kafka-python
            value = msg.value

            # Parse headers
            headers = {}
            if msg.headers:
                headers = {k: v.decode("utf-8") if isinstance(v, bytes) else v for k, v in msg.headers}

            # Get high watermark (kafka-python doesn't provide this directly)
            watermark = None
            try:
                partitions = self._consumer.end_offsets([topic_partition])
                watermark = partitions.get(topic_partition)
            except Exception:
                pass

            return KafkaMessage(
                topic=msg.topic,
                partition=msg.partition,
                offset=msg.offset,
                key=msg.key,
                value=value,
                headers=headers,
                timestamp=datetime.fromtimestamp(msg.timestamp / 1000.0),
                high_watermark=watermark,
            )

        except Exception as e:
            logger.error(f"Failed to parse message: {e}")
            return None

    def commit(self, message: Optional[KafkaMessage] = None) -> None:
        """
        Commit consumer offsets.

        Args:
            message: Specific message to commit (None = commit all)
        """
        if self._consumer is None:
            return

        try:
            if self.backend == "confluent":
                if message:
                    # Commit specific offset (not directly supported, use async)
                    self._consumer.commit(asynchronous=False)
                else:
                    self._consumer.commit(asynchronous=False)

            elif self.backend == "kafka-python":
                if message:
                    # Commit specific offset
                    from kafka import TopicPartition
                    tp = TopicPartition(message.topic, message.partition)
                    self._consumer.commit({tp: message.offset + 1})
                else:
                    self._consumer.commit()

            logger.debug("Offsets committed")

        except Exception as e:
            logger.error(f"Failed to commit offsets: {e}")
            raise

    # -------------------------------------------------------------------------
    # Lag tracking
    # -------------------------------------------------------------------------

    def _update_offset_tracking(
        self,
        topic: str,
        partition: int,
        offset: int,
        high_watermark: Optional[int],
    ) -> None:
        """Update internal offset tracking for lag calculation."""
        if topic not in self._topic_partition_offsets:
            self._topic_partition_offsets[topic] = {}
        self._topic_partition_offsets[topic][partition] = offset

        if high_watermark is not None:
            if topic not in self._topic_partition_watermarks:
                self._topic_partition_watermarks[topic] = {}
            self._topic_partition_watermarks[topic][partition] = high_watermark

    def get_consumer_lag(self, topic: Optional[str] = None, partition: Optional[int] = None) -> int:
        """
        Get consumer lag.

        Args:
            topic: Specific topic (None = total across all topics)
            partition: Specific partition (None = total across all partitions)

        Returns:
            Consumer lag (messages behind)
        """
        if topic and partition is not None:
            # Specific topic-partition lag
            current_offset = self._topic_partition_offsets.get(topic, {}).get(partition, 0)
            watermark = self._topic_partition_watermarks.get(topic, {}).get(partition, 0)
            return max(0, watermark - current_offset)

        elif topic:
            # Total lag for topic across all partitions
            lag = 0
            topic_offsets = self._topic_partition_offsets.get(topic, {})
            topic_watermarks = self._topic_partition_watermarks.get(topic, {})
            for part in topic_watermarks.keys():
                offset = topic_offsets.get(part, 0)
                watermark = topic_watermarks.get(part, 0)
                lag += max(0, watermark - offset)
            return lag

        else:
            # Total lag across all topics
            total_lag = 0
            for topic in self._topic_partition_watermarks.keys():
                total_lag += self.get_consumer_lag(topic=topic)
            return total_lag

    # -------------------------------------------------------------------------
    # DLQ handling
    # -------------------------------------------------------------------------

    def send_to_dlq(
        self,
        original_topic: str,
        event: Event,
        error: Exception,
        retry_count: int,
        agent_name: str,
        additional_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send failed event to Dead Letter Queue.

        Args:
            original_topic: Topic where message failed
            event: Original event
            error: Exception that caused failure
            retry_count: Number of retries attempted
            agent_name: Name of agent that failed to process
            additional_context: Optional additional error context
        """
        if not self.config.enable_dlq:
            logger.warning("DLQ disabled, dropping failed event")
            return

        dlq_topic = f"{original_topic}{self.config.dlq_suffix}"

        dlq_payload = {
            "original_event": event.model_dump() if hasattr(event, "model_dump") else event,
            "error": {
                "code": type(error).__name__,
                "message": str(error),
                "failed_at": datetime.utcnow().isoformat(),
                "retry_count": retry_count,
                "max_retries": self.config.max_retries,
                "agent_name": agent_name,
                "original_topic": original_topic,
            },
        }

        if additional_context:
            dlq_payload["error"].update(additional_context)

        # Create DLQ event
        dlq_event = {
            "event_id": f"dlq_{uuid.uuid4()}",
            "event_type": "dlq.event.failed",
            "event_source": agent_name,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": event.correlation_id if hasattr(event, "correlation_id") else None,
            "entity_id": event.entity_id if hasattr(event, "entity_id") else "unknown",
            "schema_version": "1.1",
            "payload": dlq_payload,
            "metadata": {
                "environment": "production",
                "original_topic": original_topic,
            },
        }

        try:
            self.publish(dlq_topic, dlq_event)
            self._dlq_events += 1
            logger.info(f"Event sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def _serialize(self, data: Dict[str, Any]) -> bytes:
        """Serialize event data based on configured format."""
        if self.config.serialization_format == SerializationFormat.JSON:
            return json.dumps(data, default=str).encode("utf-8")
        elif self.config.serialization_format == SerializationFormat.AVRO:
            # TODO: Implement Avro serialization with schema registry
            raise NotImplementedError("Avro serialization not yet implemented")
        elif self.config.serialization_format == SerializationFormat.PROTOBUF:
            # TODO: Implement Protobuf serialization
            raise NotImplementedError("Protobuf serialization not yet implemented")
        else:
            raise ValueError(f"Unknown serialization format: {self.config.serialization_format}")

    def _deserialize(self, data: bytes) -> Dict[str, Any]:
        """Deserialize event data based on configured format."""
        if self.config.serialization_format == SerializationFormat.JSON:
            return json.loads(data.decode("utf-8"))
        elif self.config.serialization_format == SerializationFormat.AVRO:
            raise NotImplementedError("Avro deserialization not yet implemented")
        elif self.config.serialization_format == SerializationFormat.PROTOBUF:
            raise NotImplementedError("Protobuf deserialization not yet implemented")
        else:
            raise ValueError(f"Unknown serialization format: {self.config.serialization_format}")

    # -------------------------------------------------------------------------
    # Schema validation
    # -------------------------------------------------------------------------

    def validate_event(self, data: Dict[str, Any]) -> Optional[Event]:
        """
        Validate message against Event schema.

        Args:
            data: Raw event dictionary

        Returns:
            Validated Event object or None on failure
        """
        try:
            return Event.model_validate(data)
        except ValidationError as e:
            logger.error(f"Event validation failed: {e}")
            return None

    # -------------------------------------------------------------------------
    # Partition key extraction
    # -------------------------------------------------------------------------

    @staticmethod
    def extract_partition_key(event: Dict[str, Any]) -> str:
        """
        Extract partition key from event.

        Default strategy: use entity_id as partition key for consistent routing.

        Args:
            event: Event dictionary

        Returns:
            Partition key string
        """
        # Use entity_id for partition affinity (all events for same entity go to same partition)
        return event.get("entity_id", str(uuid.uuid4()))

    # -------------------------------------------------------------------------
    # Metrics and monitoring
    # -------------------------------------------------------------------------

    def get_metrics(self) -> Dict[str, Any]:
        """Get client metrics."""
        return {
            "messages_published": self._messages_published,
            "messages_consumed": self._messages_consumed,
            "publish_errors": self._publish_errors,
            "consume_errors": self._consume_errors,
            "dlq_events": self._dlq_events,
            "subscribed_topics": self._subscribed_topics,
            "group_id": self._group_id,
            "consumer_lag": self.get_consumer_lag(),
        }

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Close Kafka connections."""
        logger.info("Closing KafkaClient")

        # Flush producer
        if self._producer:
            try:
                self.flush(timeout=5.0)
            except Exception as e:
                logger.warning(f"Error flushing producer: {e}")

        # Close consumer
        if self._consumer:
            try:
                if self.backend == "confluent":
                    self._consumer.close()
                elif self.backend == "kafka-python":
                    self._consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer: {e}")

        logger.info("KafkaClient closed")

    # -------------------------------------------------------------------------
    # Context manager support
    # -------------------------------------------------------------------------

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
