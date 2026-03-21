import os
import uuid
import socket
import signal
import threading
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Optional, Set
from pydantic import ValidationError

from schemas.event_schema import Event, DLQEvent

logger = logging.getLogger(__name__)

# Optional psutil import for system metrics
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - CPU/memory metrics will not be collected")


class BaseAgent(ABC):
    """
    Base agent class for ACIS-X v1.1 event-driven system.

    Features:
    - Kafka consumer loop with graceful shutdown
    - Kafka producer wrapper with validation
    - DLQ handling for failed events
    - Idempotent processing using event_id
    - Heartbeat publishing to acis.agent.health
    - Metrics publishing to acis.system
    - Registry registration on startup
    - schema_version support
    - correlation_id propagation
    """

    SCHEMA_VERSION = "1.1"
    HEALTH_TOPIC = "acis.agent.health"
    SYSTEM_TOPIC = "acis.system"
    REGISTRY_TOPIC = "acis.registry"
    HEARTBEAT_INTERVAL_SECONDS = 30
    OVERLOAD_COOLDOWN_SECONDS = 60

    def __init__(
        self,
        agent_name: str,
        agent_version: str,
        group_id: str,
        subscribed_topics: List[str],
        capabilities: List[str],
        kafka_client: Any,
        max_retries: int = 3,
        # New instance identification fields
        agent_type: Optional[str] = None,
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        replica_index: Optional[int] = None,
        replica_count: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ):
        self.agent_name = agent_name
        self.agent_type = agent_type or agent_name
        self.agent_version = agent_version
        self.group_id = group_id
        self.subscribed_topics = subscribed_topics
        self.capabilities = capabilities
        self.kafka_client = kafka_client
        self.max_retries = max_retries

        # Instance identification
        self.instance_id = instance_id or self._generate_instance_id(agent_name)
        self.host = host or self._get_hostname()
        self.replica_index = replica_index
        self.replica_count = replica_count
        self.max_replicas = max_replicas

        # State management
        self._running = False
        self._shutdown_event = threading.Event()
        self._processed_event_ids: Set[str] = set()
        self._processed_event_ids_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._consumer_thread: Optional[threading.Thread] = None

        # Current correlation context
        self._current_correlation_id: Optional[str] = None

        # Timing
        self._start_time: Optional[datetime] = None

        # Metrics
        self._events_processed = 0
        self._events_failed = 0
        self._last_heartbeat: Optional[datetime] = None
        self._queue_depth = 0
        self._consumer_lag = 0
        self._error_count = 0
        self._restart_count = 0

        # Metrics lock for thread safety
        self._metrics_lock = threading.Lock()

        # Process reference for psutil
        self._process: Optional[Any] = None
        if PSUTIL_AVAILABLE:
            self._process = psutil.Process(os.getpid())

        # Overload event cooldown tracking
        self._last_overload_event: Optional[datetime] = None

    @staticmethod
    def _generate_instance_id(agent_name: str) -> str:
        """Generate unique instance ID in format: agent_<name>_<uuid>."""
        name_clean = agent_name.lower().replace(" ", "_")
        return f"agent_{name_clean}_{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _get_hostname() -> str:
        """Get the current hostname."""
        try:
            return socket.gethostname()
        except Exception:
            return "unknown"

    # -------------------------------------------------------------------------
    # Abstract methods - must be implemented by subclasses
    # -------------------------------------------------------------------------

    @abstractmethod
    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        raise NotImplementedError

    @abstractmethod
    def process_event(self, event: Event) -> None:
        """Process a single event. Must be implemented by subclass."""
        raise NotImplementedError

    # -------------------------------------------------------------------------
    # Lifecycle methods
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the agent: subscribe, register, and begin consuming."""
        logger.info(f"Starting agent: {self.agent_name} v{self.agent_version} (instance: {self.instance_id})")

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self._running = True
        self._start_time = datetime.utcnow()

        # Subscribe to topics FIRST (so registry has correct topic list)
        topics = self.subscribe()
        self.subscribed_topics = topics
        self.kafka_client.subscribe(topics, self.group_id)

        # THEN register with registry
        self._register_with_registry()

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"{self.agent_name}-heartbeat"
        )
        self._heartbeat_thread.start()

        # Start consumer loop
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name=f"{self.agent_name}-consumer"
        )
        self._consumer_thread.start()

        logger.info(f"Agent {self.agent_name} started successfully on {self.host}")

    def stop(self) -> None:
        """Gracefully stop the agent."""
        logger.info(f"Stopping agent: {self.agent_name} (instance: {self.instance_id})")

        self._running = False
        self._shutdown_event.set()

        # Deregister from registry
        self._deregister_from_registry()

        # Wait for threads to finish
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=5)

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=10)

        # Close Kafka connections
        self.kafka_client.close()

        logger.info(f"Agent {self.agent_name} stopped")

    def run(self) -> None:
        """Run the agent (blocking). Call stop() to terminate."""
        self.start()
        self._shutdown_event.wait()

    def _signal_handler(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.stop()

    # -------------------------------------------------------------------------
    # Consumer loop
    # -------------------------------------------------------------------------

    def _consumer_loop(self) -> None:
        """Main consumer loop that polls and processes events."""
        logger.info(f"Consumer loop started for {self.agent_name}")

        while self._running:
            try:
                # Poll for messages with timeout
                messages = self.kafka_client.poll(timeout_ms=1000)

                # Update queue depth metric
                with self._metrics_lock:
                    self._queue_depth = len(messages) if messages else 0

                for message in messages:
                    if not self._running:
                        break

                    self._handle_message(message)

            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                with self._metrics_lock:
                    self._error_count += 1
                if self._running:
                    continue

        logger.info(f"Consumer loop stopped for {self.agent_name}")

    def _handle_message(self, message: Any) -> None:
        """Handle a single Kafka message with validation and idempotency."""
        try:
            # Parse and validate event
            event = self._validate_event(message.value)
            if event is None:
                return

            # Check idempotency
            if self._is_duplicate(event.event_id):
                logger.debug(f"Duplicate event {event.event_id}, skipping")
                self.kafka_client.commit(message)
                return

            # Set correlation context
            self._current_correlation_id = event.correlation_id

            # Process with retry logic
            retry_count = 0
            while retry_count <= self.max_retries:
                try:
                    self.process_event(event)
                    self._mark_processed(event.event_id)
                    with self._metrics_lock:
                        self._events_processed += 1
                    self.kafka_client.commit(message)
                    break

                except Exception as e:
                    retry_count += 1
                    logger.warning(
                        f"Error processing event {event.event_id}, "
                        f"attempt {retry_count}/{self.max_retries}: {e}"
                    )

                    if retry_count > self.max_retries:
                        self._send_to_dlq(event, message, e, retry_count)
                        with self._metrics_lock:
                            self._events_failed += 1
                            self._error_count += 1
                        self.kafka_client.commit(message)

            # Clear correlation context
            self._current_correlation_id = None

        except Exception as e:
            logger.error(f"Fatal error handling message: {e}")
            with self._metrics_lock:
                self._events_failed += 1
                self._error_count += 1

    def _validate_event(self, raw_data: Dict[str, Any]) -> Optional[Event]:
        """Validate and parse raw message data into Event."""
        try:
            event = Event.model_validate(raw_data)
            return event
        except ValidationError as e:
            logger.error(f"Event validation failed: {e}")
            return None

    # -------------------------------------------------------------------------
    # Idempotency
    # -------------------------------------------------------------------------

    def _is_duplicate(self, event_id: str) -> bool:
        """Check if event has already been processed."""
        with self._processed_event_ids_lock:
            return event_id in self._processed_event_ids

    def _mark_processed(self, event_id: str) -> None:
        """Mark event as processed."""
        with self._processed_event_ids_lock:
            self._processed_event_ids.add(event_id)

            # Limit set size to prevent memory growth
            if len(self._processed_event_ids) > 100000:
                # Remove oldest entries (simplified - in production use LRU cache)
                to_remove = list(self._processed_event_ids)[:50000]
                for eid in to_remove:
                    self._processed_event_ids.discard(eid)

    # -------------------------------------------------------------------------
    # Dead Letter Queue
    # -------------------------------------------------------------------------

    def _send_to_dlq(
        self,
        event: Event,
        message: Any,
        error: Exception,
        retry_count: int
    ) -> None:
        """Send failed event to Dead Letter Queue."""
        topic = message.topic if hasattr(message, 'topic') else 'unknown'
        dlq_topic = f"{topic}.dlq"

        dlq_event = DLQEvent(
            event_id=f"dlq_{uuid.uuid4()}",
            event_type="dlq.event.failed",
            event_source=self.agent_name,
            event_time=datetime.utcnow(),
            correlation_id=event.correlation_id,
            entity_id=event.entity_id,
            schema_version=self.SCHEMA_VERSION,
            payload={
                "original_event": event.model_dump(),
                "error": {
                    "code": type(error).__name__,
                    "message": str(error),
                    "failed_at": datetime.utcnow().isoformat(),
                    "retry_count": retry_count,
                    "max_retries": self.max_retries,
                    "consumer_group": self.group_id,
                    "partition": getattr(message, 'partition', None),
                    "offset": getattr(message, 'offset', None),
                    "topic": topic,
                    "agent_id": self._get_agent_id(),
                    "instance_id": self.instance_id,
                    "host": self.host,
                }
            },
            metadata={
                "environment": "production",
                "original_topic": topic,
            }
        )

        try:
            self.kafka_client.publish(dlq_topic, dlq_event.model_dump())
            logger.info(f"Event {event.event_id} sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")

    # -------------------------------------------------------------------------
    # Event publishing
    # -------------------------------------------------------------------------

    def publish_event(
        self,
        topic: str,
        event_type: str,
        entity_id: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Event:
        """
        Publish an event to a Kafka topic.

        Automatically handles:
        - event_id generation
        - timestamp
        - schema_version
        - correlation_id propagation
        """
        # Use current correlation context if not provided
        if correlation_id is None:
            correlation_id = self._current_correlation_id

        event = Event(
            event_id=f"evt_{uuid.uuid4()}",
            event_type=event_type,
            event_source=self.agent_name,
            event_time=datetime.utcnow(),
            correlation_id=correlation_id,
            entity_id=entity_id,
            schema_version=self.SCHEMA_VERSION,
            payload=payload,
            metadata=metadata or {"environment": "production"},
        )

        self.kafka_client.publish(topic, event.model_dump())
        logger.debug(f"Published event {event.event_id} to {topic}")

        return event

    def publish_event_raw(self, topic: str, event: Event) -> None:
        """Publish a pre-built Event object."""
        self.kafka_client.publish(topic, event.model_dump())

    # -------------------------------------------------------------------------
    # Metrics collection
    # -------------------------------------------------------------------------

    def _get_cpu_percent(self) -> Optional[float]:
        """Get current CPU usage percentage."""
        if not PSUTIL_AVAILABLE or self._process is None:
            return None
        try:
            return self._process.cpu_percent(interval=0.1)
        except Exception:
            return None

    def _get_memory_percent(self) -> Optional[float]:
        """Get current memory usage percentage."""
        if not PSUTIL_AVAILABLE or self._process is None:
            return None
        try:
            return self._process.memory_percent()
        except Exception:
            return None

    def _get_uptime_seconds(self) -> int:
        """Get agent uptime in seconds."""
        if self._start_time is None:
            return 0
        delta = datetime.utcnow() - self._start_time
        return int(delta.total_seconds())

    def _get_consumer_lag(self) -> int:
        """Get consumer lag from Kafka client if available."""
        try:
            if hasattr(self.kafka_client, 'get_consumer_lag'):
                return self.kafka_client.get_consumer_lag() or 0
        except Exception:
            pass
        return self._consumer_lag

    def update_consumer_lag(self, lag: int) -> None:
        """Update consumer lag metric (can be called by subclasses or Kafka client)."""
        with self._metrics_lock:
            self._consumer_lag = lag

    def update_queue_depth(self, depth: int) -> None:
        """Update queue depth metric."""
        with self._metrics_lock:
            self._queue_depth = depth

    def collect_metrics(self) -> Dict[str, Any]:
        """Collect all current metrics."""
        with self._metrics_lock:
            return {
                "cpu_percent": self._get_cpu_percent(),
                "memory_percent": self._get_memory_percent(),
                "events_processed": self._events_processed,
                "events_failed": self._events_failed,
                "queue_depth": self._queue_depth,
                "consumer_lag": self._get_consumer_lag(),
                "uptime_seconds": self._get_uptime_seconds(),
                "error_count": self._error_count,
                "restart_count": self._restart_count,
            }

    # -------------------------------------------------------------------------
    # Heartbeat and Metrics Publishing
    # -------------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """Background thread that sends periodic heartbeats and metrics."""
        logger.info(f"Heartbeat loop started for {self.agent_name}")

        while self._running:
            try:
                self.send_heartbeat()
                self._publish_metrics()
            except Exception as e:
                logger.error(f"Error sending heartbeat/metrics: {e}")

            # Wait for interval or shutdown
            self._shutdown_event.wait(timeout=self.HEARTBEAT_INTERVAL_SECONDS)

        logger.info(f"Heartbeat loop stopped for {self.agent_name}")

    def send_heartbeat(self) -> None:
        """Send heartbeat event to acis.agent.health topic."""
        self._last_heartbeat = datetime.utcnow()
        metrics = self.collect_metrics()

        heartbeat_payload = {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "status": "healthy",
            "error_code": None,
            "error_message": None,
            "timestamp": self._last_heartbeat.isoformat(),
            "metrics": {
                "cpu_percent": metrics["cpu_percent"],
                "memory_percent": metrics["memory_percent"],
                "queue_depth": metrics["queue_depth"],
                "consumer_lag": metrics["consumer_lag"],
                "error_count": metrics["error_count"],
                "restart_count": metrics["restart_count"],
                "events_processed": metrics["events_processed"],
                "events_per_second": None,  # Can be calculated over time
                "latency_ms": None,
                "uptime_seconds": metrics["uptime_seconds"],
            },
            "replica_count": self.replica_count,
            "max_replicas": self.max_replicas,
            "replica_index": self.replica_index,
            "details": {
                "subscribed_topics": self.subscribed_topics,
                "group_id": self.group_id,
                "version": self.agent_version,
            }
        }

        self.publish_event(
            topic=self.HEALTH_TOPIC,
            event_type="agent.heartbeat",
            entity_id=self.agent_name,
            payload=heartbeat_payload,
            correlation_id=None,
            metadata={"environment": "production"},
        )

        logger.debug(f"Heartbeat sent for {self.agent_name}")

    def _publish_metrics(self) -> None:
        """Publish metrics event to acis.system topic."""
        metrics = self.collect_metrics()

        metrics_payload = {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "timestamp": datetime.utcnow().isoformat(),

            # Core metrics
            "cpu_percent": metrics["cpu_percent"],
            "memory_percent": metrics["memory_percent"],
            "queue_depth": metrics["queue_depth"],
            "consumer_lag": metrics["consumer_lag"],
            "error_count": metrics["error_count"],
            "restart_count": metrics["restart_count"],

            # Throughput metrics
            "events_processed": metrics["events_processed"],
            "events_failed": metrics["events_failed"],
            "events_per_second": None,  # Can be calculated

            # Uptime
            "uptime_seconds": metrics["uptime_seconds"],

            # Kafka context
            "topic": self.subscribed_topics[0] if self.subscribed_topics else None,
            "partition": None,
            "consumer_group": self.group_id,

            # Replica info
            "replica_count": self.replica_count,
            "max_replicas": self.max_replicas,
            "replica_index": self.replica_index,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type="metrics.updated",
            entity_id=self.agent_name,
            payload=metrics_payload,
            correlation_id=None,
            metadata={"environment": "production"},
        )

        # Check for overload conditions and emit alert
        self._check_and_emit_overload(metrics)

        logger.debug(f"Metrics published for {self.agent_name}")

    def _check_and_emit_overload(self, metrics: Dict[str, Any]) -> None:
        """Check for overload conditions and emit agent.overloaded event if needed."""
        # Check cooldown first to prevent event spam
        now = datetime.utcnow()
        if self._last_overload_event is not None:
            elapsed = (now - self._last_overload_event).total_seconds()
            if elapsed < self.OVERLOAD_COOLDOWN_SECONDS:
                return  # Still in cooldown period

        # Define thresholds
        CPU_THRESHOLD = 90.0
        MEMORY_THRESHOLD = 90.0
        QUEUE_THRESHOLD = 1000
        LAG_THRESHOLD = 10000

        is_overloaded = False
        recommended_action = None
        decision_rule = None

        # Check CPU overload
        if metrics.get("cpu_percent") and metrics["cpu_percent"] > CPU_THRESHOLD:
            is_overloaded = True
            recommended_action = "scale_up"
            decision_rule = "CPU_THRESHOLD_EXCEEDED"

        # Check memory overload
        elif metrics.get("memory_percent") and metrics["memory_percent"] > MEMORY_THRESHOLD:
            is_overloaded = True
            recommended_action = "restart"
            decision_rule = "MEMORY_THRESHOLD_EXCEEDED"

        # Check queue depth overload
        elif metrics.get("queue_depth") and metrics["queue_depth"] > QUEUE_THRESHOLD:
            is_overloaded = True
            recommended_action = "scale_up"
            decision_rule = "QUEUE_DEPTH_EXCEEDED"

        # Check consumer lag overload
        elif metrics.get("consumer_lag") and metrics["consumer_lag"] > LAG_THRESHOLD:
            is_overloaded = True
            recommended_action = "scale_up"
            decision_rule = "CONSUMER_LAG_EXCEEDED"

        if is_overloaded:
            overload_payload = {
                "agent_id": self._get_agent_id(),
                "agent_type": self.agent_type,
                "agent_name": self.agent_name,
                "instance_id": self.instance_id,
                "host": self.host,
                "status": "overloaded",
                "timestamp": datetime.utcnow().isoformat(),

                # Current metrics
                "cpu_percent": metrics.get("cpu_percent"),
                "memory_percent": metrics.get("memory_percent"),
                "queue_depth": metrics.get("queue_depth"),
                "consumer_lag": metrics.get("consumer_lag"),
                "error_count": metrics.get("error_count"),

                # Thresholds
                "cpu_threshold": CPU_THRESHOLD,
                "memory_threshold": MEMORY_THRESHOLD,
                "queue_threshold": QUEUE_THRESHOLD,
                "lag_threshold": LAG_THRESHOLD,

                # Kafka context
                "topic": self.subscribed_topics[0] if self.subscribed_topics else None,
                "consumer_group": self.group_id,

                # Replica info
                "replica_count": self.replica_count,
                "max_replicas": self.max_replicas,
                "replica_index": self.replica_index,

                # Recommendation
                "recommended_action": recommended_action,
                "decision_rule": decision_rule,
                "decision_score": 0.95,
            }

            self.publish_event(
                topic=self.SYSTEM_TOPIC,
                event_type="agent.overloaded",
                entity_id=self.agent_name,
                payload=overload_payload,
                correlation_id=None,
                metadata={"environment": "production"},
            )

            # Update cooldown timestamp
            self._last_overload_event = now

            logger.warning(
                f"Agent {self.agent_name} overloaded: {decision_rule}, "
                f"recommended action: {recommended_action}"
            )

    # -------------------------------------------------------------------------
    # Registry
    # -------------------------------------------------------------------------

    def _get_agent_id(self) -> str:
        """Generate unique agent ID including instance for replica tracking."""
        base = self.agent_name.lower().replace(" ", "_")
        return f"agent_{base}_{self.instance_id}"

    def _register_with_registry(self) -> None:
        """Register agent with the registry service on startup."""
        registration_payload = {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "capabilities": self.capabilities,
            "topics": {
                "consumes": self.subscribed_topics,
                "produces": [],  # Subclasses should override
            },
            "status": "registered",
            "version": self.agent_version,
            "registered_at": datetime.utcnow().isoformat(),
            "group_id": self.group_id,
            "replica_index": self.replica_index,
            "replica_count": self.replica_count,
            "max_replicas": self.max_replicas,
        }

        self.publish_event(
            topic=self.REGISTRY_TOPIC,
            event_type="registry.agent.registered",
            entity_id=self.agent_name,
            payload=registration_payload,
            correlation_id=None,
            metadata={"environment": "production"},
        )

        logger.info(f"Agent {self.agent_name} (instance: {self.instance_id}) registered with registry")

    def _deregister_from_registry(self) -> None:
        """Deregister agent from registry on shutdown."""
        deregistration_payload = {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "capabilities": self.capabilities,
            "topics": None,
            "status": "deregistered",
            "version": self.agent_version,
            "registered_at": None,
            "deregistered_at": datetime.utcnow().isoformat(),
        }

        try:
            self.publish_event(
                topic=self.REGISTRY_TOPIC,
                event_type="registry.agent.deregistered",
                entity_id=self.agent_name,
                payload=deregistration_payload,
                correlation_id=None,
                metadata={"environment": "production"},
            )
            logger.info(f"Agent {self.agent_name} deregistered from registry")
        except Exception as e:
            logger.warning(f"Failed to deregister from registry: {e}")

    # -------------------------------------------------------------------------
    # Agent card
    # -------------------------------------------------------------------------

    def get_agent_card(self) -> Dict[str, Any]:
        """Return agent metadata for registry/discovery."""
        metrics = self.collect_metrics()
        return {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "agent_version": self.agent_version,
            "instance_id": self.instance_id,
            "host": self.host,
            "group_id": self.group_id,
            "subscribed_topics": self.subscribed_topics,
            "capabilities": self.capabilities,
            "schema_version": self.SCHEMA_VERSION,
            "status": "healthy" if self._running else "stopped",
            "replica_index": self.replica_index,
            "replica_count": self.replica_count,
            "max_replicas": self.max_replicas,
            "metrics": metrics,
            "last_heartbeat": (
                self._last_heartbeat.isoformat()
                if self._last_heartbeat else None
            ),
        }

    # -------------------------------------------------------------------------
    # Correlation ID helpers
    # -------------------------------------------------------------------------

    def get_correlation_id(self) -> Optional[str]:
        """Get current correlation ID for event chain tracing."""
        return self._current_correlation_id

    def create_correlation_id(self) -> str:
        """Create a new correlation ID for starting a new event chain."""
        return f"corr_{uuid.uuid4()}"

    # -------------------------------------------------------------------------
    # Status helpers
    # -------------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        """Check if agent is currently running."""
        return self._running

    def get_status(self) -> Dict[str, Any]:
        """Get current agent status."""
        metrics = self.collect_metrics()
        return {
            "agent_id": self._get_agent_id(),
            "agent_type": self.agent_type,
            "agent_name": self.agent_name,
            "instance_id": self.instance_id,
            "host": self.host,
            "running": self._running,
            "uptime_seconds": metrics["uptime_seconds"],
            "events_processed": metrics["events_processed"],
            "events_failed": metrics["events_failed"],
            "cpu_percent": metrics["cpu_percent"],
            "memory_percent": metrics["memory_percent"],
            "queue_depth": metrics["queue_depth"],
            "consumer_lag": metrics["consumer_lag"],
            "replica_index": self.replica_index,
            "replica_count": self.replica_count,
            "last_heartbeat": (
                self._last_heartbeat.isoformat()
                if self._last_heartbeat else None
            ),
            "subscribed_topics": self.subscribed_topics,
        }

    # -------------------------------------------------------------------------
    # Replica management helpers
    # -------------------------------------------------------------------------

    def set_replica_info(
        self,
        replica_index: Optional[int] = None,
        replica_count: Optional[int] = None,
        max_replicas: Optional[int] = None,
    ) -> None:
        """Update replica information (for dynamic scaling)."""
        if replica_index is not None:
            self.replica_index = replica_index
        if replica_count is not None:
            self.replica_count = replica_count
        if max_replicas is not None:
            self.max_replicas = max_replicas

    def increment_restart_count(self) -> None:
        """Increment restart count (called after agent restart)."""
        with self._metrics_lock:
            self._restart_count += 1
