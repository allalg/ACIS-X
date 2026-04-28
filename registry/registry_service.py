"""
RegistryService - Central agent registry for ACIS-X.

Maintains in-memory registry of all running agents in the system.
Provides service discovery and topology management.

Subscribes to:
    - acis.agent.health (heartbeats)
    - acis.registry (registration/deregistration events)
    - acis.system (spawned, stopped, restart events)

Publishes:
    - registry.agent.registered
    - registry.agent.updated
    - registry.topology.changed

Provides queries:
    - Find agents by capability
    - Find agents by name
    - Find healthy agents
    - Get system topology
"""

import json
import logging
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from runtime.kafka_client import KafkaClient, KafkaMessage
from schemas.event_schema import Event, RegistryEventType, SystemEventType, AgentStatus
from registry.agent_card import AgentCard

logger = logging.getLogger(__name__)


# =============================================================================
# Registered Agent Model
# =============================================================================

@dataclass
class RegisteredAgent:
    """Represents a registered agent in the system."""

    # Identity
    agent_id: str
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: Optional[str] = None

    # Network
    host: Optional[str] = None
    port: Optional[int] = None

    # Configuration
    version: str = "1.0.0"
    group_id: Optional[str] = None
    capabilities: List[str] = field(default_factory=list)

    # Topics
    topics_consumed: List[str] = field(default_factory=list)
    topics_produced: List[str] = field(default_factory=list)

    # State
    status: str = AgentStatus.HEALTHY.value
    registered_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    last_updated: Optional[datetime] = None

    # Replica info
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None

    # Metrics (latest from heartbeat)
    metrics: Dict[str, Any] = field(default_factory=dict)

    # AgentCard (for discovery)
    agent_card: Optional[AgentCard] = None

    def is_healthy(self, heartbeat_timeout_seconds: int = 90) -> bool:
        """Check if agent is healthy based on heartbeat."""
        if self.status not in [AgentStatus.HEALTHY.value, AgentStatus.DEGRADED.value]:
            return False
        if self.last_heartbeat is None:
            return False
        age = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        return age < heartbeat_timeout_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime to ISO format
        if self.registered_at:
            data["registered_at"] = self.registered_at.isoformat()
        if self.last_heartbeat:
            data["last_heartbeat"] = self.last_heartbeat.isoformat()
        if self.last_updated:
            data["last_updated"] = self.last_updated.isoformat()
        # Convert AgentCard to dict
        if self.agent_card:
            data["agent_card"] = self.agent_card.to_dict()
        return data


# =============================================================================
# RegistryService
# =============================================================================

class RegistryService:
    """
    Central registry service for ACIS-X agent discovery and topology management.

    Maintains in-memory registry of all agents and provides service discovery.
    """

    # Heartbeat timeout for health checks (agent must send heartbeat every 2s)
    HEARTBEAT_TIMEOUT_SECONDS = 30  # 15x heartbeat interval - more conservative

    # Heartbeat cleanup interval (check for stale agents every 10s)
    CLEANUP_INTERVAL_SECONDS = 10

    # Stale agent threshold (agents without heartbeat for 60s are removed)
    STALE_AGENT_TIMEOUT_SECONDS = 60

    # Topology change detection (emit event if topology changed)
    TOPOLOGY_CHANGE_COOLDOWN_SECONDS = 30

    def __init__(
        self,
        kafka_client: KafkaClient,
        service_id: Optional[str] = None,
    ):
        """
        Initialize RegistryService.

        Args:
            kafka_client: KafkaClient instance for pub/sub
            service_id: Optional service identifier (auto-generated if not provided)
        """
        self.kafka_client = kafka_client
        self.service_id = service_id or f"registry_{uuid.uuid4().hex[:8]}"

        # In-memory registry: agent_id -> RegisteredAgent
        self._registry: Dict[str, RegisteredAgent] = {}
        self._registry_lock = threading.Lock()

        # Topology tracking
        self._topology_version = 0
        self._last_topology_change: Optional[datetime] = None
        self._last_topology_event: Optional[datetime] = None

        # Consumer state
        self._running = False
        self._consumer_thread: Optional[threading.Thread] = None
        self._cleanup_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        # Metrics
        self._events_processed = 0
        self._registry_updates = 0
        self._topology_changes = 0
        self._start_time: Optional[datetime] = None

        # Outbound publish queue: _handle_message() and its _publish_* helpers
        # append (topic, event_dict, key) tuples here instead of calling
        # kafka_client.publish() directly.  The consumer loop drains the queue
        # after committing the inbound offset so that:
        #   1. A publish failure cannot leave the in-memory registry in an
        #      inconsistent state (state mutation has already completed).
        #   2. The inbound offset is committed before any outbound I/O, so a
        #      crash mid-publish doesn't cause double-processing on restart.
        self._publish_queue: list = []

        logger.info(f"RegistryService initialized (id: {self.service_id})")

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the registry service."""
        logger.info("Starting RegistryService")

        self._running = True
        self._start_time = datetime.utcnow()

        # Subscribe to topics with STABLE consumer group (not random UUID)
        # This ensures we don't replay old events on restart
        self.kafka_client.subscribe(
            topics=["acis.agent.health", "acis.registry", "acis.system"],
            group_id="acis-registry-service",  # Stable group ID
        )

        # Start consumer loop
        self._consumer_thread = threading.Thread(
            target=self._consumer_loop,
            daemon=True,
            name="registry-consumer",
        )
        self._consumer_thread.start()

        # Start cleanup loop
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="registry-cleanup",
        )
        self._cleanup_thread.start()

        logger.info("RegistryService started")

    def stop(self) -> None:
        """Stop the registry service."""
        logger.info("Stopping RegistryService")

        self._running = False
        self._shutdown_event.set()

        # Wait for threads
        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=5)

        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)

        try:
            self.kafka_client.close()
        except Exception as exc:
            logger.warning(f"Error closing RegistryService Kafka client: {exc}")

        logger.info("RegistryService stopped")

    # -------------------------------------------------------------------------
    # Consumer loop
    # -------------------------------------------------------------------------

    def _consumer_loop(self) -> None:
        """Main consumer loop for processing registry events."""
        logger.info("Registry consumer loop started")

        while self._running:
            try:
                # Poll for messages
                messages = self.kafka_client.poll(timeout_ms=1000, max_messages=100)

                for msg in messages:
                    if not self._running:
                        break
                    self._handle_message(msg)
                    self.kafka_client.commit(msg)

                    # Drain outbound publish queue AFTER committing the inbound
                    # offset so the registry state and Kafka log stay consistent.
                    for _topic, _event, _key in self._publish_queue:
                        try:
                            self.kafka_client.publish(_topic, _event, key=_key)
                        except Exception as pub_exc:
                            logger.error(
                                "RegistryService: publish to %s failed: %s",
                                _topic, pub_exc,
                            )
                    self._publish_queue.clear()

            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                if self._running:
                    continue

        logger.info("Registry consumer loop stopped")

    def _handle_message(self, msg: KafkaMessage) -> None:
        """Handle a single message."""
        try:
            # Validate event
            event = self.kafka_client.validate_event(msg.value)
            if event is None:
                logger.warning(f"Invalid event schema on {msg.topic}")
                return

            # CRITICAL: Ignore events from before this registry instance started
            # This prevents replaying old historical events from Kafka
            if self._start_time and event.event_time < self._start_time:
                logger.debug(f"Ignoring old event {event.event_type} from {event.event_time}")
                return

            self._events_processed += 1

            # Route to appropriate handler
            event_type = event.event_type

            if event_type == "agent.heartbeat":
                self._handle_heartbeat(event)

            elif event_type == SystemEventType.AGENT_SPAWNED.value:
                self._handle_agent_spawned(event)

            elif event_type == "agent.stopped":
                self._handle_agent_stopped(event)

            elif event_type == SystemEventType.AGENT_RESTART_COMPLETED.value:
                self._handle_restart_completed(event)

            elif event_type == RegistryEventType.AGENT_REGISTERED.value:
                self._handle_agent_registered(event)

            elif event_type == RegistryEventType.AGENT_DEREGISTERED.value:
                self._handle_agent_deregistered(event)

            elif event_type == RegistryEventType.AGENT_UPDATED.value:
                self._handle_agent_updated(event)

            elif event_type == "registry.agent.card.updated":
                self._handle_agent_card_updated(event)

            elif event_type == RegistryEventType.DISCOVERY_REQUEST.value:
                self._handle_discovery_request(event)

            else:
                logger.debug(f"Unhandled event type: {event_type}")

        except Exception as e:
            logger.error(f"Error handling message: {e}")

    # -------------------------------------------------------------------------
    # Event handlers
    # -------------------------------------------------------------------------

    def _handle_heartbeat(self, event: Event) -> None:
        """Handle agent.heartbeat event - update heartbeat timestamp and metrics."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Heartbeat missing agent_id for {agent_name}")
            return

        with self._registry_lock:
            if agent_id not in self._registry:
                # Discard heartbeats from agents that have not yet sent a
                # registry.agent.registered event.  Auto-registering here
                # produces an entry with empty capabilities, which poisons
                # capability-based discovery until the real registration arrives.
                logger.debug(
                    "Heartbeat from unregistered agent %s, ignoring until "
                    "registry.agent.registered arrives",
                    agent_id,
                )
                return

            # Update existing entry only -- no auto-registration
            agent = self._registry[agent_id]
            agent.last_heartbeat = datetime.utcnow()
            agent.status = payload.get("status", agent.status)
            agent.metrics = payload.get("metrics", agent.metrics)
            agent.last_updated = datetime.utcnow()

            # Update replica info if present
            if payload.get("replica_count") is not None:
                agent.replica_count = payload.get("replica_count")
            if payload.get("max_replicas") is not None:
                agent.max_replicas = payload.get("max_replicas")
            if payload.get("replica_index") is not None:
                agent.replica_index = payload.get("replica_index")

        logger.debug(f"Heartbeat processed for {agent_id}")

    def _handle_agent_spawned(self, event: Event) -> None:
        """Handle agent.spawned event - register new agent."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Spawned event missing agent_id for {agent_name}")
            return

        agent = RegisteredAgent(
            agent_id=agent_id,
            agent_name=agent_name,
            agent_type=payload.get("agent_type"),
            instance_id=payload.get("instance_id"),
            host=payload.get("host"),
            port=payload.get("port"),
            version=payload.get("version", "1.0.0"),
            group_id=payload.get("group_id"),
            capabilities=payload.get("capabilities", []),
            topics_consumed=payload.get("subscribed_topics", []),
            topics_produced=payload.get("produced_topics", []),
            status=AgentStatus.HEALTHY.value,
            registered_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
            replica_index=payload.get("replica_index"),
            replica_count=payload.get("replica_count"),
            max_replicas=payload.get("max_replicas"),
        )

        self.register_agent(agent)
        logger.info(f"Agent spawned and registered: {agent_id}")

    def _handle_agent_stopped(self, event: Event) -> None:
        """Handle agent.stopped event - remove agent from registry."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if agent_id:
            self.remove_agent(agent_id)
            logger.info(f"Agent stopped and deregistered: {agent_id}")
        else:
            logger.warning(f"Stopped event missing agent_id for {agent_name}")

    def _handle_restart_completed(self, event: Event) -> None:
        """Handle agent.restart.completed - update agent status."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Restart event missing agent_id for {agent_name}")
            return

        with self._registry_lock:
            if agent_id in self._registry:
                agent = self._registry[agent_id]
                agent.status = payload.get("status", AgentStatus.HEALTHY.value)
                agent.last_updated = datetime.utcnow()

                # Update restart count in metrics
                if "restart_count" in payload:
                    if "restart_count" not in agent.metrics:
                        agent.metrics = dict(agent.metrics)
                    agent.metrics["restart_count"] = payload["restart_count"]

                logger.info(f"Agent restart completed: {agent_id}")

                # Publish update event
                self._publish_agent_updated(agent, reason="restart_completed")

    def _handle_agent_registered(self, event: Event) -> None:
        """Handle registry.agent.registered - register or update agent."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Registration event missing agent_id for {agent_name}")
            return

        # Extract topics
        topics = payload.get("topics", {})
        topics_consumed = []
        topics_produced = []
        if isinstance(topics, dict):
            topics_consumed = topics.get("consumes", [])
            topics_produced = topics.get("produces", [])

        agent = RegisteredAgent(
            agent_id=agent_id,
            agent_name=agent_name,
            agent_type=payload.get("agent_type"),
            instance_id=payload.get("instance_id"),
            host=payload.get("host"),
            port=payload.get("port"),
            version=payload.get("version", "1.0.0"),
            group_id=payload.get("group_id"),
            capabilities=payload.get("capabilities", []),
            topics_consumed=topics_consumed,
            topics_produced=topics_produced,
            status=payload.get("status", AgentStatus.HEALTHY.value),
            registered_at=datetime.utcnow(),
            last_updated=datetime.utcnow(),
            replica_index=payload.get("replica_index"),
            replica_count=payload.get("replica_count"),
            max_replicas=payload.get("max_replicas"),
        )

        with self._registry_lock:
            is_new = agent_id not in self._registry
            was_auto_registered = False

            # Check if this was auto-registered from heartbeat (has heartbeat but minimal fields)
            if agent_id in self._registry:
                existing = self._registry[agent_id]
                was_auto_registered = existing.last_heartbeat is not None
                # Preserve heartbeat timestamp from existing entry
                agent.last_heartbeat = existing.last_heartbeat

            self._registry[agent_id] = agent
            self._registry_updates += 1

        # Only trigger topology change if truly new (not if upgrading from auto-registered)
        if is_new and not was_auto_registered:
            self._mark_topology_changed()
            logger.info(f"Agent registered: {agent_id}")
        elif was_auto_registered:
            logger.info(f"Agent registration upgraded from auto-register: {agent_id}")

    def _handle_agent_deregistered(self, event: Event) -> None:
        """Handle registry.agent.deregistered - remove agent."""
        # Ignore events we published ourselves (from remove_agent -> _publish_agent_deregistered)
        if event.event_source == self.service_id:
            logger.debug(f"Ignoring self-published deregistration event")
            return

        payload = event.payload
        agent_id = payload.get("agent_id")

        if agent_id:
            removed = self.remove_agent(agent_id)
            if removed:
                logger.info(f"Agent deregistered via event: {agent_id}")

    def _handle_agent_updated(self, event: Event) -> None:
        """Handle registry.agent.updated - update agent metadata."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Update event missing agent_id for {agent_name}")
            return

        with self._registry_lock:
            if agent_id in self._registry:
                agent = self._registry[agent_id]

                # Update fields if present
                if "status" in payload:
                    agent.status = payload["status"]
                if "capabilities" in payload:
                    agent.capabilities = payload["capabilities"]
                if "version" in payload:
                    agent.version = payload["version"]
                if "host" in payload:
                    agent.host = payload["host"]
                if "port" in payload:
                    agent.port = payload["port"]
                if "metrics" in payload:
                    agent.metrics = payload["metrics"]

                agent.last_updated = datetime.utcnow()

                logger.info(f"Agent updated: {agent_id}")

    def _handle_agent_card_updated(self, event: Event) -> None:
        """Handle registry.agent.card.updated - store agent card for discovery."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if not agent_id:
            logger.warning(f"Agent card missing agent_id for {agent_name}")
            return

        try:
            # Create AgentCard from payload
            agent_card = AgentCard.from_dict(payload)

            with self._registry_lock:
                # Update existing agent or create new entry
                if agent_id in self._registry:
                    agent = self._registry[agent_id]
                    agent.agent_card = agent_card

                    # Update agent fields from card
                    agent.capabilities = agent_card.capabilities
                    agent.topics_consumed = agent_card.topics_consumed
                    agent.topics_produced = agent_card.topics_produced
                    agent.version = agent_card.version
                    if agent_card.status is not None:
                        agent.status = agent_card.status
                    agent.replica_index = agent_card.replica_index
                    agent.replica_count = agent_card.replica_count
                    agent.max_replicas = agent_card.max_replicas
                    agent.last_updated = datetime.utcnow()

                    logger.info(f"Agent card updated: {agent_id}")
                else:
                    # Create minimal entry if agent not yet registered
                    self._registry[agent_id] = RegisteredAgent(
                        agent_id=agent_id,
                        agent_name=agent_name,
                        agent_type=agent_card.agent_type,
                        instance_id=agent_card.instance_id,
                        host=agent_card.host,
                        version=agent_card.version,
                        group_id=agent_card.group_id,
                        capabilities=agent_card.capabilities,
                        topics_consumed=agent_card.topics_consumed,
                        topics_produced=agent_card.topics_produced,
                        status=agent_card.status if agent_card.status is not None else AgentStatus.HEALTHY.value,
                        replica_index=agent_card.replica_index,
                        replica_count=agent_card.replica_count,
                        max_replicas=agent_card.max_replicas,
                        registered_at=datetime.utcnow(),
                        last_updated=datetime.utcnow(),
                        agent_card=agent_card,
                    )
                    logger.info(f"Agent card registered: {agent_id}")

            # Publish updated event
            with self._registry_lock:
                agent = self._registry.get(agent_id)
                if agent:
                    self._publish_agent_updated(agent, "agent_card_updated")

            # Mark topology changed
            self._mark_topology_changed()

        except Exception as e:
            logger.error(f"Failed to process agent card for {agent_id}: {e}")

    def _handle_discovery_request(self, event: Event) -> None:
        """Handle registry.discovery.request - respond with matching agents."""
        payload = event.payload
        requester = event.event_source
        correlation_id = event.correlation_id

        # Extract query criteria
        capability = payload.get("capability")
        agent_name = payload.get("agent_name")
        status_filter = payload.get("status")

        # Query registry
        with self._registry_lock:
            results = list(self._registry.values())

        # Apply filters
        if capability:
            results = [a for a in results if capability in a.capabilities]
        if agent_name:
            results = [a for a in results if a.agent_name == agent_name]
        if status_filter:
            results = [a for a in results if a.status == status_filter]

        # Publish discovery response
        self._publish_discovery_response(
            requester=requester,
            agents=results,
            query=payload,
            correlation_id=correlation_id,
        )

        logger.info(f"Discovery request processed: {len(results)} agents matched")

    # -------------------------------------------------------------------------
    # Registry management methods
    # -------------------------------------------------------------------------

    def register_agent(self, agent: RegisteredAgent) -> None:
        """
        Register a new agent or update existing one.

        Args:
            agent: RegisteredAgent instance to register
        """
        with self._registry_lock:
            is_new = agent.agent_id not in self._registry

            if is_new:
                agent.registered_at = datetime.utcnow()
            agent.last_updated = datetime.utcnow()

            self._registry[agent.agent_id] = agent
            self._registry_updates += 1

        if is_new:
            self._mark_topology_changed()
            self._publish_agent_registered(agent)
            logger.info(f"Agent registered: {agent.agent_id} ({agent.agent_name})")
        else:
            self._publish_agent_updated(agent, reason="manual_update")
            logger.info(f"Agent updated: {agent.agent_id}")

    def update_agent(
        self,
        agent_id: str,
        status: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
        capabilities: Optional[List[str]] = None,
    ) -> bool:
        """
        Update agent metadata.

        Args:
            agent_id: Agent ID to update
            status: New status
            metrics: New metrics
            capabilities: New capabilities

        Returns:
            True if agent was updated, False if not found
        """
        with self._registry_lock:
            if agent_id not in self._registry:
                logger.warning(f"Cannot update unknown agent: {agent_id}")
                return False

            agent = self._registry[agent_id]
            updated = False

            if status is not None and status != agent.status:
                agent.status = status
                updated = True

            if metrics is not None:
                agent.metrics = metrics
                updated = True

            if capabilities is not None and capabilities != agent.capabilities:
                agent.capabilities = capabilities
                updated = True

            if updated:
                agent.last_updated = datetime.utcnow()
                self._registry_updates += 1

        if updated:
            self._publish_agent_updated(agent, reason="metadata_update")
            logger.info(f"Agent updated: {agent_id}")

        return updated

    def remove_agent(self, agent_id: str) -> bool:
        """
        Remove agent from registry.

        Args:
            agent_id: Agent ID to remove

        Returns:
            True if agent was removed, False if not found
        """
        with self._registry_lock:
            if agent_id not in self._registry:
                return False

            agent = self._registry.pop(agent_id)

        self._mark_topology_changed()
        logger.info(f"Agent removed from registry: {agent_id}")

        # Publish deregistration event
        self._publish_agent_deregistered(agent)

        return True

    # -------------------------------------------------------------------------
    # Query methods
    # -------------------------------------------------------------------------

    def find_by_capability(self, capability: str) -> List[RegisteredAgent]:
        """
        Find agents with a specific capability.

        Args:
            capability: Capability to search for

        Returns:
            List of matching agents
        """
        with self._registry_lock:
            return [
                agent for agent in self._registry.values()
                if capability in agent.capabilities
            ]

    def find_by_name(self, agent_name: str) -> List[RegisteredAgent]:
        """
        Find all agents with a specific name (all replicas).

        Args:
            agent_name: Agent name to search for

        Returns:
            List of matching agents (replicas)
        """
        with self._registry_lock:
            return [
                agent for agent in self._registry.values()
                if agent.agent_name == agent_name
            ]

    def find_by_id(self, agent_id: str) -> Optional[RegisteredAgent]:
        """
        Find agent by ID.

        Args:
            agent_id: Agent ID to find

        Returns:
            RegisteredAgent or None if not found
        """
        with self._registry_lock:
            return self._registry.get(agent_id)

    def find_healthy(self) -> List[RegisteredAgent]:
        """
        Find all healthy agents.

        Returns:
            List of healthy agents
        """
        with self._registry_lock:
            return [
                agent for agent in self._registry.values()
                if agent.is_healthy(self.HEARTBEAT_TIMEOUT_SECONDS)
            ]

    def find_by_type(self, agent_type: str) -> List[RegisteredAgent]:
        """
        Find agents by agent type.

        Args:
            agent_type: Agent type to search for

        Returns:
            List of matching agents
        """
        with self._registry_lock:
            return [
                agent for agent in self._registry.values()
                if agent.agent_type == agent_type
            ]

    def find_healthy_by_capability(self, capability: str) -> List[RegisteredAgent]:
        """
        Find healthy agents with a specific capability.

        Args:
            capability: Capability to search for

        Returns:
            List of healthy agents with the capability
        """
        with self._registry_lock:
            return [
                agent for agent in self._registry.values()
                if capability in agent.capabilities
                and agent.is_healthy(self.HEARTBEAT_TIMEOUT_SECONDS)
            ]

    def find_best_agent(self, capability: str) -> Optional[RegisteredAgent]:
        """
        Find the best (most available) agent with a specific capability.

        Selection criteria (in order):
        1. Agent must be healthy
        2. Agent must have the capability
        3. Lowest consumer lag
        4. Lowest CPU usage
        5. Most recent heartbeat

        Args:
            capability: Required capability

        Returns:
            Best matching agent, or None if no healthy agent found
        """
        candidates = self.find_healthy_by_capability(capability)
        if not candidates:
            return None

        def score_agent(agent: RegisteredAgent) -> tuple:
            """Score agent for selection (lower is better)."""
            # Metrics
            lag = agent.metrics.get("consumer_lag", 0) if agent.metrics else 0
            cpu = agent.metrics.get("cpu_percent", 0) if agent.metrics else 0

            # Heartbeat recency (negative because more recent is better)
            heartbeat_age = 0
            if agent.last_heartbeat:
                heartbeat_age = (datetime.utcnow() - agent.last_heartbeat).total_seconds()

            return (lag, cpu, heartbeat_age)

        # Sort by score (ascending) and return best
        candidates.sort(key=score_agent)
        return candidates[0]

    def get_all_agents(self) -> List[RegisteredAgent]:
        """
        Get all registered agents.

        Returns:
            List of all agents
        """
        with self._registry_lock:
            return list(self._registry.values())

    # -------------------------------------------------------------------------
    # Topology Helper Methods
    # -------------------------------------------------------------------------

    def get_all_agent_cards(self) -> List[AgentCard]:
        """
        Get all registered agents as AgentCard objects.

        Returns:
            List of AgentCard objects
        """
        with self._registry_lock:
            return [a.agent_card for a in self._registry.values() if a.agent_card]

    def get_agents_by_host(self, host: str) -> List[AgentCard]:
        """
        Return agents running on a host.

        Args:
            host: Host name to filter by

        Returns:
            List of AgentCard objects on the specified host
        """
        with self._registry_lock:
            return [
                a.agent_card for a in self._registry.values()
                if a.agent_card and a.host == host
            ]

    def get_agents_by_type(self, agent_name: str) -> List[AgentCard]:
        """
        Return all replicas of an agent type.

        Args:
            agent_name: Agent name/type to filter by

        Returns:
            List of AgentCard objects matching the agent name
        """
        with self._registry_lock:
            return [
                a.agent_card for a in self._registry.values()
                if a.agent_card and a.agent_name == agent_name
            ]

    def get_replica_count(self, agent_name: str) -> int:
        """
        Get the count of replicas for an agent type.

        Args:
            agent_name: Agent name/type

        Returns:
            Number of replicas
        """
        return len(self.get_agents_by_type(agent_name))

    def get_host_load(self, host: str) -> int:
        """
        Get host load (number of agents on host).

        Args:
            host: Host name

        Returns:
            Number of agents running on the host
        """
        return len(self.get_agents_by_host(host))

    def get_hosts(self) -> List[str]:
        """
        Return all known hosts.

        Returns:
            List of unique host names
        """
        with self._registry_lock:
            hosts = set()
            for a in self._registry.values():
                if a.host:
                    hosts.add(a.host)
            return list(hosts)

    def get_least_loaded_host(self) -> Optional[str]:
        """
        Get the host with the least number of agents.

        Returns:
            Host name with minimum load, or None if no hosts exist
        """
        hosts = self.get_hosts()

        if not hosts:
            return None

        best_host = None
        best_load = None

        for h in hosts:
            load = self.get_host_load(h)

            if best_load is None or load < best_load:
                best_host = h
                best_load = load

        return best_host

    def get_topology(self) -> Dict[str, Any]:
        """
        Get system topology.

        Returns:
            Dictionary with topology information:
            - agents: List of all agents
            - agents_by_type: Count per agent type
            - agents_by_host: Count per host
            - total_agents: Total count
            - healthy_agents: Healthy count
            - topology_version: Current version
        """
        with self._registry_lock:
            agents = list(self._registry.values())
            healthy = [a for a in agents if a.is_healthy(self.HEARTBEAT_TIMEOUT_SECONDS)]

            # Group by type
            agents_by_type: Dict[str, int] = {}
            for agent in agents:
                agents_by_type[agent.agent_name] = agents_by_type.get(agent.agent_name, 0) + 1

            # Group by host
            agents_by_host: Dict[str, int] = {}
            for agent in agents:
                if agent.host:
                    agents_by_host[agent.host] = agents_by_host.get(agent.host, 0) + 1

            # Group by capability
            capabilities_map: Dict[str, List[str]] = {}
            for agent in agents:
                for cap in agent.capabilities:
                    if cap not in capabilities_map:
                        capabilities_map[cap] = []
                    capabilities_map[cap].append(agent.agent_id)

            return {
                "topology_version": self._topology_version,
                "total_agents": len(agents),
                "healthy_agents": len(healthy),
                "agents_by_type": agents_by_type,
                "agents_by_host": agents_by_host,
                "capabilities_map": capabilities_map,
                "agents": [a.to_dict() for a in agents],
                "timestamp": datetime.utcnow().isoformat(),
            }

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    def _cleanup_loop(self) -> None:
        """Periodic cleanup of stale agents."""
        logger.info("Registry cleanup loop started")

        while self._running:
            try:
                self._cleanup_stale_agents()
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")

            # Wait for next cleanup cycle
            self._shutdown_event.wait(timeout=self.CLEANUP_INTERVAL_SECONDS)

        logger.info("Registry cleanup loop stopped")

    def _cleanup_stale_agents(self) -> None:
        """Remove agents with stale heartbeats."""
        now = datetime.utcnow()
        stale_agents = []

        with self._registry_lock:
            for agent_id, agent in list(self._registry.items()):
                should_remove = False

                if agent.last_heartbeat is not None:
                    # Agent has sent at least one heartbeat - check if stale
                    age_seconds = (now - agent.last_heartbeat).total_seconds()
                    if age_seconds > self.STALE_AGENT_TIMEOUT_SECONDS:
                        should_remove = True
                        logger.debug(f"Agent {agent_id} stale: last heartbeat {age_seconds:.1f}s ago")
                else:
                    # Agent has never sent a heartbeat
                    if agent.registered_at is not None:
                        age_seconds = (now - agent.registered_at).total_seconds()
                        if age_seconds > self.STALE_AGENT_TIMEOUT_SECONDS:
                            # Zombie agent - registered but never heartbeated
                            should_remove = True
                            logger.debug(f"Agent {agent_id} zombie: registered {age_seconds:.1f}s ago, no heartbeat")
                    # If no registered_at, don't remove (defensive)

                if should_remove:
                    stale_agents.append(agent_id)

        # Remove stale agents outside the lock
        for agent_id in stale_agents:
            self.remove_agent(agent_id)
            logger.warning(f"Removed stale agent: {agent_id}")

    # -------------------------------------------------------------------------
    # Topology tracking
    # -------------------------------------------------------------------------

    def _mark_topology_changed(self) -> None:
        """Mark that topology has changed and emit event if needed."""
        now = datetime.utcnow()
        self._topology_version += 1
        self._last_topology_change = now
        self._topology_changes += 1

        # Check cooldown before emitting event
        if self._last_topology_event is not None:
            elapsed = (now - self._last_topology_event).total_seconds()
            if elapsed < self.TOPOLOGY_CHANGE_COOLDOWN_SECONDS:
                return  # Still in cooldown

        # Emit topology changed event
        self._publish_topology_changed()
        self._last_topology_event = now

    # -------------------------------------------------------------------------
    # Event publishers
    # -------------------------------------------------------------------------

    def _publish_agent_registered(self, agent: RegisteredAgent) -> None:
        """Publish registry.agent.registered event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "port": agent.port,
            "capabilities": agent.capabilities,
            "topics": {
                "consumes": agent.topics_consumed,
                "produces": agent.topics_produced,
            },
            "status": agent.status,
            "version": agent.version,
            "registered_at": agent.registered_at.isoformat() if agent.registered_at else None,
            "group_id": agent.group_id,
            "replica_index": agent.replica_index,
            "replica_count": agent.replica_count,
            "max_replicas": agent.max_replicas,
        }

        event_dict = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_type": RegistryEventType.AGENT_REGISTERED.value,
            "event_source": self.service_id,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": f"corr_{uuid.uuid4()}",
            "entity_id": agent.agent_name,
            "schema_version": "1.1",
            "payload": payload,
            "metadata": {"environment": "production"},
        }

        self._publish_queue.append((
            "acis.registry",
            event_dict,
            agent.agent_id,
        ))

    def _publish_agent_updated(self, agent: RegisteredAgent, reason: str) -> None:
        """Publish registry.agent.updated event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "status": agent.status,
            "metrics": agent.metrics,
            "updated_at": datetime.utcnow().isoformat(),
            "update_reason": reason,
            "replica_count": agent.replica_count,
            "replica_index": agent.replica_index,
        }

        event_dict = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_type": RegistryEventType.AGENT_UPDATED.value,
            "event_source": self.service_id,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": f"corr_{uuid.uuid4()}",
            "entity_id": agent.agent_name,
            "schema_version": "1.1",
            "payload": payload,
            "metadata": {"environment": "production"},
        }

        self._publish_queue.append((
            "acis.registry",
            event_dict,
            agent.agent_id,
        ))

    def _publish_agent_deregistered(self, agent: RegisteredAgent) -> None:
        """Publish registry.agent.deregistered event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "capabilities": agent.capabilities,
            "topics": None,
            "status": "deregistered",
            "version": agent.version,
            "registered_at": None,
            "deregistered_at": datetime.utcnow().isoformat(),
        }

        event_dict = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_type": RegistryEventType.AGENT_DEREGISTERED.value,
            "event_source": self.service_id,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": f"corr_{uuid.uuid4()}",
            "entity_id": agent.agent_name,
            "schema_version": "1.1",
            "payload": payload,
            "metadata": {"environment": "production"},
        }

        self._publish_queue.append((
            "acis.registry",
            event_dict,
            agent.agent_id,
        ))

    def _publish_topology_changed(self) -> None:
        """Publish registry.topology.changed event."""
        topology = self.get_topology()

        payload = {
            "topology_version": self._topology_version,
            "total_agents": topology["total_agents"],
            "healthy_agents": topology["healthy_agents"],
            "agents_by_type": topology["agents_by_type"],
            "agents_by_host": topology["agents_by_host"],
            "change_type": "agents_added_or_removed",
            "changed_at": datetime.utcnow().isoformat(),
        }

        event_dict = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_type": RegistryEventType.TOPOLOGY_CHANGED.value,
            "event_source": self.service_id,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": f"corr_{uuid.uuid4()}",
            "entity_id": "system_topology",
            "schema_version": "1.1",
            "payload": payload,
            "metadata": {"environment": "production"},
        }

        self._publish_queue.append((
            "acis.registry",
            event_dict,
            "topology",
        ))

        logger.info(f"Topology changed event queued (version: {self._topology_version})")

    def _publish_discovery_response(
        self,
        requester: str,
        agents: List[RegisteredAgent],
        query: Dict[str, Any],
        correlation_id: Optional[str],
    ) -> None:
        """Publish discovery response event."""
        payload = {
            "requester": requester,
            "query": query,
            "results": [a.to_dict() for a in agents],
            "result_count": len(agents),
            "responded_at": datetime.utcnow().isoformat(),
        }

        event_dict = {
            "event_id": f"evt_{uuid.uuid4()}",
            "event_type": RegistryEventType.DISCOVERY_RESPONSE.value,
            "event_source": self.service_id,
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id or f"corr_{uuid.uuid4()}",
            "entity_id": requester,
            "schema_version": "1.1",
            "payload": payload,
            "metadata": {"environment": "production"},
        }

        self._publish_queue.append((
            "acis.registry",
            event_dict,
            requester,
        ))

    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        with self._registry_lock:
            total = len(self._registry)
            healthy = len([a for a in self._registry.values() if a.is_healthy(self.HEARTBEAT_TIMEOUT_SECONDS)])

            # Count by status
            status_counts: Dict[str, int] = {}
            for agent in self._registry.values():
                status_counts[agent.status] = status_counts.get(agent.status, 0) + 1

            # Count by type
            type_counts: Dict[str, int] = {}
            for agent in self._registry.values():
                type_counts[agent.agent_name] = type_counts.get(agent.agent_name, 0) + 1

            uptime_seconds = 0
            if self._start_time:
                uptime_seconds = int((datetime.utcnow() - self._start_time).total_seconds())

            return {
                "service_id": self.service_id,
                "total_agents": total,
                "healthy_agents": healthy,
                "status_counts": status_counts,
                "type_counts": type_counts,
                "topology_version": self._topology_version,
                "topology_changes": self._topology_changes,
                "events_processed": self._events_processed,
                "registry_updates": self._registry_updates,
                "uptime_seconds": uptime_seconds,
            }

    # -------------------------------------------------------------------------
    # Admin operations
    # -------------------------------------------------------------------------

    def clear_registry(self) -> int:
        """
        Clear all agents from registry (for testing/admin).

        Returns:
            Number of agents removed
        """
        with self._registry_lock:
            count = len(self._registry)
            self._registry.clear()

        if count > 0:
            self._mark_topology_changed()
            logger.warning(f"Registry cleared: {count} agents removed")

        return count

    def export_registry(self) -> Dict[str, Any]:
        """
        Export registry to JSON-serializable dictionary.

        Returns:
            Complete registry snapshot
        """
        topology = self.get_topology()
        stats = self.get_stats()

        return {
            "topology": topology,
            "stats": stats,
            "exported_at": datetime.utcnow().isoformat(),
        }
