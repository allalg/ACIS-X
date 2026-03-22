"""
PlacementAgent - Handles agent lifecycle orchestration for ACIS-X.

Processes spawn/restart/scale/shutdown commands and simulates deployment.
In production, this would integrate with Docker/Kubernetes.

Subscribes to:
    - acis.system (command events from SelfHealingAgent)
    - acis.registry (track agent registrations)

Handles:
    - agent.spawn.requested
    - agent.restart.requested
    - agent.scale.requested
    - agent.shutdown.requested

Publishes:
    - registry.agent.registered (on spawn)
    - registry.agent.deregistered (on shutdown)
    - agent.spawned
    - agent.restart.completed
    - agent.scale.completed
    - placement.completed
"""

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from agents.base.base_agent import BaseAgent
from schemas.event_schema import (
    Event,
    SystemEventType,
    RegistryEventType,
    AgentStatus,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Running Agent Tracking
# =============================================================================

@dataclass
class RunningAgent:
    """Tracks a running agent instance."""

    agent_id: str
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: str = ""
    host: str = "localhost"
    port: Optional[int] = None
    version: str = "1.0.0"
    group_id: Optional[str] = None

    # State
    status: str = AgentStatus.HEALTHY.value
    started_at: Optional[datetime] = None
    restart_count: int = 0

    # Replica info
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None

    # Capabilities and topics
    capabilities: List[str] = field(default_factory=list)
    subscribed_topics: List[str] = field(default_factory=list)
    produced_topics: List[str] = field(default_factory=list)


# =============================================================================
# PlacementAgent
# =============================================================================

class PlacementAgent(BaseAgent):
    """
    Placement agent for ACIS-X orchestration.

    Simulates agent deployment by:
    - Creating instance IDs
    - Publishing registry events
    - Tracking running agents internally

    In production, this would integrate with container orchestration
    (Docker, Kubernetes, etc.).
    """

    # Default agent configurations (simulated)
    DEFAULT_AGENT_CONFIGS: Dict[str, Dict[str, Any]] = {
        "InvoiceMonitoringAgent": {
            "group_id": "invoice-monitoring-group",
            "topics": ["acis.invoices"],
            "capabilities": ["invoice_monitoring", "overdue_detection"],
            "max_replicas": 3,
        },
        "PaymentPredictionAgent": {
            "group_id": "payment-prediction-group",
            "topics": ["acis.payments", "acis.invoices"],
            "capabilities": ["payment_prediction", "ml_inference"],
            "max_replicas": 5,
        },
        "RiskScoringAgent": {
            "group_id": "risk-scoring-group",
            "topics": ["acis.risk", "acis.customers"],
            "capabilities": ["risk_scoring", "credit_assessment"],
            "max_replicas": 3,
        },
        "PolicyEnforcementAgent": {
            "group_id": "policy-enforcement-group",
            "topics": ["acis.policy", "acis.risk"],
            "capabilities": ["policy_enforcement", "compliance_check"],
            "max_replicas": 2,
        },
        "ExternalDataAgent": {
            "group_id": "external-data-group",
            "topics": ["acis.commands", "acis.external"],
            "capabilities": ["external_data_fetch", "api_integration"],
            "max_replicas": 3,
        },
        "CustomerProfileAgent": {
            "group_id": "customer-profile-group",
            "topics": ["acis.customers"],
            "capabilities": ["customer_profiling", "segmentation"],
            "max_replicas": 2,
        },
        "ScenarioGeneratorAgent": {
            "group_id": "scenario-generator-group",
            "topics": [],
            "capabilities": ["scenario_generation", "synthetic_data"],
            "max_replicas": 1,
        },
        "NotificationAgent": {
            "group_id": "notification-group",
            "topics": ["acis.commands"],
            "capabilities": ["notification", "alerting"],
            "max_replicas": 2,
        },
        "AuditLoggingAgent": {
            "group_id": "audit-logging-group",
            "topics": ["acis.system", "acis.registry"],
            "capabilities": ["audit_logging", "compliance_reporting"],
            "max_replicas": 2,
        },
        "RecoveryCoordinatorAgent": {
            "group_id": "recovery-coordinator-group",
            "topics": ["acis.system"],
            "capabilities": ["recovery_coordination", "failover"],
            "max_replicas": 2,
        },
    }

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        simulated_hosts: Optional[List[str]] = None,
    ):
        """
        Initialize PlacementAgent.

        Args:
            kafka_client: Kafka client for pub/sub
            agent_version: Version string
            instance_id: Optional instance ID
            host: Optional host identifier
            simulated_hosts: List of simulated host names for placement
        """
        super().__init__(
            agent_name="PlacementAgent",
            agent_version=agent_version,
            group_id="placement-group",
            subscribed_topics=["acis.system", "acis.registry"],
            capabilities=[
                "agent_placement",
                "spawn_orchestration",
                "restart_orchestration",
                "scale_orchestration",
                "shutdown_orchestration",
            ],
            kafka_client=kafka_client,
            agent_type="PlacementAgent",
            instance_id=instance_id,
            host=host,
        )

        # Running agents: agent_id -> RunningAgent
        self.running_agents: Dict[str, RunningAgent] = {}
        self._agents_lock = threading.Lock()

        # Simulated hosts for round-robin placement
        self.simulated_hosts = simulated_hosts or [
            "host-1.acis.local",
            "host-2.acis.local",
            "host-3.acis.local",
        ]
        self._next_host_index = 0

        # Track agent types and their replica counts
        self._replica_counters: Dict[str, int] = {}

        # Track our own actions to avoid self-loops
        self._self_agent_names = {"PlacementAgent", self.agent_name}

    # -------------------------------------------------------------------------
    # BaseAgent abstract methods
    # -------------------------------------------------------------------------

    def subscribe(self) -> List[str]:
        """Return topics to subscribe to."""
        return ["acis.system", "acis.registry"]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        event_type = event.event_type

        # Handle command events
        if event_type == SystemEventType.AGENT_SPAWN_REQUESTED.value:
            self._handle_spawn_requested(event)

        elif event_type == SystemEventType.AGENT_RESTART_REQUESTED.value:
            self._handle_restart_requested(event)

        elif event_type == SystemEventType.AGENT_SCALE_REQUESTED.value:
            self._handle_scale_requested(event)

        elif event_type == SystemEventType.AGENT_SHUTDOWN_REQUESTED.value:
            self._handle_shutdown_requested(event)

        # Handle registry events (track running agents)
        elif event_type == RegistryEventType.AGENT_REGISTERED.value:
            self._handle_agent_registered(event)

        elif event_type == RegistryEventType.AGENT_DEREGISTERED.value:
            self._handle_agent_deregistered(event)

        else:
            logger.debug(f"Unhandled event type: {event_type}")

    # -------------------------------------------------------------------------
    # Command Handlers
    # -------------------------------------------------------------------------

    def _handle_spawn_requested(self, event: Event) -> None:
        """Handle agent.spawn.requested - create new agent instance."""
        payload = event.payload
        agent_name = payload.get("agent_name")
        agent_type = payload.get("agent_type") or agent_name
        reason = payload.get("reason", "unknown")
        priority = payload.get("priority", "normal")
        requested_host = payload.get("host")
        correlation_id = event.correlation_id

        # Skip self
        if agent_name in self._self_agent_names:
            logger.debug(f"Skipping spawn request for self: {agent_name}")
            return

        logger.info(
            f"Processing spawn request for {agent_name} "
            f"(priority: {priority}, reason: {reason})"
        )

        # Get agent configuration
        config = self._get_agent_config(agent_name)
        max_replicas = config.get("max_replicas", 5)

        # Check current replica count
        current_count = self._get_replica_count(agent_name)
        if current_count >= max_replicas:
            logger.warning(
                f"Cannot spawn {agent_name}: at max replicas ({current_count}/{max_replicas})"
            )
            self._publish_placement_failed(
                agent_name=agent_name,
                agent_type=agent_type,
                reason=f"Max replicas reached ({current_count}/{max_replicas})",
                correlation_id=correlation_id,
            )
            return

        # Simulate spawning
        start_time = datetime.utcnow()
        new_agent = self._create_agent_instance(
            agent_name=agent_name,
            agent_type=agent_type,
            config=config,
            requested_host=requested_host,
        )

        # Track the new agent
        with self._agents_lock:
            self.running_agents[new_agent.agent_id] = new_agent

        spawn_duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        # Publish agent.spawned event
        self._publish_agent_spawned(new_agent, spawn_duration_ms, correlation_id)

        # Publish registry.agent.registered
        self._publish_registry_registered(new_agent, correlation_id)

        # Publish placement.completed
        self._publish_placement_completed(new_agent, correlation_id)

        logger.info(
            f"Spawned {agent_name} as {new_agent.agent_id} on {new_agent.host} "
            f"(replica {new_agent.replica_index + 1}/{new_agent.replica_count})"
        )

    def _handle_restart_requested(self, event: Event) -> None:
        """Handle agent.restart.requested - restart existing agent."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name")
        instance_id = payload.get("instance_id")
        reason = payload.get("reason", "unknown")
        graceful = payload.get("graceful", True)
        correlation_id = event.correlation_id

        # Skip self
        if agent_name in self._self_agent_names:
            return

        logger.info(
            f"Processing restart request for {agent_name} (id: {agent_id}) "
            f"(graceful: {graceful}, reason: {reason})"
        )

        # Find the agent
        with self._agents_lock:
            agent = self.running_agents.get(agent_id)
            if not agent:
                # Try to find by instance_id
                agent = next(
                    (a for a in self.running_agents.values() if a.instance_id == instance_id),
                    None
                )

        if not agent:
            logger.warning(f"Agent not found for restart: {agent_id or instance_id}")
            return

        # Simulate restart
        start_time = datetime.utcnow()

        # Update agent state
        with self._agents_lock:
            if agent.agent_id in self.running_agents:
                self.running_agents[agent.agent_id].restart_count += 1
                self.running_agents[agent.agent_id].status = AgentStatus.HEALTHY.value
                self.running_agents[agent.agent_id].started_at = datetime.utcnow()

        restart_duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        # Publish restart completed
        self._publish_restart_completed(agent, restart_duration_ms, correlation_id)

        logger.info(
            f"Restarted {agent_name} (id: {agent.agent_id}, "
            f"restart_count: {agent.restart_count + 1})"
        )

    def _handle_scale_requested(self, event: Event) -> None:
        """Handle agent.scale.requested - scale agent replicas."""
        payload = event.payload
        agent_name = payload.get("agent_name")
        agent_type = payload.get("agent_type") or agent_name
        current_replicas = payload.get("current_replicas", 1)
        desired_replicas = payload.get("desired_replicas", 2)
        max_replicas = payload.get("max_replicas", 5)
        scale_direction = payload.get("scale_direction", "up")
        reason = payload.get("reason", "unknown")
        correlation_id = event.correlation_id

        # Skip self
        if agent_name in self._self_agent_names:
            return

        logger.info(
            f"Processing scale request for {agent_name}: "
            f"{current_replicas} → {desired_replicas} ({scale_direction})"
        )

        start_time = datetime.utcnow()
        instances_changed = []

        if scale_direction == "up":
            # Spawn additional replicas
            config = self._get_agent_config(agent_name)
            replicas_to_add = min(desired_replicas - current_replicas, max_replicas - current_replicas)

            for _ in range(replicas_to_add):
                new_agent = self._create_agent_instance(
                    agent_name=agent_name,
                    agent_type=agent_type,
                    config=config,
                )

                with self._agents_lock:
                    self.running_agents[new_agent.agent_id] = new_agent

                # Publish registry registration
                self._publish_registry_registered(new_agent, correlation_id)

                instances_changed.append({
                    "agent_id": new_agent.agent_id,
                    "instance_id": new_agent.instance_id,
                    "host": new_agent.host,
                    "action": "added",
                })

        elif scale_direction == "down":
            # Remove replicas (oldest first)
            with self._agents_lock:
                agents_of_type = [
                    a for a in self.running_agents.values()
                    if a.agent_name == agent_name
                ]
                # Sort by started_at (oldest first)
                agents_of_type.sort(key=lambda a: a.started_at or datetime.min)

                replicas_to_remove = current_replicas - desired_replicas
                for agent in agents_of_type[:replicas_to_remove]:
                    del self.running_agents[agent.agent_id]
                    instances_changed.append({
                        "agent_id": agent.agent_id,
                        "instance_id": agent.instance_id,
                        "host": agent.host,
                        "action": "removed",
                    })

                    # Publish deregistration
                    self._publish_registry_deregistered(agent, correlation_id)

        scale_duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        new_replica_count = self._get_replica_count(agent_name)

        # Publish scale completed
        self._publish_scale_completed(
            agent_name=agent_name,
            agent_type=agent_type,
            previous_replicas=current_replicas,
            current_replicas=new_replica_count,
            max_replicas=max_replicas,
            scale_direction=scale_direction,
            scale_duration_ms=scale_duration_ms,
            instances=instances_changed,
            correlation_id=correlation_id,
        )

        logger.info(
            f"Scaled {agent_name}: {current_replicas} → {new_replica_count} "
            f"({len(instances_changed)} instances changed)"
        )

    def _handle_shutdown_requested(self, event: Event) -> None:
        """Handle agent.shutdown.requested - shutdown agent instance."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name")
        instance_id = payload.get("instance_id")
        reason = payload.get("reason", "unknown")
        graceful = payload.get("graceful", True)
        correlation_id = event.correlation_id

        # Skip self
        if agent_name in self._self_agent_names:
            return

        logger.info(
            f"Processing shutdown request for {agent_name} (id: {agent_id}) "
            f"(graceful: {graceful}, reason: {reason})"
        )

        # Find and remove the agent
        with self._agents_lock:
            agent = self.running_agents.get(agent_id)
            if not agent:
                agent = next(
                    (a for a in self.running_agents.values() if a.instance_id == instance_id),
                    None
                )

            if agent and agent.agent_id in self.running_agents:
                del self.running_agents[agent.agent_id]

        if agent:
            # Publish deregistration
            self._publish_registry_deregistered(agent, correlation_id)
            logger.info(f"Shutdown {agent_name} (id: {agent.agent_id})")
        else:
            logger.warning(f"Agent not found for shutdown: {agent_id or instance_id}")

    # -------------------------------------------------------------------------
    # Registry Event Handlers (for tracking)
    # -------------------------------------------------------------------------

    def _handle_agent_registered(self, event: Event) -> None:
        """Track externally registered agents."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self and already tracked agents
        if agent_name in self._self_agent_names:
            return

        with self._agents_lock:
            if agent_id and agent_id not in self.running_agents:
                # Track externally registered agent
                self.running_agents[agent_id] = RunningAgent(
                    agent_id=agent_id,
                    agent_name=agent_name,
                    agent_type=payload.get("agent_type"),
                    instance_id=payload.get("instance_id", ""),
                    host=payload.get("host", "unknown"),
                    version=payload.get("version", "1.0.0"),
                    group_id=payload.get("group_id"),
                    status=AgentStatus.HEALTHY.value,
                    started_at=datetime.utcnow(),
                    replica_index=payload.get("replica_index"),
                    replica_count=payload.get("replica_count"),
                    max_replicas=payload.get("max_replicas"),
                )
                logger.debug(f"Tracking external agent: {agent_id}")

    def _handle_agent_deregistered(self, event: Event) -> None:
        """Remove deregistered agents from tracking."""
        payload = event.payload
        agent_id = payload.get("agent_id")

        with self._agents_lock:
            if agent_id and agent_id in self.running_agents:
                del self.running_agents[agent_id]
                logger.debug(f"Removed agent from tracking: {agent_id}")

    # -------------------------------------------------------------------------
    # Agent Creation
    # -------------------------------------------------------------------------

    def _create_agent_instance(
        self,
        agent_name: str,
        agent_type: Optional[str],
        config: Dict[str, Any],
        requested_host: Optional[str] = None,
    ) -> RunningAgent:
        """Create a new simulated agent instance."""
        # Generate unique identifiers
        instance_uuid = uuid.uuid4().hex[:8]
        instance_id = f"agent_{agent_name.lower()}_{instance_uuid}"
        agent_id = f"agent_{agent_name.lower()}_{instance_id}"

        # Select host
        host = requested_host or self._select_host()

        # Get replica index
        replica_index = self._get_next_replica_index(agent_name)
        replica_count = self._get_replica_count(agent_name) + 1  # Including this one
        max_replicas = config.get("max_replicas", 5)

        return RunningAgent(
            agent_id=agent_id,
            agent_name=agent_name,
            agent_type=agent_type or agent_name,
            instance_id=instance_id,
            host=host,
            port=self._allocate_port(),
            version="1.0.0",
            group_id=config.get("group_id"),
            status=AgentStatus.HEALTHY.value,
            started_at=datetime.utcnow(),
            restart_count=0,
            replica_index=replica_index,
            replica_count=replica_count,
            max_replicas=max_replicas,
            capabilities=config.get("capabilities", []),
            subscribed_topics=config.get("topics", []),
            produced_topics=[],
        )

    def _get_agent_config(self, agent_name: str) -> Dict[str, Any]:
        """Get configuration for agent type."""
        return self.DEFAULT_AGENT_CONFIGS.get(agent_name, {
            "group_id": f"{agent_name.lower()}-group",
            "topics": [],
            "capabilities": [],
            "max_replicas": 5,
        })

    def _select_host(self) -> str:
        """Select host for placement (round-robin)."""
        host = self.simulated_hosts[self._next_host_index]
        self._next_host_index = (self._next_host_index + 1) % len(self.simulated_hosts)
        return host

    def _allocate_port(self) -> int:
        """Allocate a port for the agent (simulated)."""
        return 8000 + len(self.running_agents)

    def _get_replica_count(self, agent_name: str) -> int:
        """Get current replica count for agent type."""
        with self._agents_lock:
            return sum(
                1 for a in self.running_agents.values()
                if a.agent_name == agent_name
            )

    def _get_next_replica_index(self, agent_name: str) -> int:
        """Get next replica index for agent type."""
        if agent_name not in self._replica_counters:
            self._replica_counters[agent_name] = 0
        index = self._replica_counters[agent_name]
        self._replica_counters[agent_name] += 1
        return index

    # -------------------------------------------------------------------------
    # Event Publishers
    # -------------------------------------------------------------------------

    def _publish_agent_spawned(
        self,
        agent: RunningAgent,
        spawn_duration_ms: int,
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.spawned event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "port": agent.port,
            "version": agent.version,
            "group_id": agent.group_id,
            "capabilities": agent.capabilities,
            "subscribed_topics": agent.subscribed_topics,
            "produced_topics": agent.produced_topics,
            "spawned_at": datetime.utcnow().isoformat(),
            "spawn_duration_ms": spawn_duration_ms,
            "replica_index": agent.replica_index,
            "replica_count": agent.replica_count,
            "max_replicas": agent.max_replicas,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_SPAWNED.value,
            entity_id=agent.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_registry_registered(
        self,
        agent: RunningAgent,
        correlation_id: Optional[str],
    ) -> None:
        """Publish registry.agent.registered event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "capabilities": agent.capabilities,
            "topics": {
                "consumes": agent.subscribed_topics,
                "produces": agent.produced_topics,
            },
            "status": "registered",
            "version": agent.version,
            "registered_at": datetime.utcnow().isoformat(),
            "group_id": agent.group_id,
            "replica_index": agent.replica_index,
            "replica_count": agent.replica_count,
            "max_replicas": agent.max_replicas,
        }

        self.publish_event(
            topic=self.REGISTRY_TOPIC,
            event_type=RegistryEventType.AGENT_REGISTERED.value,
            entity_id=agent.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_registry_deregistered(
        self,
        agent: RunningAgent,
        correlation_id: Optional[str],
    ) -> None:
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

        self.publish_event(
            topic=self.REGISTRY_TOPIC,
            event_type=RegistryEventType.AGENT_DEREGISTERED.value,
            entity_id=agent.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_restart_completed(
        self,
        agent: RunningAgent,
        restart_duration_ms: int,
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.restart.completed event."""
        payload = {
            "agent_id": agent.agent_id,
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "version": agent.version,
            "restarted_at": datetime.utcnow().isoformat(),
            "restart_duration_ms": restart_duration_ms,
            "restart_count": agent.restart_count + 1,
            "previous_error": None,
            "status": AgentStatus.HEALTHY.value,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_RESTART_COMPLETED.value,
            entity_id=agent.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_scale_completed(
        self,
        agent_name: str,
        agent_type: Optional[str],
        previous_replicas: int,
        current_replicas: int,
        max_replicas: int,
        scale_direction: str,
        scale_duration_ms: int,
        instances: List[Dict[str, Any]],
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.scale.completed event."""
        payload = {
            "agent_type": agent_type,
            "agent_name": agent_name,
            "previous_replicas": previous_replicas,
            "current_replicas": current_replicas,
            "max_replicas": max_replicas,
            "scale_direction": scale_direction,
            "scaled_at": datetime.utcnow().isoformat(),
            "scale_duration_ms": scale_duration_ms,
            "instances": instances,
            "status": "completed",
            "error_message": None,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_SCALE_COMPLETED.value,
            entity_id=agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_placement_completed(
        self,
        agent: RunningAgent,
        correlation_id: Optional[str],
    ) -> None:
        """Publish placement.completed event."""
        payload = {
            "agent_type": agent.agent_type,
            "agent_name": agent.agent_name,
            "instance_id": agent.instance_id,
            "host": agent.host,
            "port": agent.port,
            "placement_decision": f"Round-robin placement on {agent.host}",
            "alternatives_considered": self.simulated_hosts,
            "placement_duration_ms": 0,
            "status": "completed",
            "error_message": None,
            "decision_rule": "ROUND_ROBIN",
            "decision_score": 1.0,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.PLACEMENT_COMPLETED.value,
            entity_id=agent.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_placement_failed(
        self,
        agent_name: str,
        agent_type: Optional[str],
        reason: str,
        correlation_id: Optional[str],
    ) -> None:
        """Publish placement.completed with failure status."""
        payload = {
            "agent_type": agent_type,
            "agent_name": agent_name,
            "instance_id": None,
            "host": None,
            "port": None,
            "placement_decision": reason,
            "alternatives_considered": self.simulated_hosts,
            "placement_duration_ms": 0,
            "status": "failed",
            "error_message": reason,
            "decision_rule": "MAX_REPLICAS_CHECK",
            "decision_score": 0.0,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.PLACEMENT_COMPLETED.value,
            entity_id=agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    # -------------------------------------------------------------------------
    # State Access Methods
    # -------------------------------------------------------------------------

    def get_running_agents(self) -> Dict[str, RunningAgent]:
        """Get all running agents."""
        with self._agents_lock:
            return dict(self.running_agents)

    def get_agents_by_type(self, agent_name: str) -> List[RunningAgent]:
        """Get all running agents of a specific type."""
        with self._agents_lock:
            return [
                a for a in self.running_agents.values()
                if a.agent_name == agent_name
            ]

    def get_agents_on_host(self, host: str) -> List[RunningAgent]:
        """Get all running agents on a specific host."""
        with self._agents_lock:
            return [
                a for a in self.running_agents.values()
                if a.host == host
            ]

    def get_placement_stats(self) -> Dict[str, Any]:
        """Get placement statistics."""
        with self._agents_lock:
            agents_by_type: Dict[str, int] = {}
            agents_by_host: Dict[str, int] = {}

            for agent in self.running_agents.values():
                agents_by_type[agent.agent_name] = agents_by_type.get(agent.agent_name, 0) + 1
                agents_by_host[agent.host] = agents_by_host.get(agent.host, 0) + 1

            return {
                "total_running_agents": len(self.running_agents),
                "agents_by_type": agents_by_type,
                "agents_by_host": agents_by_host,
                "available_hosts": self.simulated_hosts,
            }
