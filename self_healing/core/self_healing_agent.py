"""
SelfHealingAgent - Production-level decision engine for ACIS-X.

Monitors agent health across the system and emits command events
for orchestration actions (restart, scale, spawn, shutdown).

Subscribes to:
    - acis.system (metrics, overload events)
    - acis.agent.health (heartbeats, health status)
    - acis.registry (agent registration/deregistration)

Publishes commands to:
    - acis.system (agent.restart.requested, agent.spawn.requested,
                   agent.scale.requested, agent.shutdown.requested)
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
    create_restart_request_event,
    create_spawn_request_event,
    create_scale_request_event,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Agent State Tracking
# =============================================================================

@dataclass
class AgentState:
    """Tracks the health and metrics state of a single agent."""

    agent_id: str
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: Optional[str] = None
    host: Optional[str] = None

    # Health tracking
    last_heartbeat: Optional[datetime] = None
    status: str = AgentStatus.HEALTHY.value
    registered: bool = False
    registered_at: Optional[datetime] = None

    # Metrics
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    error_count: int = 0
    restart_count: int = 0
    consumer_lag: int = 0
    queue_depth: int = 0
    events_processed: int = 0

    # Replica info
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None
    replica_index: Optional[int] = None

    # Kafka context
    consumer_group: Optional[str] = None
    subscribed_topics: List[str] = field(default_factory=list)

    # Decision tracking (prevent duplicate actions)
    last_restart_requested: Optional[datetime] = None
    last_scale_requested: Optional[datetime] = None
    last_spawn_requested: Optional[datetime] = None

    def update_from_heartbeat(self, payload: Dict[str, Any]) -> None:
        """Update state from heartbeat event payload."""
        self.last_heartbeat = datetime.utcnow()
        self.status = payload.get("status", self.status)
        self.agent_type = payload.get("agent_type", self.agent_type)
        self.instance_id = payload.get("instance_id", self.instance_id)
        self.host = payload.get("host", self.host)

        # Update metrics if present
        metrics = payload.get("metrics", {})
        if metrics:
            self.cpu_percent = metrics.get("cpu_percent", self.cpu_percent)
            self.memory_percent = metrics.get("memory_percent", self.memory_percent)
            self.error_count = metrics.get("error_count", self.error_count) or 0
            self.restart_count = metrics.get("restart_count", self.restart_count) or 0
            self.consumer_lag = metrics.get("consumer_lag", self.consumer_lag) or 0
            self.queue_depth = metrics.get("queue_depth", self.queue_depth) or 0
            self.events_processed = metrics.get("events_processed", self.events_processed) or 0

        # Update replica info
        self.replica_count = payload.get("replica_count", self.replica_count)
        self.max_replicas = payload.get("max_replicas", self.max_replicas)
        self.replica_index = payload.get("replica_index", self.replica_index)

        # Update topics from details
        details = payload.get("details", {})
        if details:
            self.subscribed_topics = details.get("subscribed_topics", self.subscribed_topics)
            self.consumer_group = details.get("group_id", self.consumer_group)

    def update_from_metrics(self, payload: Dict[str, Any]) -> None:
        """Update state from metrics.updated event payload."""
        self.cpu_percent = payload.get("cpu_percent", self.cpu_percent)
        self.memory_percent = payload.get("memory_percent", self.memory_percent)
        self.error_count = payload.get("error_count", self.error_count) or 0
        self.restart_count = payload.get("restart_count", self.restart_count) or 0
        self.consumer_lag = payload.get("consumer_lag", self.consumer_lag) or 0
        self.queue_depth = payload.get("queue_depth", self.queue_depth) or 0
        self.events_processed = payload.get("events_processed", self.events_processed) or 0

        self.replica_count = payload.get("replica_count", self.replica_count)
        self.max_replicas = payload.get("max_replicas", self.max_replicas)
        self.replica_index = payload.get("replica_index", self.replica_index)
        self.consumer_group = payload.get("consumer_group", self.consumer_group)

    def update_from_registry(self, payload: Dict[str, Any], registered: bool) -> None:
        """Update state from registry event payload."""
        self.registered = registered
        self.agent_type = payload.get("agent_type", self.agent_type)
        self.instance_id = payload.get("instance_id", self.instance_id)
        self.host = payload.get("host", self.host)
        self.consumer_group = payload.get("group_id", self.consumer_group)

        if registered:
            self.registered_at = datetime.utcnow()

        topics = payload.get("topics", {})
        if topics and isinstance(topics, dict):
            self.subscribed_topics = topics.get("consumes", self.subscribed_topics)

        self.replica_count = payload.get("replica_count", self.replica_count)
        self.max_replicas = payload.get("max_replicas", self.max_replicas)
        self.replica_index = payload.get("replica_index", self.replica_index)


# =============================================================================
# SelfHealingAgent
# =============================================================================

class SelfHealingAgent(BaseAgent):
    """
    Production-level self-healing agent for ACIS-X.

    Decision rules:
        1. Missing heartbeat (timeout) → restart
        2. Consumer lag > threshold → scale
        3. CPU > 90% → scale
        4. Error count > threshold → restart
        5. Restart count > 3 → spawn new instance
        6. Replica count < max_replicas AND lag high → spawn
        7. Registry missing expected agent → spawn

    Publishes command events to acis.system:
        - agent.restart.requested
        - agent.spawn.requested
        - agent.scale.requested
        - agent.shutdown.requested
    """

    # -------------------------------------------------------------------------
    # Configuration thresholds
    # -------------------------------------------------------------------------

    # Heartbeat timeout (seconds) - rule 1
    HEARTBEAT_TIMEOUT_SECONDS = 90

    # Consumer lag threshold - rules 2 and 6
    LAG_THRESHOLD = 10000

    # CPU threshold - rule 3
    CPU_THRESHOLD = 90.0

    # Error count threshold - rule 4
    ERROR_THRESHOLD = 10

    # Max restarts before spawning new - rule 5
    MAX_RESTART_COUNT = 3

    # Cooldown periods to prevent event spam (seconds)
    RESTART_COOLDOWN_SECONDS = 120
    SCALE_COOLDOWN_SECONDS = 300
    SPAWN_COOLDOWN_SECONDS = 300

    # Decision evaluation interval (seconds)
    DECISION_INTERVAL_SECONDS = 30

    # Minimum time an agent must be registered before decisions apply
    MIN_REGISTRATION_AGE_SECONDS = 60

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        expected_agents: Optional[Set[str]] = None,
    ):
        """
        Initialize SelfHealingAgent.

        Args:
            kafka_client: Kafka client for pub/sub
            agent_version: Version string
            instance_id: Optional instance ID (auto-generated if not provided)
            host: Optional host identifier
            expected_agents: Set of agent names that should always be running (for rule 7)
        """
        super().__init__(
            agent_name="SelfHealingAgent",
            agent_version=agent_version,
            group_id="self-healing-group",
            subscribed_topics=["acis.system", "acis.agent.health", "acis.registry"],
            capabilities=[
                "health_monitoring",
                "decision_engine",
                "restart_orchestration",
                "scale_orchestration",
                "spawn_orchestration",
            ],
            kafka_client=kafka_client,
            agent_type="SelfHealingAgent",
            instance_id=instance_id,
            host=host,
        )

        # Agent state tracking: agent_id -> AgentState (keyed by agent_id for replica support)
        self.agent_state: Dict[str, AgentState] = {}
        self._state_lock = threading.Lock()

        # Expected agents that should always be running (rule 7)
        self.expected_agents: Set[str] = expected_agents or set()

        # Decision loop thread
        self._decision_thread: Optional[threading.Thread] = None

        # Track our own actions to avoid acting on self
        self._self_agent_names = {"SelfHealingAgent", self.agent_name}

    # -------------------------------------------------------------------------
    # BaseAgent abstract methods
    # -------------------------------------------------------------------------

    def subscribe(self) -> List[str]:
        """Return topics to subscribe to."""
        return ["acis.system", "acis.agent.health", "acis.registry"]

    def process_event(self, event: Event) -> None:
        """Process incoming events and update agent state."""
        event_type = event.event_type
        payload = event.payload

        # Route event to appropriate handler
        if event_type == "agent.heartbeat":
            self._handle_heartbeat(event)

        elif event_type == "metrics.updated":
            self._handle_metrics_updated(event)

        elif event_type == "agent.overloaded":
            self._handle_overloaded(event)

        elif event_type == "registry.agent.registered":
            self._handle_agent_registered(event)

        elif event_type == "registry.agent.deregistered":
            self._handle_agent_deregistered(event)

        elif event_type == "agent.health.degraded":
            self._handle_health_degraded(event)

        elif event_type == "agent.health.critical":
            self._handle_health_critical(event)

        elif event_type == "agent.error":
            self._handle_agent_error(event)

        elif event_type == "agent.restart.completed":
            self._handle_restart_completed(event)

        # Ignore our own command events
        elif event_type in (
            "agent.restart.requested",
            "agent.spawn.requested",
            "agent.scale.requested",
            "agent.shutdown.requested",
        ):
            pass  # Ignore command events we may have published

        else:
            logger.debug(f"Unhandled event type: {event_type}")

    # -------------------------------------------------------------------------
    # Lifecycle overrides
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start agent and decision loop."""
        super().start()

        # Start decision evaluation loop
        self._decision_thread = threading.Thread(
            target=self._decision_loop,
            daemon=True,
            name=f"{self.agent_name}-decision-loop"
        )
        self._decision_thread.start()

        logger.info("SelfHealingAgent decision loop started")

    def stop(self) -> None:
        """Stop agent and decision loop."""
        super().stop()

        if self._decision_thread and self._decision_thread.is_alive():
            self._decision_thread.join(timeout=5)

        logger.info("SelfHealingAgent stopped")

    # -------------------------------------------------------------------------
    # Event handlers
    # -------------------------------------------------------------------------

    def _handle_heartbeat(self, event: Event) -> None:
        """Handle agent.heartbeat events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self
        if agent_name in self._self_agent_names:
            return

        # Must have agent_id for proper replica tracking
        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id not in self.agent_state:
                self.agent_state[agent_id] = AgentState(
                    agent_id=agent_id,
                    agent_name=agent_name,
                )

            self.agent_state[agent_id].update_from_heartbeat(payload)

        logger.debug(f"Heartbeat received from {agent_name} (id: {agent_id})")

    def _handle_metrics_updated(self, event: Event) -> None:
        """Handle metrics.updated events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self
        if agent_name in self._self_agent_names:
            return

        # Must have agent_id for proper replica tracking
        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id not in self.agent_state:
                self.agent_state[agent_id] = AgentState(
                    agent_id=agent_id,
                    agent_name=agent_name,
                )

            self.agent_state[agent_id].update_from_metrics(payload)

        logger.debug(f"Metrics updated for {agent_name} (id: {agent_id})")

    def _handle_overloaded(self, event: Event) -> None:
        """Handle agent.overloaded events - may trigger immediate scale."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self
        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id not in self.agent_state:
                self.agent_state[agent_id] = AgentState(
                    agent_id=agent_id,
                    agent_name=agent_name,
                )

            state = self.agent_state[agent_id]
            state.status = AgentStatus.OVERLOADED.value
            state.cpu_percent = payload.get("cpu_percent", state.cpu_percent)
            state.memory_percent = payload.get("memory_percent", state.memory_percent)
            state.consumer_lag = payload.get("consumer_lag", state.consumer_lag) or 0
            state.queue_depth = payload.get("queue_depth", state.queue_depth) or 0

        logger.warning(f"Agent {agent_name} (id: {agent_id}) reported overloaded")

        # Evaluate scaling decision immediately
        self._evaluate_agent(agent_id)

    def _handle_agent_registered(self, event: Event) -> None:
        """Handle registry.agent.registered events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self
        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id not in self.agent_state:
                self.agent_state[agent_id] = AgentState(
                    agent_id=agent_id,
                    agent_name=agent_name,
                )

            self.agent_state[agent_id].update_from_registry(payload, registered=True)

        logger.info(f"Agent {agent_name} (id: {agent_id}) registered")

    def _handle_agent_deregistered(self, event: Event) -> None:
        """Handle registry.agent.deregistered events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        # Skip self
        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id in self.agent_state:
                self.agent_state[agent_id].registered = False
                self.agent_state[agent_id].status = AgentStatus.STOPPED.value

        logger.info(f"Agent {agent_name} (id: {agent_id}) deregistered")

        # Check if this is an expected agent that needs respawning (rule 7)
        if agent_name in self.expected_agents:
            self._evaluate_spawn_missing_agent(agent_name)

    def _handle_health_degraded(self, event: Event) -> None:
        """Handle agent.health.degraded events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id in self.agent_state:
                self.agent_state[agent_id].status = AgentStatus.DEGRADED.value

        logger.warning(f"Agent {agent_name} (id: {agent_id}) health degraded")

    def _handle_health_critical(self, event: Event) -> None:
        """Handle agent.health.critical events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id in self.agent_state:
                self.agent_state[agent_id].status = AgentStatus.CRITICAL.value

        logger.error(f"Agent {agent_name} (id: {agent_id}) health critical")

        # Evaluate restart immediately
        self._evaluate_agent(agent_id)

    def _handle_agent_error(self, event: Event) -> None:
        """Handle agent.error events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id in self.agent_state:
                self.agent_state[agent_id].status = AgentStatus.ERROR.value
                self.agent_state[agent_id].error_count += 1

        logger.error(f"Agent {agent_name} (id: {agent_id}) error event received")

    def _handle_restart_completed(self, event: Event) -> None:
        """Handle agent.restart.completed events."""
        payload = event.payload
        agent_id = payload.get("agent_id")
        agent_name = payload.get("agent_name", event.entity_id)

        if agent_name in self._self_agent_names:
            return

        if not agent_id:
            agent_id = f"agent_{agent_name}"

        with self._state_lock:
            if agent_id in self.agent_state:
                state = self.agent_state[agent_id]
                state.restart_count = payload.get("restart_count", state.restart_count + 1)
                state.status = payload.get("status", AgentStatus.HEALTHY.value)

        logger.info(f"Agent {agent_name} (id: {agent_id}) restart completed")

    # -------------------------------------------------------------------------
    # Decision loop
    # -------------------------------------------------------------------------

    def _decision_loop(self) -> None:
        """Periodic decision evaluation for all tracked agents."""
        logger.info("Decision loop started")

        while self._running:
            try:
                self._evaluate_all_agents()
                self._check_expected_agents()
            except Exception as e:
                logger.error(f"Error in decision loop: {e}")

            # Wait for next evaluation cycle
            self._shutdown_event.wait(timeout=self.DECISION_INTERVAL_SECONDS)

        logger.info("Decision loop stopped")

    def _evaluate_all_agents(self) -> None:
        """Evaluate decision rules for all tracked agents."""
        with self._state_lock:
            agent_ids = list(self.agent_state.keys())

        for agent_id in agent_ids:
            self._evaluate_agent(agent_id)

    def _evaluate_agent(self, agent_id: str) -> None:
        """Evaluate all decision rules for a single agent."""
        with self._state_lock:
            if agent_id not in self.agent_state:
                return
            # Make a copy to avoid holding lock during I/O
            state = self.agent_state[agent_id]

            # FIX 4: Skip self (safety check)
            if state.agent_name in self._self_agent_names:
                return

            state_snapshot = AgentState(
                agent_id=state.agent_id,
                agent_name=state.agent_name,
                agent_type=state.agent_type,
                instance_id=state.instance_id,
                host=state.host,
                last_heartbeat=state.last_heartbeat,
                status=state.status,
                registered=state.registered,
                registered_at=state.registered_at,
                cpu_percent=state.cpu_percent,
                memory_percent=state.memory_percent,
                error_count=state.error_count,
                restart_count=state.restart_count,
                consumer_lag=state.consumer_lag,
                queue_depth=state.queue_depth,
                events_processed=state.events_processed,
                replica_count=state.replica_count,
                max_replicas=state.max_replicas,
                replica_index=state.replica_index,
                consumer_group=state.consumer_group,
                subscribed_topics=list(state.subscribed_topics),
                last_restart_requested=state.last_restart_requested,
                last_scale_requested=state.last_scale_requested,
                last_spawn_requested=state.last_spawn_requested,
            )

        now = datetime.utcnow()

        # FIX 3: Skip agents without instance_id (registry not yet updated)
        if not state_snapshot.instance_id:
            return

        # Skip agents that just registered (give them time to stabilize)
        if state_snapshot.registered_at:
            age_seconds = (now - state_snapshot.registered_at).total_seconds()
            if age_seconds < self.MIN_REGISTRATION_AGE_SECONDS:
                return

        # Rule 5: restart_count > 3 → spawn new (highest priority - agent is failing repeatedly)
        if state_snapshot.restart_count > self.MAX_RESTART_COUNT:
            if self._can_spawn(state_snapshot, now):
                self._request_spawn(
                    state_snapshot,
                    reason=f"Agent exceeded max restart count ({state_snapshot.restart_count} > {self.MAX_RESTART_COUNT})",
                    decision_rule="RESTART_COUNT_EXCEEDED",
                )
                return  # Don't issue other commands

        # Rule 1: Missing heartbeat → restart
        if state_snapshot.last_heartbeat:
            heartbeat_age = (now - state_snapshot.last_heartbeat).total_seconds()
            if heartbeat_age > self.HEARTBEAT_TIMEOUT_SECONDS:
                if self._can_restart(state_snapshot, now):
                    self._request_restart(
                        state_snapshot,
                        reason=f"Heartbeat timeout ({heartbeat_age:.0f}s > {self.HEARTBEAT_TIMEOUT_SECONDS}s)",
                        decision_rule="HEARTBEAT_TIMEOUT",
                    )
                    return

        # Rule 4: errors > threshold → restart
        if state_snapshot.error_count > self.ERROR_THRESHOLD:
            if self._can_restart(state_snapshot, now):
                self._request_restart(
                    state_snapshot,
                    reason=f"Error count exceeded ({state_snapshot.error_count} > {self.ERROR_THRESHOLD})",
                    decision_rule="ERROR_THRESHOLD_EXCEEDED",
                )
                return

        # Rule 6: replica_count < max_replicas AND lag high → spawn
        if (
            state_snapshot.replica_count is not None
            and state_snapshot.max_replicas is not None
            and state_snapshot.replica_count < state_snapshot.max_replicas
            and state_snapshot.consumer_lag > self.LAG_THRESHOLD
        ):
            if self._can_spawn(state_snapshot, now):
                self._request_spawn(
                    state_snapshot,
                    reason=f"High lag ({state_snapshot.consumer_lag}) with room to scale ({state_snapshot.replica_count}/{state_snapshot.max_replicas})",
                    decision_rule="LAG_WITH_CAPACITY",
                )
                return

        # Rule 2: lag > threshold → scale
        if state_snapshot.consumer_lag > self.LAG_THRESHOLD:
            if self._can_scale(state_snapshot, now):
                self._request_scale(
                    state_snapshot,
                    reason=f"Consumer lag exceeded ({state_snapshot.consumer_lag} > {self.LAG_THRESHOLD})",
                    decision_rule="LAG_THRESHOLD_EXCEEDED",
                    trigger_metric="consumer_lag",
                    trigger_value=float(state_snapshot.consumer_lag),
                )
                return

        # Rule 3: CPU > 90 → scale
        if state_snapshot.cpu_percent and state_snapshot.cpu_percent > self.CPU_THRESHOLD:
            if self._can_scale(state_snapshot, now):
                self._request_scale(
                    state_snapshot,
                    reason=f"CPU exceeded ({state_snapshot.cpu_percent:.1f}% > {self.CPU_THRESHOLD}%)",
                    decision_rule="CPU_THRESHOLD_EXCEEDED",
                    trigger_metric="cpu_percent",
                    trigger_value=state_snapshot.cpu_percent,
                )
                return

    def _check_expected_agents(self) -> None:
        """Rule 7: Check if expected agents are missing and spawn them."""
        now = datetime.utcnow()

        for agent_name in self.expected_agents:
            # FIX 2: Search by agent_name across all states (since state is keyed by agent_id)
            with self._state_lock:
                # Find any registered instance of this agent type
                found_registered = any(
                    s.agent_name == agent_name and s.registered
                    for s in self.agent_state.values()
                )
                # Find any instance with recent heartbeat
                found_healthy = any(
                    s.agent_name == agent_name
                    and s.last_heartbeat is not None
                    and (now - s.last_heartbeat).total_seconds() <= self.HEARTBEAT_TIMEOUT_SECONDS * 2
                    for s in self.agent_state.values()
                )

            # Agent not found or not registered
            if not found_registered:
                self._evaluate_spawn_missing_agent(agent_name)
                continue

            # Agent registered but no healthy instance
            if not found_healthy:
                self._evaluate_spawn_missing_agent(agent_name)

    def _evaluate_spawn_missing_agent(self, agent_name: str) -> None:
        """Spawn a missing expected agent (rule 7)."""
        now = datetime.utcnow()

        # Find any existing state for this agent type to check cooldown
        with self._state_lock:
            existing_states = [
                s for s in self.agent_state.values()
                if s.agent_name == agent_name
            ]

        # Check cooldown on any existing state for this agent type
        for state in existing_states:
            if state.last_spawn_requested:
                elapsed = (now - state.last_spawn_requested).total_seconds()
                if elapsed < self.SPAWN_COOLDOWN_SECONDS:
                    return  # Still in cooldown

        # Create a synthetic state for spawn request (using agent_name as temp key)
        # This is for expected agents that have never been seen
        synthetic_agent_id = f"agent_{agent_name}_pending"
        state = AgentState(
            agent_id=synthetic_agent_id,
            agent_name=agent_name,
        )

        self._request_spawn(
            state,
            reason=f"Expected agent {agent_name} not found in registry",
            decision_rule="EXPECTED_AGENT_MISSING",
        )

        # Track spawn request to prevent spam
        with self._state_lock:
            if synthetic_agent_id not in self.agent_state:
                self.agent_state[synthetic_agent_id] = state
            self.agent_state[synthetic_agent_id].last_spawn_requested = now

    # -------------------------------------------------------------------------
    # Cooldown checks
    # -------------------------------------------------------------------------

    def _can_restart(self, state: AgentState, now: datetime) -> bool:
        """Check if restart is allowed (cooldown elapsed)."""
        if state.last_restart_requested is None:
            return True
        elapsed = (now - state.last_restart_requested).total_seconds()
        return elapsed >= self.RESTART_COOLDOWN_SECONDS

    def _can_scale(self, state: AgentState, now: datetime) -> bool:
        """Check if scale is allowed (cooldown elapsed)."""
        if state.last_scale_requested is None:
            return True
        elapsed = (now - state.last_scale_requested).total_seconds()
        return elapsed >= self.SCALE_COOLDOWN_SECONDS

    def _can_spawn(self, state: AgentState, now: datetime) -> bool:
        """Check if spawn is allowed (cooldown elapsed)."""
        if state.last_spawn_requested is None:
            return True
        elapsed = (now - state.last_spawn_requested).total_seconds()
        return elapsed >= self.SPAWN_COOLDOWN_SECONDS

    # -------------------------------------------------------------------------
    # Command publishers
    # -------------------------------------------------------------------------

    def _request_restart(
        self,
        state: AgentState,
        reason: str,
        decision_rule: str,
    ) -> None:
        """Publish agent.restart.requested event."""
        logger.info(
            f"Requesting restart for {state.agent_name}: {reason} "
            f"(rule: {decision_rule})"
        )

        event_data = create_restart_request_event(
            event_source=self.agent_name,
            agent_id=state.agent_id,
            agent_name=state.agent_name,
            instance_id=state.instance_id or f"instance_{state.agent_name}",
            reason=reason,
            agent_type=state.agent_type,
            graceful=True,
            timeout_seconds=30,
            restart_count=state.restart_count,
            max_restarts=self.MAX_RESTART_COUNT,
            decision_rule=decision_rule,
            decision_score=0.95,
            correlation_id=self.create_correlation_id(),
        )

        self.kafka_client.publish(self.SYSTEM_TOPIC, event_data)

        # Update cooldown timestamp (by agent_id)
        with self._state_lock:
            if state.agent_id in self.agent_state:
                self.agent_state[state.agent_id].last_restart_requested = datetime.utcnow()

    def _request_scale(
        self,
        state: AgentState,
        reason: str,
        decision_rule: str,
        trigger_metric: Optional[str] = None,
        trigger_value: Optional[float] = None,
    ) -> None:
        """Publish agent.scale.requested event."""
        current_replicas = state.replica_count or 1
        max_replicas = state.max_replicas or 5
        desired_replicas = min(current_replicas + 1, max_replicas)

        # Don't scale if already at max
        if current_replicas >= max_replicas:
            logger.info(
                f"Skipping scale for {state.agent_name}: already at max replicas "
                f"({current_replicas}/{max_replicas})"
            )
            return

        logger.info(
            f"Requesting scale for {state.agent_name}: {current_replicas} → {desired_replicas} "
            f"({reason}, rule: {decision_rule})"
        )

        event_data = create_scale_request_event(
            event_source=self.agent_name,
            agent_name=state.agent_name,
            current_replicas=current_replicas,
            desired_replicas=desired_replicas,
            max_replicas=max_replicas,
            reason=reason,
            agent_type=state.agent_type,
            trigger_metric=trigger_metric,
            trigger_value=trigger_value,
            decision_rule=decision_rule,
            decision_score=0.90,
            correlation_id=self.create_correlation_id(),
        )

        self.kafka_client.publish(self.SYSTEM_TOPIC, event_data)

        # Update cooldown timestamp (by agent_id)
        with self._state_lock:
            if state.agent_id in self.agent_state:
                self.agent_state[state.agent_id].last_scale_requested = datetime.utcnow()

    def _request_spawn(
        self,
        state: AgentState,
        reason: str,
        decision_rule: str,
    ) -> None:
        """Publish agent.spawn.requested event."""
        logger.info(
            f"Requesting spawn for {state.agent_name}: {reason} "
            f"(rule: {decision_rule})"
        )

        event_data = create_spawn_request_event(
            event_source=self.agent_name,
            agent_name=state.agent_name,
            reason=reason,
            agent_type=state.agent_type,
            priority="high" if decision_rule == "EXPECTED_AGENT_MISSING" else "normal",
            source_agent_id=state.agent_id,
            source_instance_id=state.instance_id,
            replica_count=state.replica_count,
            max_replicas=state.max_replicas,
            decision_rule=decision_rule,
            decision_score=0.95,
            correlation_id=self.create_correlation_id(),
        )

        self.kafka_client.publish(self.SYSTEM_TOPIC, event_data)

        # Update cooldown timestamp (by agent_id)
        with self._state_lock:
            if state.agent_id in self.agent_state:
                self.agent_state[state.agent_id].last_spawn_requested = datetime.utcnow()

    def _request_shutdown(
        self,
        state: AgentState,
        reason: str,
        decision_rule: str,
    ) -> None:
        """Publish agent.shutdown.requested event."""
        logger.info(
            f"Requesting shutdown for {state.agent_name}: {reason} "
            f"(rule: {decision_rule})"
        )

        shutdown_payload = {
            "agent_id": state.agent_id,
            "agent_type": state.agent_type,
            "agent_name": state.agent_name,
            "instance_id": state.instance_id or f"instance_{state.agent_name}",
            "reason": reason,
            "requester": self.agent_name,
            "graceful": True,
            "timeout_seconds": 30,
            "decision_rule": decision_rule,
            "decision_score": 0.90,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_SHUTDOWN_REQUESTED.value,
            entity_id=state.agent_name,
            payload=shutdown_payload,
            correlation_id=self.create_correlation_id(),
        )

    # -------------------------------------------------------------------------
    # State access methods
    # -------------------------------------------------------------------------

    def get_agent_state_by_id(self, agent_id: str) -> Optional[AgentState]:
        """Get state for a specific agent by agent_id."""
        with self._state_lock:
            return self.agent_state.get(agent_id)

    def get_agent_states_by_name(self, agent_name: str) -> List[AgentState]:
        """Get all states for agents with a given name (all replicas)."""
        with self._state_lock:
            return [
                state for state in self.agent_state.values()
                if state.agent_name == agent_name
            ]

    def get_all_agent_states(self) -> Dict[str, AgentState]:
        """Get copy of all agent states."""
        with self._state_lock:
            return dict(self.agent_state)

    def get_unhealthy_agents(self) -> List[str]:
        """Get list of agent_ids with non-healthy status."""
        unhealthy_statuses = {
            AgentStatus.DEGRADED.value,
            AgentStatus.CRITICAL.value,
            AgentStatus.OVERLOADED.value,
            AgentStatus.ERROR.value,
            AgentStatus.TIMEOUT.value,
            AgentStatus.UNREACHABLE.value,
        }

        with self._state_lock:
            return [
                agent_id for agent_id, state in self.agent_state.items()
                if state.status in unhealthy_statuses
            ]

    def add_expected_agent(self, agent_name: str) -> None:
        """Add an agent to the expected agents set."""
        self.expected_agents.add(agent_name)

    def remove_expected_agent(self, agent_name: str) -> None:
        """Remove an agent from the expected agents set."""
        self.expected_agents.discard(agent_name)

    # -------------------------------------------------------------------------
    # Status and diagnostics
    # -------------------------------------------------------------------------

    def get_decision_stats(self) -> Dict[str, Any]:
        """Get statistics about decisions made."""
        with self._state_lock:
            total_agents = len(self.agent_state)
            registered = sum(1 for s in self.agent_state.values() if s.registered)
            healthy = sum(
                1 for s in self.agent_state.values()
                if s.status == AgentStatus.HEALTHY.value
            )

            return {
                "total_tracked_agents": total_agents,
                "registered_agents": registered,
                "healthy_agents": healthy,
                "expected_agents": list(self.expected_agents),
                "unhealthy_agents": self.get_unhealthy_agents(),
                "thresholds": {
                    "heartbeat_timeout_seconds": self.HEARTBEAT_TIMEOUT_SECONDS,
                    "lag_threshold": self.LAG_THRESHOLD,
                    "cpu_threshold": self.CPU_THRESHOLD,
                    "error_threshold": self.ERROR_THRESHOLD,
                    "max_restart_count": self.MAX_RESTART_COUNT,
                },
                "cooldowns": {
                    "restart_seconds": self.RESTART_COOLDOWN_SECONDS,
                    "scale_seconds": self.SCALE_COOLDOWN_SECONDS,
                    "spawn_seconds": self.SPAWN_COOLDOWN_SECONDS,
                },
            }
