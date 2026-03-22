"""
SelfHealingAgent - Event-driven recovery decision agent for ACIS-X.

Consumes monitoring and registry events, maintains in-memory recovery state,
and publishes orchestration commands without directly controlling agents.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import (
    AgentStatus,
    Event,
    RegistryEventType,
    SystemEventType,
    create_restart_request_event,
    create_scale_request_event,
    create_spawn_request_event,
)

logger = logging.getLogger(__name__)


@dataclass
class AgentRecoveryState:
    """Tracks recovery-relevant state for one agent instance."""

    agent_id: str
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: Optional[str] = None
    host: Optional[str] = None
    consumer_group: Optional[str] = None

    status: str = AgentStatus.HEALTHY.value
    registered: bool = False
    last_event_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    last_timeout_at: Optional[datetime] = None
    last_degraded_at: Optional[datetime] = None
    last_critical_at: Optional[datetime] = None
    last_overloaded_at: Optional[datetime] = None
    last_lag_at: Optional[datetime] = None

    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    consumer_lag: int = 0
    error_count: int = 0
    latency_ms: Optional[float] = None
    events_per_second: float = 0.0

    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None

    candidate_fallbacks: List[str] = field(default_factory=list)

    last_restart_requested: Optional[datetime] = None
    last_spawn_requested: Optional[datetime] = None
    last_scale_requested: Optional[datetime] = None
    last_fallback_selected: Optional[datetime] = None
    last_recovery_triggered: Optional[datetime] = None
    last_placement_requested: Optional[datetime] = None

    def update_identity(self, payload: Dict[str, Any]) -> None:
        """Refresh agent identity and placement-related metadata."""
        self.agent_type = payload.get("agent_type", self.agent_type)
        self.instance_id = payload.get("instance_id", self.instance_id)
        self.host = payload.get("host", self.host)
        self.consumer_group = (
            payload.get("consumer_group")
            or payload.get("group_id")
            or self.consumer_group
        )
        self.replica_index = payload.get("replica_index", self.replica_index)
        self.replica_count = payload.get("replica_count", self.replica_count)
        self.max_replicas = payload.get("max_replicas", self.max_replicas)


class SelfHealingAgent(BaseAgent):
    """
    Recovery decision engine for ACIS-X.

    Inputs:
    - agent.health.degraded
    - agent.health.critical
    - agent.overloaded
    - lag.detected
    - agent.timeout
    - registry agent lifecycle updates

    Outputs:
    - agent.restart.requested
    - agent.spawn.requested
    - agent.scale.requested
    - placement.requested
    - recovery.triggered
    - fallback.agent.selected
    """

    DECISION_INTERVAL_SECONDS = 15
    RESTART_COOLDOWN_SECONDS = 120
    SPAWN_COOLDOWN_SECONDS = 180
    SCALE_COOLDOWN_SECONDS = 180
    FALLBACK_COOLDOWN_SECONDS = 120
    RECOVERY_EVENT_COOLDOWN_SECONDS = 60
    PLACEMENT_REQUEST_COOLDOWN_SECONDS = 180

    DEGRADED_RESTART_DELAY_SECONDS = 30
    LAG_SCALE_THRESHOLD = 5000
    CRITICAL_LAG_THRESHOLD = 10000
    SCALE_UP_STEP = 1
    DEFAULT_MAX_REPLICAS = 3
    MAX_RESTARTS_BEFORE_SPAWN = 2

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        fallback_agents: Optional[Dict[str, List[str]]] = None,
    ):
        super().__init__(
            agent_name="SelfHealingAgent",
            agent_version=agent_version,
            group_id="self-healing-group",
            subscribed_topics=["acis.system", "acis.agent.health", "acis.registry"],
            capabilities=[
                "recovery_decisioning",
                "restart_decisioning",
                "spawn_decisioning",
                "scale_decisioning",
                "fallback_selection",
            ],
            kafka_client=kafka_client,
            agent_type="SelfHealingAgent",
            instance_id=instance_id,
            host=host,
        )

        self._states: Dict[str, AgentRecoveryState] = {}
        self._state_lock = threading.Lock()
        self._decision_thread: Optional[threading.Thread] = None
        self._fallback_agents = fallback_agents or {}

    def subscribe(self) -> List[str]:
        """Return subscribed Kafka topics."""
        return ["acis.system", "acis.agent.health", "acis.registry"]

    def start(self) -> None:
        """Start agent and periodic evaluation loop."""
        super().start()
        self._decision_thread = threading.Thread(
            target=self._decision_loop,
            daemon=True,
            name=f"{self.agent_name}-decision",
        )
        self._decision_thread.start()

    def stop(self) -> None:
        """Stop agent and periodic evaluation loop."""
        super().stop()
        if self._decision_thread and self._decision_thread.is_alive():
            self._decision_thread.join(timeout=5)

    def process_event(self, event: Event) -> None:
        """Consume monitoring and registry events and emit recovery decisions."""
        if event.event_source == self.agent_name:
            return

        event_type = event.event_type

        if event_type == SystemEventType.AGENT_HEALTH_DEGRADED.value:
            self._handle_degraded(event)
        elif event_type == SystemEventType.AGENT_HEALTH_CRITICAL.value:
            self._handle_critical(event)
        elif event_type == SystemEventType.AGENT_OVERLOADED.value:
            self._handle_overloaded(event)
        elif event_type == SystemEventType.LAG_DETECTED.value:
            self._handle_lag_detected(event)
        elif event_type == SystemEventType.AGENT_TIMEOUT.value:
            self._handle_timeout(event)
        elif event_type in (
            RegistryEventType.AGENT_REGISTERED.value,
            RegistryEventType.AGENT_UPDATED.value,
        ):
            self._handle_registry_upsert(event)
        elif event_type == RegistryEventType.AGENT_DEREGISTERED.value:
            self._handle_registry_removed(event)

    def _decision_loop(self) -> None:
        """Evaluate tracked states periodically for follow-up recovery actions."""
        logger.info("SelfHealing decision loop started")

        while self._running:
            try:
                self._evaluate_all_states()
            except Exception as exc:
                logger.error(f"SelfHealing decision loop error: {exc}")

            self._shutdown_event.wait(timeout=self.DECISION_INTERVAL_SECONDS)

        logger.info("SelfHealing decision loop stopped")

    def _handle_degraded(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.status = AgentStatus.DEGRADED.value
            state.last_event_at = event.event_time
            state.last_degraded_at = event.event_time
            state.update_identity(event.payload)
            self._update_metrics_from_payload(state, event.payload.get("metrics") or event.payload)

        self._evaluate_state(state.agent_id, trigger="degraded")

    def _handle_critical(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.status = AgentStatus.CRITICAL.value
            state.last_event_at = event.event_time
            state.last_critical_at = event.event_time
            state.update_identity(event.payload)
            self._update_metrics_from_payload(state, event.payload.get("metrics") or event.payload)

        self._evaluate_state(state.agent_id, trigger="critical")

    def _handle_overloaded(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.status = AgentStatus.OVERLOADED.value
            state.last_event_at = event.event_time
            state.last_overloaded_at = event.event_time
            state.update_identity(event.payload)
            self._update_metrics_from_payload(state, event.payload)

        self._evaluate_state(state.agent_id, trigger="overloaded")

    def _handle_lag_detected(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.last_event_at = event.event_time
            state.last_lag_at = event.event_time
            state.update_identity(event.payload)
            lag_value = event.payload.get("lag", event.payload.get("consumer_lag", state.consumer_lag))
            state.consumer_lag = int(lag_value or 0)

        self._evaluate_state(state.agent_id, trigger="lag")

    def _handle_timeout(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.status = AgentStatus.TIMEOUT.value
            state.last_event_at = event.event_time
            state.last_timeout_at = event.event_time
            state.update_identity(event.payload)

        self._evaluate_state(state.agent_id, trigger="timeout")

    def _handle_registry_upsert(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.registered = True
            state.last_event_at = event.event_time
            state.update_identity(event.payload)
            payload_status = event.payload.get("status")
            if payload_status and payload_status != "registered":
                state.status = payload_status

    def _handle_registry_removed(self, event: Event) -> None:
        state = self._get_or_create_state(event.payload, event.entity_id)

        with self._state_lock:
            state.registered = False
            state.last_event_at = event.event_time
            state.status = AgentStatus.STOPPED.value

    def _get_or_create_state(self, payload: Dict[str, Any], fallback_name: str) -> AgentRecoveryState:
        """Look up or create a tracked state entry."""
        agent_name = payload.get("agent_name", fallback_name)
        agent_id = payload.get("agent_id") or f"agent_{agent_name}"

        with self._state_lock:
            if agent_id not in self._states:
                self._states[agent_id] = AgentRecoveryState(
                    agent_id=agent_id,
                    agent_name=agent_name,
                    candidate_fallbacks=list(self._fallback_agents.get(agent_name, [])),
                )
            return self._states[agent_id]

    def _update_metrics_from_payload(self, state: AgentRecoveryState, payload: Dict[str, Any]) -> None:
        """Refresh tracked metrics from event payload."""
        if payload.get("cpu_percent") is not None:
            state.cpu_percent = payload["cpu_percent"]
        if payload.get("memory_percent") is not None:
            state.memory_percent = payload["memory_percent"]
        if payload.get("consumer_lag") is not None:
            state.consumer_lag = int(payload["consumer_lag"] or 0)
        if payload.get("lag") is not None:
            state.consumer_lag = int(payload["lag"] or 0)
        if payload.get("error_count") is not None:
            state.error_count = int(payload["error_count"] or 0)
        if payload.get("latency_ms") is not None:
            state.latency_ms = payload["latency_ms"]
        if payload.get("events_per_second") is not None:
            state.events_per_second = float(payload["events_per_second"] or 0.0)

    def _evaluate_all_states(self) -> None:
        """Re-run decision logic across all tracked agents."""
        with self._state_lock:
            agent_ids = list(self._states.keys())

        for agent_id in agent_ids:
            self._evaluate_state(agent_id, trigger="periodic")

    def _evaluate_state(self, agent_id: str, trigger: str) -> None:
        """Apply recovery rules to a single tracked agent."""
        with self._state_lock:
            state = self._states.get(agent_id)
            if state is None or state.agent_name == self.agent_name:
                return

            snapshot = AgentRecoveryState(**state.__dict__)

        now = datetime.utcnow()

        if snapshot.status == AgentStatus.STOPPED.value:
            return

        if snapshot.status == AgentStatus.TIMEOUT.value or trigger == "timeout":
            self._emit_recovery_triggered(snapshot, "restart", "TIMEOUT_DETECTED")
            if self._can_restart(snapshot, now):
                self._publish_restart(snapshot, "Timeout detected by monitoring", "TIMEOUT_DETECTED")
            else:
                self._maybe_publish_fallback(snapshot, "TIMEOUT_DETECTED")
            return

        if snapshot.status == AgentStatus.CRITICAL.value or trigger == "critical":
            self._emit_recovery_triggered(snapshot, "restart", "CRITICAL_HEALTH")
            if self._can_restart(snapshot, now):
                self._publish_restart(snapshot, "Critical health condition reported", "CRITICAL_HEALTH")
            else:
                self._emit_recovery_triggered(snapshot, "spawn", "CRITICAL_HEALTH_ESCALATED")
                self._publish_spawn_and_placement(snapshot, "Critical agent needs replacement capacity", "CRITICAL_HEALTH_ESCALATED")
                self._maybe_publish_fallback(snapshot, "CRITICAL_HEALTH")
            return

        if snapshot.consumer_lag >= self.CRITICAL_LAG_THRESHOLD:
            self._emit_recovery_triggered(snapshot, "scale", "CRITICAL_LAG")
            if self._can_scale(snapshot, now):
                self._publish_scale(snapshot, "Critical consumer lag detected", "CRITICAL_LAG", float(snapshot.consumer_lag))
            else:
                self._publish_spawn_and_placement(snapshot, "Lag remains critical during scale cooldown", "CRITICAL_LAG_ESCALATED")
            return

        if snapshot.status == AgentStatus.OVERLOADED.value or trigger == "overloaded":
            self._emit_recovery_triggered(snapshot, "scale", "OVERLOADED_AGENT")
            if self._can_scale(snapshot, now):
                self._publish_scale(snapshot, "Overloaded agent requires more capacity", "OVERLOADED_AGENT", self._overload_signal(snapshot))
            else:
                self._maybe_publish_fallback(snapshot, "OVERLOADED_AGENT")
            return

        if snapshot.consumer_lag >= self.LAG_SCALE_THRESHOLD or trigger == "lag":
            self._emit_recovery_triggered(snapshot, "scale", "LAG_DETECTED")
            if self._can_scale(snapshot, now):
                self._publish_scale(snapshot, "High lag detected by monitoring", "LAG_DETECTED", float(snapshot.consumer_lag))
            return

        if snapshot.status == AgentStatus.DEGRADED.value and snapshot.last_degraded_at:
            degraded_age = (now - snapshot.last_degraded_at).total_seconds()
            if degraded_age >= self.DEGRADED_RESTART_DELAY_SECONDS:
                self._emit_recovery_triggered(snapshot, "restart", "DEGRADED_PERSISTED")
                if self._can_restart(snapshot, now):
                    self._publish_restart(snapshot, "Degraded condition persisted beyond grace period", "DEGRADED_PERSISTED")
                else:
                    self._maybe_publish_fallback(snapshot, "DEGRADED_PERSISTED")

    def _overload_signal(self, state: AgentRecoveryState) -> float:
        """Choose the most relevant overload signal value for scale events."""
        return float(
            max(
                state.consumer_lag,
                int(state.cpu_percent or 0),
                int(state.memory_percent or 0),
                int(state.latency_ms or 0),
            )
        )

    def _can_restart(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_restart_requested is None:
            return True
        return (now - state.last_restart_requested).total_seconds() >= self.RESTART_COOLDOWN_SECONDS

    def _can_spawn(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_spawn_requested is None:
            return True
        return (now - state.last_spawn_requested).total_seconds() >= self.SPAWN_COOLDOWN_SECONDS

    def _can_scale(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_scale_requested is None:
            return True
        return (now - state.last_scale_requested).total_seconds() >= self.SCALE_COOLDOWN_SECONDS

    def _can_emit_recovery(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_recovery_triggered is None:
            return True
        return (now - state.last_recovery_triggered).total_seconds() >= self.RECOVERY_EVENT_COOLDOWN_SECONDS

    def _can_request_placement(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_placement_requested is None:
            return True
        return (now - state.last_placement_requested).total_seconds() >= self.PLACEMENT_REQUEST_COOLDOWN_SECONDS

    def _can_select_fallback(self, state: AgentRecoveryState, now: datetime) -> bool:
        if state.last_fallback_selected is None:
            return True
        return (now - state.last_fallback_selected).total_seconds() >= self.FALLBACK_COOLDOWN_SECONDS

    def _emit_recovery_triggered(self, state: AgentRecoveryState, action: str, decision_rule: str) -> None:
        """Publish recovery.triggered event."""
        now = datetime.utcnow()
        if not self._can_emit_recovery(state, now):
            return

        payload = {
            "agent_id": state.agent_id,
            "agent_type": state.agent_type,
            "agent_name": state.agent_name,
            "instance_id": state.instance_id,
            "triggered_at": now.isoformat(),
            "recommended_action": action,
            "decision_rule": decision_rule,
            "status": state.status,
            "consumer_lag": state.consumer_lag,
            "error_count": state.error_count,
            "latency_ms": state.latency_ms,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.RECOVERY_TRIGGERED.value,
            entity_id=state.agent_name,
            payload=payload,
            correlation_id=self.create_correlation_id(),
        )

        with self._state_lock:
            current = self._states.get(state.agent_id)
            if current is not None:
                current.last_recovery_triggered = now

    def _publish_restart(self, state: AgentRecoveryState, reason: str, decision_rule: str) -> None:
        """Publish agent.restart.requested."""
        event_data = create_restart_request_event(
            event_source=self.agent_name,
            agent_id=state.agent_id,
            agent_name=state.agent_name,
            instance_id=state.instance_id or f"instance_{state.agent_name}",
            reason=reason,
            agent_type=state.agent_type,
            graceful=True,
            timeout_seconds=30,
            restart_count=0,
            max_restarts=self.MAX_RESTARTS_BEFORE_SPAWN,
            decision_rule=decision_rule,
            decision_score=0.95,
            correlation_id=self.create_correlation_id(),
        )
        self.kafka_client.publish(self.SYSTEM_TOPIC, event_data)

        with self._state_lock:
            current = self._states.get(state.agent_id)
            if current is not None:
                current.last_restart_requested = datetime.utcnow()

    def _publish_scale(self, state: AgentRecoveryState, reason: str, decision_rule: str, trigger_value: float) -> None:
        """Publish agent.scale.requested."""
        current_replicas = state.replica_count or 1
        max_replicas = state.max_replicas or self.DEFAULT_MAX_REPLICAS
        if current_replicas >= max_replicas:
            self._publish_spawn_and_placement(state, "Scale limit reached; requesting replacement capacity", "SCALE_LIMIT_REACHED")
            return

        desired_replicas = min(current_replicas + self.SCALE_UP_STEP, max_replicas)
        event_data = create_scale_request_event(
            event_source=self.agent_name,
            agent_name=state.agent_name,
            current_replicas=current_replicas,
            desired_replicas=desired_replicas,
            max_replicas=max_replicas,
            reason=reason,
            agent_type=state.agent_type,
            trigger_metric="consumer_lag" if state.consumer_lag else "overload_signal",
            trigger_value=trigger_value,
            decision_rule=decision_rule,
            decision_score=0.9,
            correlation_id=self.create_correlation_id(),
        )
        self.kafka_client.publish(self.SYSTEM_TOPIC, event_data)

        with self._state_lock:
            current = self._states.get(state.agent_id)
            if current is not None:
                current.last_scale_requested = datetime.utcnow()

    def _publish_spawn_and_placement(self, state: AgentRecoveryState, reason: str, decision_rule: str) -> None:
        """Publish agent.spawn.requested and placement.requested when capacity recovery is needed."""
        now = datetime.utcnow()
        if not self._can_spawn(state, now):
            return

        spawn_event = create_spawn_request_event(
            event_source=self.agent_name,
            agent_name=state.agent_name,
            reason=reason,
            agent_type=state.agent_type,
            instance_id=None,
            host=None,
            config=None,
            priority="high",
            source_agent_id=state.agent_id,
            source_instance_id=state.instance_id,
            replica_count=state.replica_count,
            max_replicas=state.max_replicas,
            decision_rule=decision_rule,
            decision_score=0.92,
            correlation_id=self.create_correlation_id(),
        )
        self.kafka_client.publish(self.SYSTEM_TOPIC, spawn_event)

        if self._can_request_placement(state, now):
            placement_payload = {
                "agent_type": state.agent_type,
                "agent_name": state.agent_name,
                "instance_id": None,
                "requirements": {
                    "reason": reason,
                    "source_agent_id": state.agent_id,
                },
                "preferred_hosts": None,
                "excluded_hosts": [state.host] if state.host else None,
                "requester": self.agent_name,
                "priority": "high",
                "decision_rule": decision_rule,
                "decision_score": 0.9,
            }
            self.publish_event(
                topic=self.SYSTEM_TOPIC,
                event_type=SystemEventType.PLACEMENT_REQUESTED.value,
                entity_id=state.agent_name,
                payload=placement_payload,
                correlation_id=self.create_correlation_id(),
            )

        with self._state_lock:
            current = self._states.get(state.agent_id)
            if current is not None:
                current.last_spawn_requested = now
                current.last_placement_requested = now

    def _maybe_publish_fallback(self, state: AgentRecoveryState, decision_rule: str) -> None:
        """Publish fallback.agent.selected when a fallback is configured."""
        now = datetime.utcnow()
        if not state.candidate_fallbacks or not self._can_select_fallback(state, now):
            return

        fallback_agent = state.candidate_fallbacks[0]
        payload = {
            "source_agent_id": state.agent_id,
            "source_agent_name": state.agent_name,
            "fallback_agent_name": fallback_agent,
            "selected_at": now.isoformat(),
            "decision_rule": decision_rule,
            "decision_score": 0.85,
            "reason": "Fallback agent selected while primary recovery is pending",
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.FALLBACK_AGENT_SELECTED.value,
            entity_id=state.agent_name,
            payload=payload,
            correlation_id=self.create_correlation_id(),
        )

        with self._state_lock:
            current = self._states.get(state.agent_id)
            if current is not None:
                current.last_fallback_selected = now
