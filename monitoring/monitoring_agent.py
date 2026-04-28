"""
MonitoringAgent - Event-driven health and metrics monitor for ACIS-X.

Consumes agent health, system, and registry events to maintain an in-memory
view of agent health across the platform. Publishes derived monitoring events
without directly querying the registry or triggering self-healing actions.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event, RegistryEventType, SystemEventType, AgentStatus

logger = logging.getLogger(__name__)


@dataclass
class AgentObservation:
    """Event-derived state for a monitored agent."""

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
    last_metrics_at: Optional[datetime] = None
    last_timeout_at: Optional[datetime] = None

    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    consumer_lag: int = 0
    events_per_second: float = 0.0
    error_count: int = 0
    latency_ms: Optional[float] = None
    events_processed: int = 0
    queue_depth: int = 0

    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None

    error_timestamps: List[datetime] = field(default_factory=list)
    last_degraded_emitted_at: Optional[datetime] = None
    last_critical_emitted_at: Optional[datetime] = None
    last_overloaded_emitted_at: Optional[datetime] = None
    last_lag_emitted_at: Optional[datetime] = None
    last_throughput_emitted_at: Optional[datetime] = None
    last_missing_heartbeat_emitted_at: Optional[datetime] = None
    last_error_emitted_at: Optional[datetime] = None

    def update_identity(self, payload: Dict[str, Any]) -> None:
        """Refresh identifying fields from an event payload."""
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

    def prune_error_window(self, now: datetime, window_seconds: int) -> None:
        """Keep only recent error timestamps for rate calculations."""
        cutoff = now.timestamp() - window_seconds
        self.error_timestamps = [ts for ts in self.error_timestamps if ts.timestamp() >= cutoff]

    def error_rate_per_minute(self, now: datetime, window_seconds: int) -> float:
        """Compute error rate over the rolling window."""
        self.prune_error_window(now, window_seconds)
        if window_seconds <= 0:
            return 0.0
        return round(len(self.error_timestamps) * 60.0 / window_seconds, 2)


class MonitoringAgent(BaseAgent):
    """
    Event-driven monitoring agent for ACIS-X.

    Responsibilities:
    - Track per-agent health and metrics from Kafka events
    - Detect degraded and critical conditions
    - Publish health, lag, overload, throughput, and aggregate metrics events
    """

    HEARTBEAT_TIMEOUT_SECONDS = 60
    HEARTBEAT_CRITICAL_SECONDS = 120
    EVALUATION_INTERVAL_SECONDS = 10

    LAG_THRESHOLD = 50            # FIX 3: was 5000 — matches ACIS_LAG_SCALE_THRESHOLD
    CRITICAL_LAG_THRESHOLD = 200  # FIX 3: was 10000 — matches ACIS_CRITICAL_LAG_THRESHOLD
    ERROR_RATE_THRESHOLD = 5.0
    CRITICAL_ERROR_RATE_THRESHOLD = 10.0
    OVERLOAD_CPU_THRESHOLD = 90.0
    OVERLOAD_MEMORY_THRESHOLD = 90.0
    OVERLOAD_LATENCY_THRESHOLD_MS = 5000.0
    THROUGHPUT_MIN_EVENTS_PER_SECOND = 0.1
    ERROR_WINDOW_SECONDS = 300

    HEALTH_EVENT_COOLDOWN_SECONDS = 60
    OVERLOAD_EVENT_COOLDOWN_SECONDS = 60
    LAG_EVENT_COOLDOWN_SECONDS = 60
    THROUGHPUT_EVENT_COOLDOWN_SECONDS = 60
    METRICS_PUBLISH_COOLDOWN_SECONDS = 30
    ERROR_EVENT_COOLDOWN_SECONDS = 60
    IGNORE_STALE_EVENTS_ON_STARTUP = True

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
    ):
        super().__init__(
            agent_name="MonitoringAgent",
            agent_version=agent_version,
            group_id="monitoring-group",
            subscribed_topics=["acis.agent.health", "acis.system", "acis.registry"],
            capabilities=[
                "health_monitoring",
                "metrics_aggregation",
                "lag_detection",
                "throughput_tracking",
                "alerting",
            ],
            kafka_client=kafka_client,
            agent_type="MonitoringAgent",
            instance_id=instance_id,
            host=host,
        )

        self._agents: Dict[str, AgentObservation] = {}
        self._agents_lock = threading.Lock()
        self._evaluation_thread: Optional[threading.Thread] = None
        self._last_metrics_event_at: Optional[datetime] = None

        # Deferred evaluation queue: event handlers add agent_ids here instead
        # of calling _evaluate_agent() inline, which prevents the handlers from
        # blocking on potentially slow health-publish logic while holding
        # _agents_lock.  The evaluation loop drains the batch on each tick.
        self._agents_needing_eval: set = set()
        self._eval_lock = threading.Lock()

    def subscribe(self) -> List[str]:
        """Return topics to subscribe to."""
        return ["acis.agent.health", "acis.system", "acis.registry"]

    def start(self) -> None:
        """Start agent and background evaluation loop."""
        super().start()

        self._evaluation_thread = threading.Thread(
            target=self._evaluation_loop,
            daemon=True,
            name=f"{self.agent_name}-evaluation",
        )
        self._evaluation_thread.start()

    def stop(self) -> None:
        """Stop agent and background evaluation loop."""
        super().stop()

        if self._evaluation_thread and self._evaluation_thread.is_alive():
            self._evaluation_thread.join(timeout=5)

    def process_event(self, event: Event) -> None:
        """Process events from Kafka and update monitoring state."""
        if event.event_source == self.agent_name:
            return
        if self._start_time and event.event_time < self._start_time:
            logger.debug("Ignoring stale event %s from %s", event.event_type, event.event_time)
            return

        event_type = event.event_type

        if event_type == SystemEventType.AGENT_HEARTBEAT.value:
            self._handle_heartbeat(event)
        elif event_type in (SystemEventType.AGENT_METRICS_UPDATED.value, SystemEventType.METRICS_UPDATED.value):
            self._handle_metrics_updated(event)
        elif event_type == SystemEventType.AGENT_OVERLOADED.value:
            self._handle_agent_overloaded(event)
        elif event_type == SystemEventType.AGENT_ERROR.value:
            self._handle_agent_error(event)
        elif event_type == SystemEventType.AGENT_TIMEOUT.value:
            self._handle_agent_timeout(event)
        elif event_type == RegistryEventType.AGENT_REGISTERED.value:
            self._handle_agent_registered(event)
        elif event_type == RegistryEventType.AGENT_DEREGISTERED.value:
            self._handle_agent_deregistered(event)
        elif event_type == RegistryEventType.AGENT_UPDATED.value:
            self._handle_agent_registered(event)
        else:
            return

        self._publish_system_metrics_if_due()

    def _evaluation_loop(self) -> None:
        """Periodically evaluate event-derived state for timeout conditions."""
        logger.info("Monitoring evaluation loop started")

        while self._running:
            try:
                # Atomically drain the pending evaluation set so handlers can
                # keep adding to it while we evaluate this batch outside the lock.
                with self._eval_lock:
                    batch = set(self._agents_needing_eval)
                    self._agents_needing_eval.clear()

                for agent_id in batch:
                    self._evaluate_agent(agent_id, "evaluation_loop")

                # Also periodically scan ALL agents (catches timeout conditions
                # for agents that have gone silent and sent no events).
                self._evaluate_all_agents()
                self._publish_system_metrics_if_due()
            except Exception as exc:
                logger.error(f"Monitoring evaluation loop error: {exc}")

            self._shutdown_event.wait(timeout=self.EVALUATION_INTERVAL_SECONDS)

        logger.info("Monitoring evaluation loop stopped")

    def _handle_heartbeat(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.last_event_at = event.event_time
            agent.last_heartbeat = event.event_time
            agent.status = payload.get("status", agent.status)
            agent.update_identity(payload)

            metrics = payload.get("metrics") or {}
            self._update_metrics_from_payload(agent, metrics)

            details = payload.get("details") or {}
            if details:
                agent.consumer_group = details.get("group_id", agent.consumer_group)

        with self._eval_lock:
            self._agents_needing_eval.add(agent.agent_id)

    def _handle_metrics_updated(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.last_event_at = event.event_time
            agent.last_metrics_at = event.event_time
            agent.update_identity(payload)
            self._update_metrics_from_payload(agent, payload)

        with self._eval_lock:
            self._agents_needing_eval.add(agent.agent_id)
        self._publish_throughput_update_if_needed(agent.agent_id)

    def _handle_agent_overloaded(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.last_event_at = event.event_time
            agent.status = AgentStatus.OVERLOADED.value
            agent.update_identity(payload)
            self._update_metrics_from_payload(agent, payload)

        self._publish_overloaded_if_needed(agent.agent_id, reason="overload_event")
        self._publish_health_event_if_needed(agent.agent_id, AgentStatus.DEGRADED.value, "overloaded_agent")

    def _handle_agent_error(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            now = event.event_time
            agent.last_event_at = now
            agent.status = AgentStatus.ERROR.value
            agent.update_identity(payload)
            agent.error_count = max(agent.error_count, int(payload.get("error_count", agent.error_count + 1) or 0))
            agent.error_timestamps.append(now)
            agent.prune_error_window(now, self.ERROR_WINDOW_SECONDS)

        with self._eval_lock:
            self._agents_needing_eval.add(agent.agent_id)

    def _handle_agent_timeout(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.last_event_at = event.event_time
            agent.last_timeout_at = event.event_time
            agent.status = AgentStatus.TIMEOUT.value
            agent.update_identity(payload)

        self._publish_health_event_if_needed(agent.agent_id, AgentStatus.CRITICAL.value, "timeout_detected")

    def _handle_agent_registered(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.registered = True
            agent.last_event_at = event.event_time
            agent.update_identity(payload)

            topics = payload.get("topics")
            if isinstance(topics, dict):
                agent.consumer_group = payload.get("group_id", agent.consumer_group)

            payload_status = payload.get("status")
            if payload_status == "registered":
                agent.status = AgentStatus.HEALTHY.value
            elif payload_status:
                agent.status = payload_status

    def _handle_agent_deregistered(self, event: Event) -> None:
        payload = event.payload
        agent = self._get_or_create_agent(payload, event.entity_id)

        with self._agents_lock:
            agent.last_event_at = event.event_time
            agent.registered = False
            agent.status = AgentStatus.STOPPED.value

    def _get_or_create_agent(self, payload: Dict[str, Any], fallback_name: str) -> AgentObservation:
        """Return existing agent state or create a new one from event data."""
        agent_name = payload.get("agent_name", fallback_name)
        agent_id = payload.get("agent_id") or f"agent_{agent_name}"

        with self._agents_lock:
            if agent_id not in self._agents:
                self._agents[agent_id] = AgentObservation(
                    agent_id=agent_id,
                    agent_name=agent_name,
                )
            return self._agents[agent_id]

    def _update_metrics_from_payload(self, agent: AgentObservation, payload: Dict[str, Any]) -> None:
        """Copy supported metrics from an incoming payload."""
        if payload.get("cpu_percent") is not None:
            agent.cpu_percent = payload["cpu_percent"]
        if payload.get("memory_percent") is not None:
            agent.memory_percent = payload["memory_percent"]
        if payload.get("consumer_lag") is not None:
            agent.consumer_lag = int(payload["consumer_lag"] or 0)
        if payload.get("events_per_second") is not None:
            agent.events_per_second = float(payload["events_per_second"] or 0.0)
        if payload.get("error_count") is not None:
            agent.error_count = int(payload["error_count"] or 0)
        if payload.get("latency_ms") is not None:
            agent.latency_ms = payload["latency_ms"]
        if payload.get("events_processed") is not None:
            agent.events_processed = int(payload["events_processed"] or 0)
        if payload.get("queue_depth") is not None:
            agent.queue_depth = int(payload["queue_depth"] or 0)

    def _evaluate_all_agents(self) -> None:
        """Evaluate all tracked agents for monitoring conditions."""
        with self._agents_lock:
            agent_ids = list(self._agents.keys())

        for agent_id in agent_ids:
            self._evaluate_agent(agent_id, reason="evaluation_loop")

    def _evaluate_agent(self, agent_id: str, reason: str) -> None:
        """Evaluate one agent against health thresholds."""
        age_seconds = None
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None or agent.agent_name == self.agent_name:
                return

            now = datetime.utcnow()
            missing_heartbeat = False
            critical_heartbeat = False
            if agent.last_heartbeat is not None:
                age_seconds = (now - agent.last_heartbeat).total_seconds()
                missing_heartbeat = age_seconds > self.HEARTBEAT_TIMEOUT_SECONDS
                critical_heartbeat = age_seconds > self.HEARTBEAT_CRITICAL_SECONDS
            else:
                age_seconds = (
                    (now - agent.last_event_at).total_seconds()
                    if agent.registered and agent.last_event_at is not None
                    else None
                )
                if age_seconds is not None:
                    missing_heartbeat = age_seconds > self.HEARTBEAT_TIMEOUT_SECONDS
                    critical_heartbeat = age_seconds > self.HEARTBEAT_CRITICAL_SECONDS

            error_rate = agent.error_rate_per_minute(now, self.ERROR_WINDOW_SECONDS)
            lag = agent.consumer_lag
            error_detected = error_rate >= self.ERROR_RATE_THRESHOLD or agent.status == AgentStatus.ERROR.value
            overloaded = (
                (agent.cpu_percent or 0.0) >= self.OVERLOAD_CPU_THRESHOLD
                or (agent.memory_percent or 0.0) >= self.OVERLOAD_MEMORY_THRESHOLD
                or lag >= self.CRITICAL_LAG_THRESHOLD
                or (agent.latency_ms or 0.0) >= self.OVERLOAD_LATENCY_THRESHOLD_MS
                or agent.status == AgentStatus.OVERLOADED.value
            )
            timeout_detected = agent.status == AgentStatus.TIMEOUT.value or agent.last_timeout_at is not None

        if lag >= self.LAG_THRESHOLD:
            self._publish_lag_detected_if_needed(agent_id)

        if error_detected:
            self._publish_error_if_needed(agent_id, error_rate)

        if overloaded:
            self._publish_overloaded_if_needed(agent_id, reason=reason)

        if timeout_detected or critical_heartbeat or lag >= self.CRITICAL_LAG_THRESHOLD or error_rate >= self.CRITICAL_ERROR_RATE_THRESHOLD:
            cause = "timeout_detected" if timeout_detected else "critical_monitoring_condition"
            self._publish_health_event_if_needed(agent_id, AgentStatus.CRITICAL.value, cause)
            return

        if missing_heartbeat or error_rate >= self.ERROR_RATE_THRESHOLD or overloaded:
            cause = "missing_heartbeat" if missing_heartbeat else "degraded_monitoring_condition"
            self._publish_health_event_if_needed(agent_id, AgentStatus.DEGRADED.value, cause)
            return

        if age_seconds is not None and age_seconds <= self.HEARTBEAT_TIMEOUT_SECONDS:
            with self._agents_lock:
                agent = self._agents.get(agent_id)
                if agent is not None:
                    agent.status = AgentStatus.HEALTHY.value

    def _publish_health_event_if_needed(self, agent_id: str, status: str, cause: str) -> None:
        """Emit degraded or critical health events with cooldowns."""
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return

            now = datetime.utcnow()
            last_emitted = (
                agent.last_critical_emitted_at
                if status == AgentStatus.CRITICAL.value
                else agent.last_degraded_emitted_at
            )
            if last_emitted and (now - last_emitted).total_seconds() < self.HEALTH_EVENT_COOLDOWN_SECONDS:
                return

            payload = {
                "agent_id": agent.agent_id,
                "agent_type": agent.agent_type,
                "agent_name": agent.agent_name,
                "instance_id": agent.instance_id,
                "host": agent.host,
                "status": status,
                "error_code": cause.upper(),
                "error_message": cause.replace("_", " "),
                "timestamp": now.isoformat(),
                "metrics": {
                    "cpu_percent": agent.cpu_percent,
                    "memory_percent": agent.memory_percent,
                    "consumer_lag": agent.consumer_lag,
                    "error_count": agent.error_count,
                    "events_per_second": agent.events_per_second,
                    "latency_ms": agent.latency_ms,
                    "events_processed": agent.events_processed,
                    "queue_depth": agent.queue_depth,
                },
                "replica_count": agent.replica_count,
                "max_replicas": agent.max_replicas,
                "replica_index": agent.replica_index,
            }

            event_type = (
                SystemEventType.AGENT_HEALTH_CRITICAL.value
                if status == AgentStatus.CRITICAL.value
                else SystemEventType.AGENT_HEALTH_DEGRADED.value
            )

        self.publish_event(
            topic=self.HEALTH_TOPIC,
            event_type=event_type,
            entity_id=payload["agent_name"],
            payload=payload,
        )

        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return
            agent.status = status
            if status == AgentStatus.CRITICAL.value:
                agent.last_critical_emitted_at = now
            else:
                agent.last_degraded_emitted_at = now
                if cause == "missing_heartbeat":
                    agent.last_missing_heartbeat_emitted_at = now

    def _publish_lag_detected_if_needed(self, agent_id: str) -> None:
        """Emit lag.detected when consumer lag crosses threshold."""
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return

            now = datetime.utcnow()
            if agent.last_lag_emitted_at and (now - agent.last_lag_emitted_at).total_seconds() < self.LAG_EVENT_COOLDOWN_SECONDS:
                return

            payload = {
                "agent_id": agent.agent_id,
                "agent_type": agent.agent_type,
                "agent_name": agent.agent_name,
                "instance_id": agent.instance_id,
                "host": agent.host,
                "consumer_group": agent.consumer_group,
                "lag": agent.consumer_lag,
                "threshold": self.LAG_THRESHOLD,
                "detected_at": now.isoformat(),
                "replica_count": agent.replica_count,
                "max_replicas": agent.max_replicas,
                "replica_index": agent.replica_index,
            }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.LAG_DETECTED.value,
            entity_id=payload["agent_name"],
            payload=payload,
        )

        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is not None:
                agent.last_lag_emitted_at = now

    def _publish_overloaded_if_needed(self, agent_id: str, reason: str) -> None:
        """Emit agent.overloaded with cooldown."""
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return

            now = datetime.utcnow()
            if agent.last_overloaded_emitted_at and (now - agent.last_overloaded_emitted_at).total_seconds() < self.OVERLOAD_EVENT_COOLDOWN_SECONDS:
                return

            payload = {
                "agent_id": agent.agent_id,
                "agent_type": agent.agent_type,
                "agent_name": agent.agent_name,
                "instance_id": agent.instance_id,
                "host": agent.host,
                "status": AgentStatus.OVERLOADED.value,
                "timestamp": now.isoformat(),
                "cpu_percent": agent.cpu_percent,
                "memory_percent": agent.memory_percent,
                "queue_depth": agent.queue_depth,
                "consumer_lag": agent.consumer_lag,
                "error_count": agent.error_count,
                "consumer_group": agent.consumer_group,
                "cpu_threshold": self.OVERLOAD_CPU_THRESHOLD,
                "memory_threshold": self.OVERLOAD_MEMORY_THRESHOLD,
                "lag_threshold": self.CRITICAL_LAG_THRESHOLD,
                "recommended_action": "observe",
                "decision_rule": reason,
                "decision_score": 0.9,
                "replica_count": agent.replica_count,
                "max_replicas": agent.max_replicas,
                "replica_index": agent.replica_index,
            }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_OVERLOADED.value,
            entity_id=payload["agent_name"],
            payload=payload,
        )

        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is not None:
                agent.last_overloaded_emitted_at = now

    def _publish_error_if_needed(self, agent_id: str, error_rate: float) -> None:
        """Emit agent.error when an agent crosses the monitoring error threshold."""
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None:
                return

            now = datetime.utcnow()
            if agent.last_error_emitted_at and (now - agent.last_error_emitted_at).total_seconds() < self.ERROR_EVENT_COOLDOWN_SECONDS:
                return

            payload = {
                "agent_id": agent.agent_id,
                "agent_type": agent.agent_type,
                "agent_name": agent.agent_name,
                "instance_id": agent.instance_id,
                "host": agent.host,
                "status": AgentStatus.ERROR.value,
                "error_code": "HIGH_ERROR_RATE",
                "error_message": f"Error rate exceeded threshold ({error_rate}/min)",
                "error_count": agent.error_count,
                "error_rate_per_minute": error_rate,
                "consumer_lag": agent.consumer_lag,
                "latency_ms": agent.latency_ms,
                "timestamp": now.isoformat(),
                "replica_count": agent.replica_count,
                "max_replicas": agent.max_replicas,
                "replica_index": agent.replica_index,
            }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_ERROR.value,
            entity_id=payload["agent_name"],
            payload=payload,
        )

        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is not None:
                agent.last_error_emitted_at = now

    def _publish_throughput_update_if_needed(self, agent_id: str) -> None:
        """Emit throughput.updated when an agent reports meaningful throughput."""
        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is None or agent.events_per_second < self.THROUGHPUT_MIN_EVENTS_PER_SECOND:
                return

            now = datetime.utcnow()
            if agent.last_throughput_emitted_at and (now - agent.last_throughput_emitted_at).total_seconds() < self.THROUGHPUT_EVENT_COOLDOWN_SECONDS:
                return

            payload = {
                "agent_id": agent.agent_id,
                "agent_type": agent.agent_type,
                "agent_name": agent.agent_name,
                "instance_id": agent.instance_id,
                "host": agent.host,
                "events_processed": agent.events_processed,
                "events_per_second": agent.events_per_second,
                "latency_ms": agent.latency_ms,
                "updated_at": now.isoformat(),
                "consumer_group": agent.consumer_group,
            }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.THROUGHPUT_UPDATED.value,
            entity_id=payload["agent_name"],
            payload=payload,
        )

        with self._agents_lock:
            agent = self._agents.get(agent_id)
            if agent is not None:
                agent.last_throughput_emitted_at = now

    def _publish_system_metrics_if_due(self) -> None:
        """Emit aggregate metrics.updated snapshots with a small cooldown."""
        now = datetime.utcnow()
        if self._last_metrics_event_at and (now - self._last_metrics_event_at).total_seconds() < self.METRICS_PUBLISH_COOLDOWN_SECONDS:
            return

        with self._agents_lock:
            agents = [
                agent
                for agent in self._agents.values()
                if agent.agent_name != self.agent_name and agent.status != AgentStatus.STOPPED.value
            ]

        total_agents = len(agents)
        healthy_agents = sum(1 for agent in agents if agent.status == AgentStatus.HEALTHY.value)
        degraded_agents = sum(1 for agent in agents if agent.status == AgentStatus.DEGRADED.value)
        critical_agents = sum(1 for agent in agents if agent.status == AgentStatus.CRITICAL.value)
        error_agents = sum(1 for agent in agents if agent.status in (AgentStatus.ERROR.value, AgentStatus.TIMEOUT.value))

        total_events_processed = sum(agent.events_processed for agent in agents)
        total_consumer_lag = sum(agent.consumer_lag for agent in agents)
        max_consumer_lag = max((agent.consumer_lag for agent in agents), default=0)
        avg_latency = round(
            sum((agent.latency_ms or 0.0) for agent in agents) / max(1, len(agents)),
            2,
        )
        max_latency = max((agent.latency_ms or 0.0 for agent in agents), default=0.0)
        total_errors = sum(agent.error_count for agent in agents)
        errors_per_minute = round(
            sum(agent.error_rate_per_minute(now, self.ERROR_WINDOW_SECONDS) for agent in agents),
            2,
        )
        events_per_second = round(sum(agent.events_per_second for agent in agents), 2)
        total_replicas = sum(agent.replica_count or 1 for agent in agents)
        max_replicas = sum(agent.max_replicas or agent.replica_count or 1 for agent in agents)

        payload = {
            "timestamp": now.isoformat(),
            "total_agents": total_agents,
            "healthy_agents": healthy_agents,
            "degraded_agents": degraded_agents,
            "critical_agents": critical_agents,
            "error_agents": error_agents,
            "total_replicas": total_replicas,
            "max_replicas": max_replicas,
            "total_events_processed": total_events_processed,
            "events_per_second": events_per_second,
            "total_consumer_lag": total_consumer_lag,
            "max_consumer_lag": max_consumer_lag,
            "avg_processing_latency_ms": avg_latency,
            "max_processing_latency_ms": max_latency,
            "total_errors": total_errors,
            "errors_per_minute": errors_per_minute,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.METRICS_UPDATED.value,
            entity_id="acis_system",
            payload=payload,
        )

        self._last_metrics_event_at = now
