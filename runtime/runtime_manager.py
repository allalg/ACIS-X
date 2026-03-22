"""
RuntimeManager - Simulated runtime orchestrator for ACIS-X.

Consumes orchestration commands from acis.system and publishes completion
events for spawn, restart, scale, and placement operations. This manager does
not interact with real containers or agents directly.
"""

import logging
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import AgentStatus, Event, SystemEventType

logger = logging.getLogger(__name__)


@dataclass
class SimulatedInstance:
    """Represents a simulated runtime instance."""

    agent_id: str
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: str = ""
    host: str = "runtime-host-1.acis.local"
    port: Optional[int] = None
    version: str = "1.0.0"
    group_id: Optional[str] = None
    status: str = AgentStatus.HEALTHY.value
    restart_count: int = 0
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None
    capabilities: List[str] = field(default_factory=list)
    subscribed_topics: List[str] = field(default_factory=list)
    produced_topics: List[str] = field(default_factory=list)


class RuntimeManager(BaseAgent):
    """Simulated runtime manager that fulfills orchestration commands via events."""

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        simulated_hosts: Optional[List[str]] = None,
    ):
        super().__init__(
            agent_name="RuntimeManager",
            agent_version=agent_version,
            group_id="runtime-manager-group",
            subscribed_topics=["acis.system"],
            capabilities=[
                "runtime_simulation",
                "spawn_execution",
                "restart_execution",
                "scale_execution",
                "placement_execution",
            ],
            kafka_client=kafka_client,
            agent_type="RuntimeManager",
            instance_id=instance_id,
            host=host,
        )

        self._instances: Dict[str, SimulatedInstance] = {}
        self._instances_lock = threading.Lock()
        self._simulated_hosts = simulated_hosts or [
            "runtime-host-1.acis.local",
            "runtime-host-2.acis.local",
            "runtime-host-3.acis.local",
        ]
        self._next_host_index = 0
        self._next_port = 9100
        self._replica_counters: Dict[str, int] = {}

    def subscribe(self) -> List[str]:
        """Runtime manager consumes orchestration commands from the system topic."""
        return ["acis.system"]

    def process_event(self, event: Event) -> None:
        """Handle simulated runtime commands."""
        event_type = event.event_type

        if event_type == SystemEventType.AGENT_SPAWN_REQUESTED.value:
            self._handle_spawn_requested(event)
        elif event_type == SystemEventType.AGENT_RESTART_REQUESTED.value:
            self._handle_restart_requested(event)
        elif event_type == SystemEventType.AGENT_SCALE_REQUESTED.value:
            self._handle_scale_requested(event)
        elif event_type == SystemEventType.PLACEMENT_REQUESTED.value:
            self._handle_placement_requested(event)

    def _handle_spawn_requested(self, event: Event) -> None:
        payload = event.payload
        agent_name = payload["agent_name"]
        agent_type = payload.get("agent_type")
        instance_id = payload.get("instance_id") or self._generate_instance_id(agent_name)
        correlation_id = event.correlation_id
        replica_index = self._next_replica_index(agent_name)
        replica_count = payload.get("replica_count")
        max_replicas = payload.get("max_replicas")

        if replica_count is None:
            replica_count = replica_index + 1

        instance = SimulatedInstance(
            agent_id=f"agent_{agent_name.lower()}_{instance_id}",
            agent_name=agent_name,
            agent_type=agent_type,
            instance_id=instance_id,
            host=self._assign_host(),
            port=self._allocate_port(),
            version="1.0.0",
            group_id=f"{agent_name.lower()}-group",
            status=AgentStatus.HEALTHY.value,
            restart_count=0,
            replica_index=replica_index,
            replica_count=replica_count,
            max_replicas=max_replicas,
        )

        with self._instances_lock:
            self._instances[instance.agent_id] = instance

        self._publish_agent_spawned(instance, correlation_id)

    def _handle_restart_requested(self, event: Event) -> None:
        payload = event.payload
        agent_id = payload["agent_id"]
        correlation_id = event.correlation_id

        with self._instances_lock:
            instance = self._instances.get(agent_id)
            if instance is None:
                instance = SimulatedInstance(
                    agent_id=agent_id,
                    agent_name=payload["agent_name"],
                    agent_type=payload.get("agent_type"),
                    instance_id=payload.get("instance_id", self._generate_instance_id(payload["agent_name"])),
                    host=self._assign_host(),
                    port=self._allocate_port(),
                    version="1.0.0",
                    group_id=f"{payload['agent_name'].lower()}-group",
                    status=AgentStatus.HEALTHY.value,
                )
                self._instances[agent_id] = instance

            instance.restart_count += 1
            instance.status = AgentStatus.HEALTHY.value

        self._publish_restart_completed(instance, correlation_id)

    def _handle_scale_requested(self, event: Event) -> None:
        payload = event.payload
        agent_name = payload["agent_name"]
        agent_type = payload.get("agent_type")
        previous_replicas = int(payload.get("current_replicas", 0))
        desired_replicas = int(payload.get("desired_replicas", previous_replicas))
        max_replicas = int(payload.get("max_replicas", desired_replicas or 1))
        scale_direction = payload.get("scale_direction", "up")
        correlation_id = event.correlation_id

        instances_snapshot = self._ensure_replicas(
            agent_name=agent_name,
            agent_type=agent_type,
            desired_replicas=desired_replicas,
            max_replicas=max_replicas,
        )

        self._publish_scale_completed(
            agent_name=agent_name,
            agent_type=agent_type,
            previous_replicas=previous_replicas,
            current_replicas=len(instances_snapshot),
            max_replicas=max_replicas,
            scale_direction=scale_direction,
            instances=instances_snapshot,
            correlation_id=correlation_id,
        )

    def _handle_placement_requested(self, event: Event) -> None:
        payload = event.payload
        agent_name = payload["agent_name"]
        agent_type = payload.get("agent_type")
        correlation_id = event.correlation_id
        preferred_hosts = payload.get("preferred_hosts") or []
        excluded_hosts = set(payload.get("excluded_hosts") or [])

        chosen_host = None
        for host in preferred_hosts:
            if host not in excluded_hosts:
                chosen_host = host
                break

        if chosen_host is None:
            for _ in range(len(self._simulated_hosts)):
                candidate = self._assign_host()
                if candidate not in excluded_hosts:
                    chosen_host = candidate
                    break

        if chosen_host is None:
            chosen_host = self.host or "runtime-host-unassigned"

        instance_id = payload.get("instance_id") or self._generate_instance_id(agent_name)
        placement_payload = {
            "agent_type": agent_type,
            "agent_name": agent_name,
            "instance_id": instance_id,
            "host": chosen_host,
            "port": None,
            "placement_decision": f"Simulated placement on {chosen_host}",
            "alternatives_considered": self._simulated_hosts,
            "placement_duration_ms": 0,
            "status": "completed",
            "error_message": None,
            "decision_rule": payload.get("decision_rule", "SIMULATED_ROUND_ROBIN"),
            "decision_score": payload.get("decision_score", 1.0),
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.PLACEMENT_COMPLETED.value,
            entity_id=agent_name,
            payload=placement_payload,
            correlation_id=correlation_id,
        )

    def _ensure_replicas(
        self,
        agent_name: str,
        agent_type: Optional[str],
        desired_replicas: int,
        max_replicas: int,
    ) -> List[Dict[str, Any]]:
        """Adjust simulated instance set to the desired replica count."""
        desired_replicas = max(0, min(desired_replicas, max_replicas))

        with self._instances_lock:
            current = [
                instance for instance in self._instances.values()
                if instance.agent_name == agent_name
            ]

            while len(current) < desired_replicas:
                instance_id = self._generate_instance_id(agent_name)
                replica_index = self._next_replica_index(agent_name)
                instance = SimulatedInstance(
                    agent_id=f"agent_{agent_name.lower()}_{instance_id}",
                    agent_name=agent_name,
                    agent_type=agent_type,
                    instance_id=instance_id,
                    host=self._assign_host(),
                    port=self._allocate_port(),
                    version="1.0.0",
                    group_id=f"{agent_name.lower()}-group",
                    status=AgentStatus.HEALTHY.value,
                    replica_index=replica_index,
                    replica_count=desired_replicas,
                    max_replicas=max_replicas,
                )
                self._instances[instance.agent_id] = instance
                current.append(instance)

            while len(current) > desired_replicas:
                removed = current.pop()
                self._instances.pop(removed.agent_id, None)

            for index, instance in enumerate(current):
                instance.replica_index = index
                instance.replica_count = desired_replicas
                instance.max_replicas = max_replicas

            return [self._instance_to_scale_dict(instance) for instance in current]

    def _publish_agent_spawned(
        self,
        instance: SimulatedInstance,
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.spawned."""
        payload = {
            "agent_id": instance.agent_id,
            "agent_type": instance.agent_type,
            "agent_name": instance.agent_name,
            "instance_id": instance.instance_id,
            "host": instance.host,
            "port": instance.port,
            "version": instance.version,
            "group_id": instance.group_id,
            "capabilities": instance.capabilities,
            "subscribed_topics": instance.subscribed_topics,
            "produced_topics": instance.produced_topics,
            "spawned_at": datetime.utcnow().isoformat(),
            "spawn_duration_ms": 0,
            "replica_index": instance.replica_index,
            "replica_count": instance.replica_count,
            "max_replicas": instance.max_replicas,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_SPAWNED.value,
            entity_id=instance.agent_name,
            payload=payload,
            correlation_id=correlation_id,
        )

    def _publish_restart_completed(
        self,
        instance: SimulatedInstance,
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.restart.completed."""
        payload = {
            "agent_id": instance.agent_id,
            "agent_type": instance.agent_type,
            "agent_name": instance.agent_name,
            "instance_id": instance.instance_id,
            "host": instance.host,
            "version": instance.version,
            "restarted_at": datetime.utcnow().isoformat(),
            "restart_duration_ms": 0,
            "restart_count": instance.restart_count,
            "previous_error": None,
            "status": AgentStatus.HEALTHY.value,
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.AGENT_RESTART_COMPLETED.value,
            entity_id=instance.agent_name,
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
        instances: List[Dict[str, Any]],
        correlation_id: Optional[str],
    ) -> None:
        """Publish agent.scale.completed."""
        payload = {
            "agent_type": agent_type,
            "agent_name": agent_name,
            "previous_replicas": previous_replicas,
            "current_replicas": current_replicas,
            "max_replicas": max_replicas,
            "scale_direction": scale_direction,
            "scaled_at": datetime.utcnow().isoformat(),
            "scale_duration_ms": 0,
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

    def _instance_to_scale_dict(self, instance: SimulatedInstance) -> Dict[str, Any]:
        """Convert a simulated instance into scale-completed payload shape."""
        return {
            "agent_id": instance.agent_id,
            "agent_name": instance.agent_name,
            "agent_type": instance.agent_type,
            "instance_id": instance.instance_id,
            "host": instance.host,
            "port": instance.port,
            "status": instance.status,
            "replica_index": instance.replica_index,
        }

    def _assign_host(self) -> str:
        """Return next simulated host using round-robin assignment."""
        host = self._simulated_hosts[self._next_host_index]
        self._next_host_index = (self._next_host_index + 1) % len(self._simulated_hosts)
        return host

    def _allocate_port(self) -> int:
        """Allocate a simulated port."""
        port = self._next_port
        self._next_port += 1
        return port

    def _generate_instance_id(self, agent_name: str) -> str:
        """Generate simulated instance ID."""
        return f"instance_{agent_name.lower()}_{uuid.uuid4().hex[:8]}"

    def _next_replica_index(self, agent_name: str) -> int:
        """Return next replica index for an agent type."""
        index = self._replica_counters.get(agent_name, 0)
        self._replica_counters[agent_name] = index + 1
        return index
