"""
PlacementEngine - Event-driven placement decider for ACIS-X.

Consumes placement requests from acis.system and publishes placement.completed
events with simulated host, replica index, and group assignment decisions.
"""

import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from agents.base.base_agent import BaseAgent
from registry.registry_service import RegistryService
from schemas.event_schema import Event, SystemEventType

logger = logging.getLogger(__name__)


class PlacementEngine(BaseAgent):
    """Simulated placement engine that decides placement but does not spawn agents."""
    IGNORE_STALE_EVENTS_ON_STARTUP = True

    def __init__(
        self,
        kafka_client: Any,
        agent_version: str = "1.0.0",
        instance_id: Optional[str] = None,
        host: Optional[str] = None,
        simulated_hosts: Optional[List[str]] = None,
        registry: Optional[RegistryService] = None,
    ):
        super().__init__(
            agent_name="PlacementEngine",
            agent_version=agent_version,
            group_id="placement-engine-group",
            subscribed_topics=["acis.system"],
            capabilities=[
                "placement_decisioning",
                "host_assignment",
                "replica_assignment",
            ],
            kafka_client=kafka_client,
            agent_type="PlacementEngine",
            instance_id=instance_id,
            host=host,
        )

        self._simulated_hosts = simulated_hosts or [
            "placement-host-1.acis.local",
            "placement-host-2.acis.local",
            "placement-host-3.acis.local",
        ]
        self._next_host_index = 0
        self._replica_counters: Dict[str, int] = {}
        self._lock = threading.Lock()
        self.registry = registry

    def subscribe(self) -> List[str]:
        """Consume placement requests from the system topic."""
        return ["acis.system"]

    def process_event(self, event: Event) -> None:
        """Handle placement.requested events only."""
        if self._start_time and event.event_time < self._start_time:
            logger.debug("[PlacementEngine] Ignoring stale event %s from %s", event.event_type, event.event_time)
            return
        if event.event_type == SystemEventType.PLACEMENT_REQUESTED.value:
            self._handle_placement_requested(event)

    def _handle_placement_requested(self, event: Event) -> None:
        """Choose host, replica index, and group assignment for a placement request."""
        payload = event.payload
        agent_name = payload.get("agent_name")
        agent_type = payload.get("agent_type")
        if not agent_name:
            logger.warning("[PlacementEngine] placement.requested missing agent_name: %s", payload)
            return
        instance_id = payload.get("instance_id") or f"instance_{agent_name.lower()}_{datetime.utcnow().strftime('%H%M%S')}"
        preferred_hosts = payload.get("preferred_hosts")
        excluded_hosts = payload.get("excluded_hosts")
        decision_rule = payload.get("decision_rule", "SIMULATED_ROUND_ROBIN")
        decision_score = payload.get("decision_score", 1.0)

        # Choose host using topology-aware logic
        host = self._choose_host(
            agent_name,
            preferred_hosts,
            excluded_hosts,
        )

        # Fallback to least loaded host if no host chosen
        if host is None and self.registry:
            host = self.registry.get_least_loaded_host()

        # Final fallback to old round-robin logic
        if host is None:
            host = self._select_host(preferred_hosts or [], set(excluded_hosts or []))

        replica_index = payload.get("replica_index")
        if replica_index is None and payload.get("replica_count") is not None:
            replica_index = max(int(payload["replica_count"]) - 1, 0)
        if replica_index is None:
            replica_index = self._next_replica_index(agent_name)
        group_id = self._derive_group_id(agent_name, payload)

        placement_payload = {
            "agent_id": payload.get("agent_id"),
            "agent_type": agent_type,
            "agent_name": agent_name,
            "instance_id": instance_id,
            "host": host,
            "port": None,
            "operation": payload.get("_operation", "spawn"),
            "placement_decision": (
                f"Placed on {host} with replica_index={replica_index} "
                f"and group_id={group_id}"
            ),
            "alternatives_considered": self._simulated_hosts,
            "placement_duration_ms": 0,
            "status": "completed",
            "error_message": None,
            "decision_rule": decision_rule,
            "decision_score": decision_score,
            "replica_index": replica_index,
            "replica_count": payload.get("replica_count"),
            "max_replicas": payload.get("max_replicas"),
            "group_id": group_id,
            "_operation": payload.get("_operation", "spawn"),
        }

        self.publish_event(
            topic=self.SYSTEM_TOPIC,
            event_type=SystemEventType.PLACEMENT_COMPLETED.value,
            entity_id=agent_name,
            payload=placement_payload,
            correlation_id=event.correlation_id,
        )

    def _get_hosts(self) -> List[str]:
        """Get available hosts from registry."""
        if not self.registry:
            return []

        return self.registry.get_hosts()

    def _get_host_load(self, host: str) -> int:
        """Get host load (number of agents on host)."""
        if not self.registry:
            return 0

        return self.registry.get_host_load(host)

    def _choose_host(
        self,
        agent_name: str,
        preferred_hosts: Optional[List[str]],
        excluded_hosts: Optional[List[str]],
    ) -> Optional[str]:
        """Choose best host based on topology and preferences."""
        hosts = self._get_hosts()

        if not hosts:
            return None

        # Remove excluded hosts
        if excluded_hosts:
            hosts = [h for h in hosts if h not in excluded_hosts]

        # If preferred hosts exist, use only them
        if preferred_hosts:
            hosts = [h for h in hosts if h in preferred_hosts]

        if not hosts:
            return None

        best_host = None
        best_load = None

        for h in hosts:
            load = self._get_host_load(h)

            if best_load is None or load < best_load:
                best_host = h
                best_load = load

        return best_host

    def _select_host(self, preferred_hosts: List[str], excluded_hosts: set[str]) -> str:
        """Pick a simulated host honoring preferences and exclusions."""
        with self._lock:
            for host in preferred_hosts:
                if host not in excluded_hosts:
                    return host

            for _ in range(len(self._simulated_hosts)):
                host = self._simulated_hosts[self._next_host_index]
                self._next_host_index = (self._next_host_index + 1) % len(self._simulated_hosts)
                if host not in excluded_hosts:
                    return host

        return self.host or "placement-host-unassigned"

    def _next_replica_index(self, agent_name: str) -> int:
        """Return the next simulated replica index for an agent type."""
        with self._lock:
            index = self._replica_counters.get(agent_name, 0)
            self._replica_counters[agent_name] = index + 1
            return index

    def _derive_group_id(self, agent_name: str, payload: Dict[str, Any]) -> str:
        """Return a group id for the placement decision."""
        group_id = payload.get("group_id")
        if group_id:
            return group_id
        agent_token = agent_name.lower().replace(" ", "-")
        return f"{agent_token}-group"
