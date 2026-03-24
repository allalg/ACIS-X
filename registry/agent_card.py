"""
AgentCard - Metadata card for agent discovery in ACIS-X.

AgentCards are published by agents on startup and contain all metadata
necessary for dynamic discovery, capability-based routing, and topology tracking.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class AgentCard:
    """
    AgentCard contains complete metadata for an agent instance.

    Used for:
    - Dynamic service discovery
    - Capability-based routing
    - Topology visualization
    - Agent selection algorithms
    """

    # Identity
    agent_id: str
    agent_name: str
    agent_type: str
    version: str

    # Capabilities & Configuration
    capabilities: List[str] = field(default_factory=list)
    group_id: Optional[str] = None

    # Topics
    topics_consumed: List[str] = field(default_factory=list)
    topics_produced: List[str] = field(default_factory=list)

    # Network & Placement
    host: Optional[str] = None
    instance_id: Optional[str] = None

    # Replica Configuration
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None

    # Status
    status: Optional[str] = None

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    last_updated: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert AgentCard to dictionary for serialization.

        Returns:
            Dictionary with datetime objects converted to ISO format.
        """
        data = asdict(self)

        # Convert datetime to ISO format
        if self.last_updated:
            data["last_updated"] = self.last_updated.isoformat()

        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentCard":
        """
        Create AgentCard from dictionary.

        Args:
            data: Dictionary containing agent card fields

        Returns:
            AgentCard instance
        """
        # Handle datetime conversion
        if isinstance(data.get("last_updated"), str):
            data["last_updated"] = datetime.fromisoformat(data["last_updated"])
        elif data.get("last_updated") is None:
            data["last_updated"] = datetime.utcnow()

        # Filter out unknown fields
        known_fields = {
            "agent_id", "agent_name", "agent_type", "version",
            "capabilities", "group_id", "topics_consumed", "topics_produced",
            "host", "instance_id", "replica_index", "replica_count",
            "max_replicas", "status", "metadata", "last_updated"
        }

        filtered_data = {k: v for k, v in data.items() if k in known_fields}

        return cls(**filtered_data)

    def matches_capability(self, capability: str) -> bool:
        """Check if this agent has the specified capability."""
        return capability in self.capabilities

    def is_healthy_replica(self) -> bool:
        """Check if this is a healthy replica (has valid replica info)."""
        return (
            self.replica_count is not None and
            self.replica_index is not None and
            self.replica_count > 0
        )

    def can_scale(self) -> bool:
        """Check if this agent can be scaled (has not reached max replicas)."""
        if self.max_replicas is None or self.replica_count is None:
            return True  # No limit defined
        return self.replica_count < self.max_replicas
