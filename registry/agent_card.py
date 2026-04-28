from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional
import socket

class AgentCard(BaseModel):
    agent_id: Optional[str] = None
    agent_name: str
    agent_type: Optional[str] = None
    instance_id: str
    group_id: str
    version: str = "1.0.0"
    capabilities: List[str]
    topics_consumed: List[str] = Field(default_factory=list)
    topics_produced: List[str] = Field(default_factory=list)
    status: Optional[str] = None
    last_heartbeat: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    pid: Optional[int] = None
    host: str = Field(default_factory=socket.gethostname)
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @property
    def subscribed_topics(self) -> List[str]:
        return self.topics_consumed

    @property
    def published_topics(self) -> List[str]:
        return self.topics_produced

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "AgentCard":
        data = dict(payload or {})
        if "topics_consumed" not in data:
            data["topics_consumed"] = data.get("subscribed_topics", [])
        if "topics_produced" not in data:
            data["topics_produced"] = data.get("published_topics", [])
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        if hasattr(self, "model_dump"):
            data = self.model_dump()
        else:
            data = self.dict()
        for key in ("last_heartbeat", "last_updated"):
            if data.get(key) is not None and hasattr(data[key], "isoformat"):
                data[key] = data[key].isoformat()
        data["subscribed_topics"] = list(self.topics_consumed)
        data["published_topics"] = list(self.topics_produced)
        return data
