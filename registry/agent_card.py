from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Literal, Optional
import socket

class AgentCard(BaseModel):
    agent_name: str
    instance_id: str
    group_id: str
    capabilities: List[str]
    subscribed_topics: List[str]
    published_topics: List[str]
    status: Literal["STARTING", "RUNNING", "DEGRADED", "STOPPED"]
    last_heartbeat: datetime
    pid: int
    host: str = Field(default_factory=socket.gethostname)
