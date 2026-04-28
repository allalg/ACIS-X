from pydantic import BaseModel, Field, constr, field_validator
from typing import Literal, Dict, Any
from datetime import datetime, timezone

class EventEnvelope(BaseModel):
    event_id: constr(min_length=1)
    event_type: constr(pattern=r"^[a-z]+(\.[a-z_]+)+$")
    event_source: str
    event_time: datetime
    entity_id: str
    schema_version: Literal["1.1"]
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator('event_time')
    def enforce_naive_utc(cls, v):
        if v.tzinfo is not None:
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v
