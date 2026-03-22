from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


# =============================================================================
# Schema Version
# =============================================================================

SCHEMA_VERSION = "1.1"


# =============================================================================
# Event Type Enums
# =============================================================================

class SystemEventType(str, Enum):
    """System and orchestration event types."""

    # Agent lifecycle
    AGENT_SPAWN_REQUESTED = "agent.spawn.requested"
    AGENT_SPAWNED = "agent.spawned"
    AGENT_SHUTDOWN_REQUESTED = "agent.shutdown.requested"
    AGENT_RESTART_REQUESTED = "agent.restart.requested"
    AGENT_RESTART_COMPLETED = "agent.restart.completed"

    # Scaling
    AGENT_SCALE_REQUESTED = "agent.scale.requested"
    AGENT_SCALE_COMPLETED = "agent.scale.completed"

    # Health and metrics
    AGENT_HEARTBEAT = "agent.heartbeat"
    AGENT_METRICS_UPDATED = "agent.metrics.updated"
    AGENT_HEALTH_DEGRADED = "agent.health.degraded"
    AGENT_HEALTH_CRITICAL = "agent.health.critical"
    AGENT_OVERLOADED = "agent.overloaded"
    AGENT_ERROR = "agent.error"
    AGENT_TIMEOUT = "agent.timeout"

    # Placement/orchestration
    PLACEMENT_REQUESTED = "placement.requested"
    PLACEMENT_COMPLETED = "placement.completed"

    # Recovery
    RECOVERY_TRIGGERED = "recovery.triggered"
    FALLBACK_AGENT_SELECTED = "fallback.agent.selected"

    # Monitoring
    METRICS_UPDATED = "metrics.updated"
    LAG_DETECTED = "lag.detected"
    THROUGHPUT_UPDATED = "throughput.updated"


class RegistryEventType(str, Enum):
    """Registry event types."""

    AGENT_REGISTERED = "registry.agent.registered"
    AGENT_DEREGISTERED = "registry.agent.deregistered"
    AGENT_UPDATED = "registry.agent.updated"
    AGENT_CARD_UPDATED = "registry.agent.card.updated"
    AGENT_CAPABILITY_CHANGED = "registry.agent.capability.changed"
    DISCOVERY_REQUEST = "registry.discovery.request"
    TOPOLOGY_CHANGED = "registry.topology.changed"


class AgentStatus(str, Enum):
    """Agent status values."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    OVERLOADED = "overloaded"
    ERROR = "error"
    TIMEOUT = "timeout"
    UNREACHABLE = "unreachable"
    STARTING = "starting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    RESTARTING = "restarting"
    SPAWNING = "spawning"
    SCALING = "scaling"


# =============================================================================
# Orchestration Metadata
# =============================================================================

class OrchestrationMetadata(BaseModel):
    """Metadata for orchestration context."""

    orchestrator: Optional[str] = Field(None, description="Orchestrator name (k8s, docker-swarm, acis-native)")
    cluster_id: Optional[str] = Field(None, description="Cluster identifier")
    node_id: Optional[str] = Field(None, description="Node/host identifier")
    namespace: Optional[str] = Field(None, description="Namespace or environment")
    deployment_id: Optional[str] = Field(None, description="Deployment identifier")
    environment: str = Field("production", description="Environment (production, staging, dev)")


# =============================================================================
# Base Event Schema
# =============================================================================

class Event(BaseModel):
    """Base event schema for ACIS-X v1.1"""

    event_id: str
    event_type: str
    event_source: str
    event_time: datetime
    correlation_id: Optional[str] = None
    entity_id: str
    schema_version: str = SCHEMA_VERSION
    payload: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


# =============================================================================
# Agent Metrics Payload
# =============================================================================

class AgentMetrics(BaseModel):
    """Metrics for agent health and performance monitoring."""

    # Resource metrics
    cpu_percent: Optional[float] = Field(None, ge=0, le=100, description="CPU usage percentage")
    memory_percent: Optional[float] = Field(None, ge=0, le=100, description="Memory usage percentage")

    # Queue and lag metrics
    queue_depth: Optional[int] = Field(None, ge=0, description="Number of messages in processing queue")
    consumer_lag: Optional[int] = Field(None, ge=0, description="Kafka consumer lag")

    # Kafka context for lag detection (FIX 2)
    topic: Optional[str] = Field(None, description="Kafka topic name")
    partition: Optional[int] = Field(None, ge=0, description="Kafka partition")
    consumer_group: Optional[str] = Field(None, description="Kafka consumer group")

    # Error and restart tracking
    error_count: Optional[int] = Field(None, ge=0, description="Total error count")
    restart_count: Optional[int] = Field(None, ge=0, description="Number of restarts")

    # Throughput metrics
    events_processed: Optional[int] = Field(None, ge=0, description="Total events processed")
    events_per_second: Optional[float] = Field(None, ge=0, description="Processing throughput")

    # Latency metrics
    latency_ms: Optional[float] = Field(None, ge=0, description="Average processing latency in ms")
    latency_p99_ms: Optional[float] = Field(None, ge=0, description="P99 latency in ms")

    # Uptime
    uptime_seconds: Optional[int] = Field(None, ge=0, description="Agent uptime in seconds")

    # Replica info for scaling decisions (FIX 1)
    replica_count: Optional[int] = Field(None, ge=0, description="Current replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum allowed replicas")
    replica_index: Optional[int] = Field(None, ge=0, description="This instance's replica index")


# =============================================================================
# Agent Instance Info
# =============================================================================

class AgentInstanceInfo(BaseModel):
    """Information about a specific agent instance."""

    agent_id: str = Field(..., description="Unique agent identifier")
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str = Field(..., description="Agent name")
    instance_id: str = Field(..., description="Unique instance identifier")
    host: Optional[str] = Field(None, description="Host where agent is running")
    port: Optional[int] = Field(None, description="Port the agent is listening on")
    version: str = Field(..., description="Agent version")
    group_id: Optional[str] = Field(None, description="Kafka consumer group ID")
    replica_index: Optional[int] = Field(None, ge=0, description="Replica index in scaling group")
    replica_count: Optional[int] = Field(None, ge=0, description="Total replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum replicas")


# =============================================================================
# Agent Health Payload
# =============================================================================

class AgentHealthPayload(BaseModel):
    """Payload for agent health events (heartbeat, degraded, critical)."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: Optional[str] = None
    host: Optional[str] = None
    status: str = Field(..., description="Agent status")
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    timestamp: datetime
    metrics: Optional[AgentMetrics] = None
    details: Optional[Dict[str, Any]] = None

    # Replica info for SelfHealing scaling decisions (FIX 1)
    replica_count: Optional[int] = Field(None, ge=0, description="Current replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum allowed replicas")
    replica_index: Optional[int] = Field(None, ge=0, description="This instance's replica index")


# =============================================================================
# Agent Spawn/Lifecycle Payloads
# =============================================================================

class AgentSpawnRequestPayload(BaseModel):
    """Payload for agent.spawn.requested event."""

    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str = Field(..., description="Type of agent to spawn")
    instance_id: Optional[str] = Field(None, description="Requested instance ID")
    host: Optional[str] = Field(None, description="Preferred host for placement")
    config: Optional[Dict[str, Any]] = Field(None, description="Agent configuration")
    reason: str = Field(..., description="Reason for spawning")
    requester: str = Field(..., description="Agent/service that requested spawn")
    priority: str = Field("normal", description="Spawn priority (low, normal, high, critical)")

    # Source agent context (for tracking which agent triggered the spawn)
    source_agent_id: Optional[str] = Field(None, description="Agent ID that triggered spawn request")
    source_instance_id: Optional[str] = Field(None, description="Instance ID that triggered spawn request")

    # Scaling context
    replica_count: Optional[int] = Field(None, ge=0, description="Current replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum allowed replicas")

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule that triggered spawn decision")
    decision_score: Optional[float] = Field(None, description="Confidence score for decision")


class AgentSpawnedPayload(BaseModel):
    """Payload for agent.spawned event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    host: str
    port: Optional[int] = None
    version: str
    group_id: Optional[str] = None
    capabilities: List[str] = []
    subscribed_topics: List[str] = []
    produced_topics: List[str] = []
    spawned_at: datetime
    spawn_duration_ms: Optional[int] = Field(None, ge=0, description="Time taken to spawn in ms")

    # Scaling context
    replica_index: Optional[int] = None
    replica_count: Optional[int] = None
    max_replicas: Optional[int] = None


class AgentShutdownRequestPayload(BaseModel):
    """Payload for agent.shutdown.requested event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    reason: str
    requester: str
    graceful: bool = Field(True, description="Whether to perform graceful shutdown")
    timeout_seconds: int = Field(30, ge=1, description="Shutdown timeout")

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule that triggered shutdown decision")
    decision_score: Optional[float] = Field(None, description="Confidence score for decision")


class AgentRestartRequestPayload(BaseModel):
    """Payload for agent.restart.requested event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    reason: str
    requester: str
    graceful: bool = True
    timeout_seconds: int = 30
    restart_count: Optional[int] = Field(None, ge=0)
    max_restarts: Optional[int] = Field(3, ge=1)

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule that triggered restart decision")
    decision_score: Optional[float] = Field(None, description="Confidence score for decision")


class AgentRestartCompletedPayload(BaseModel):
    """Payload for agent.restart.completed event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    host: str
    version: str
    restarted_at: datetime
    restart_duration_ms: Optional[int] = None
    restart_count: int
    previous_error: Optional[str] = None
    status: str = "healthy"


# =============================================================================
# Scaling Payloads
# =============================================================================

class AgentScaleRequestPayload(BaseModel):
    """Payload for agent.scale.requested event."""

    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str = Field(..., description="Agent type to scale")
    current_replicas: int = Field(..., ge=0, description="Current replica count")
    desired_replicas: int = Field(..., ge=0, description="Desired replica count")
    max_replicas: int = Field(..., ge=1, description="Maximum allowed replicas")
    min_replicas: int = Field(1, ge=0, description="Minimum required replicas")
    scale_direction: str = Field(..., description="up or down")
    reason: str
    requester: str

    # Trigger metrics
    trigger_metric: Optional[str] = Field(None, description="Metric that triggered scaling")
    trigger_value: Optional[float] = None
    trigger_threshold: Optional[float] = None

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule that triggered scale decision")
    decision_score: Optional[float] = Field(None, description="Confidence score for decision")


class AgentScaleCompletedPayload(BaseModel):
    """Payload for agent.scale.completed event."""

    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    previous_replicas: int
    current_replicas: int
    max_replicas: int
    scale_direction: str
    scaled_at: datetime
    scale_duration_ms: Optional[int] = None
    instances: List[Dict[str, Any]] = Field(default_factory=list, description="List of instance details")
    status: str = Field("completed", description="completed, partial, failed")
    error_message: Optional[str] = None


# =============================================================================
# Overload Payload
# =============================================================================

class AgentOverloadedPayload(BaseModel):
    """Payload for agent.overloaded event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    host: Optional[str] = None
    status: str = "overloaded"
    timestamp: datetime

    # Overload indicators
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    queue_depth: Optional[int] = None
    consumer_lag: Optional[int] = None
    error_count: Optional[int] = None

    # Kafka context (FIX 2)
    topic: Optional[str] = Field(None, description="Topic with lag/overload")
    partition: Optional[int] = Field(None, description="Partition with lag/overload")
    consumer_group: Optional[str] = Field(None, description="Consumer group")

    # Thresholds
    cpu_threshold: Optional[float] = Field(None, description="CPU threshold that was exceeded")
    memory_threshold: Optional[float] = Field(None, description="Memory threshold that was exceeded")
    queue_threshold: Optional[int] = Field(None, description="Queue depth threshold")
    lag_threshold: Optional[int] = Field(None, description="Consumer lag threshold")

    # Replica info (FIX 1)
    replica_count: Optional[int] = Field(None, ge=0, description="Current replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum replicas")
    replica_index: Optional[int] = Field(None, ge=0, description="This instance's replica index")

    # Recommendations
    recommended_action: Optional[str] = Field(None, description="scale_up, restart, shed_load")
    decision_rule: Optional[str] = Field(None, description="Rule recommending action")
    decision_score: Optional[float] = Field(None, description="Confidence score")
    details: Optional[Dict[str, Any]] = None


# =============================================================================
# Placement Payloads
# =============================================================================

class PlacementRequestPayload(BaseModel):
    """Payload for placement.requested event."""

    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str = Field(..., description="Agent type to place")
    instance_id: Optional[str] = None
    requirements: Optional[Dict[str, Any]] = Field(None, description="Resource requirements")
    affinity: Optional[Dict[str, Any]] = Field(None, description="Placement affinity rules")
    anti_affinity: Optional[Dict[str, Any]] = Field(None, description="Placement anti-affinity rules")
    preferred_hosts: Optional[List[str]] = None
    excluded_hosts: Optional[List[str]] = None
    requester: str
    priority: str = "normal"

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule for placement decision")
    decision_score: Optional[float] = Field(None, description="Placement score")


class PlacementCompletedPayload(BaseModel):
    """Payload for placement.completed event."""

    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: str
    host: str
    port: Optional[int] = None
    placement_decision: str = Field(..., description="Reason for placement decision")
    alternatives_considered: Optional[List[str]] = Field(None, description="Other hosts considered")
    placement_duration_ms: Optional[int] = None
    status: str = Field("completed", description="completed, failed")
    error_message: Optional[str] = None

    # Decision context (FIX 3)
    decision_rule: Optional[str] = Field(None, description="Rule used for placement")
    decision_score: Optional[float] = Field(None, description="Placement confidence score")


# =============================================================================
# Metrics Update Payload
# =============================================================================

class AgentMetricsUpdatePayload(BaseModel):
    """Payload for agent.metrics.updated event."""

    agent_id: str
    agent_type: Optional[str] = Field(None, description="Logical agent type (FIX 4)")
    agent_name: str
    instance_id: Optional[str] = None
    host: Optional[str] = None
    timestamp: datetime

    # Core metrics
    cpu_percent: Optional[float] = None
    memory_percent: Optional[float] = None
    queue_depth: Optional[int] = None
    consumer_lag: Optional[int] = None
    error_count: Optional[int] = None
    restart_count: Optional[int] = None

    # Kafka context (FIX 2)
    topic: Optional[str] = Field(None, description="Primary topic being consumed")
    partition: Optional[int] = Field(None, description="Partition assignment")
    consumer_group: Optional[str] = Field(None, description="Consumer group")

    # Throughput metrics
    events_processed: Optional[int] = None
    events_per_second: Optional[float] = None

    # Latency metrics
    latency_ms: Optional[float] = None
    latency_p50_ms: Optional[float] = None
    latency_p95_ms: Optional[float] = None
    latency_p99_ms: Optional[float] = None

    # Kafka metrics
    partitions_assigned: Optional[int] = None
    commit_lag_ms: Optional[float] = None

    # Replica info for scaling decisions (FIX 1)
    replica_count: Optional[int] = Field(None, ge=0, description="Current replica count")
    max_replicas: Optional[int] = Field(None, ge=1, description="Maximum allowed replicas")
    replica_index: Optional[int] = Field(None, ge=0, description="This instance's replica index")

    # Custom metrics
    custom_metrics: Optional[Dict[str, Any]] = None


# =============================================================================
# System-wide Metrics Payload
# =============================================================================

class SystemMetricsPayload(BaseModel):
    """Payload for system-wide metrics.updated event."""

    timestamp: datetime

    # Orchestration context (FIX 5)
    orchestrator: Optional[str] = Field(None, description="Orchestrator name")
    cluster_id: Optional[str] = Field(None, description="Cluster identifier")
    node_id: Optional[str] = Field(None, description="Node identifier")

    # Agent counts
    total_agents: int = 0
    healthy_agents: int = 0
    degraded_agents: int = 0
    critical_agents: int = 0
    error_agents: int = 0

    # Replica counts
    total_replicas: int = Field(0, description="Total running replicas")
    max_replicas: int = Field(0, description="Maximum configured replicas")

    # Throughput
    total_events_processed: int = 0
    events_per_second: float = 0

    # Lag
    total_consumer_lag: int = 0
    max_consumer_lag: int = 0

    # Latency
    avg_processing_latency_ms: float = 0
    max_processing_latency_ms: float = 0

    # Errors
    total_errors: int = 0
    errors_per_minute: float = 0

    # Per-agent breakdown
    agent_metrics: Optional[Dict[str, AgentMetrics]] = None


# =============================================================================
# DLQ Event Schema
# =============================================================================

class DLQErrorInfo(BaseModel):
    """Error information for DLQ events."""

    code: str = Field(..., description="Error code/type")
    message: str = Field(..., description="Error message")
    stack_trace: Optional[str] = None
    failed_at: datetime
    retry_count: int = Field(..., ge=0)
    max_retries: int = Field(..., ge=0)
    consumer_group: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    topic: Optional[str] = Field(None, description="Source topic")

    # Additional context
    agent_id: Optional[str] = None
    agent_type: Optional[str] = None
    instance_id: Optional[str] = None
    host: Optional[str] = None


class DLQEvent(BaseModel):
    """Dead Letter Queue event wrapper."""

    event_id: str
    event_type: str = "dlq.event.failed"
    event_source: str
    event_time: datetime
    correlation_id: Optional[str] = None
    entity_id: str
    schema_version: str = SCHEMA_VERSION
    payload: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class DLQEventPayload(BaseModel):
    """Structured payload for DLQ events."""

    original_event: Dict[str, Any] = Field(..., description="The original event that failed")
    error: DLQErrorInfo = Field(..., description="Error information")
    original_topic: str = Field(..., description="Topic where event originated")
    dlq_reason: str = Field(..., description="Reason for DLQ routing")


# =============================================================================
# Event Factories
# =============================================================================

def create_agent_health_event(
    event_source: str,
    agent_id: str,
    agent_name: str,
    status: str,
    event_type: str = SystemEventType.AGENT_HEARTBEAT,
    agent_type: Optional[str] = None,
    instance_id: Optional[str] = None,
    host: Optional[str] = None,
    metrics: Optional[AgentMetrics] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    correlation_id: Optional[str] = None,
    replica_count: Optional[int] = None,
    max_replicas: Optional[int] = None,
    replica_index: Optional[int] = None,
    orchestration: Optional[OrchestrationMetadata] = None,
) -> Dict[str, Any]:
    """Factory to create agent health events."""
    import uuid

    payload = AgentHealthPayload(
        agent_id=agent_id,
        agent_type=agent_type,
        agent_name=agent_name,
        instance_id=instance_id,
        host=host,
        status=status,
        error_code=error_code,
        error_message=error_message,
        timestamp=datetime.utcnow(),
        metrics=metrics,
        replica_count=replica_count,
        max_replicas=max_replicas,
        replica_index=replica_index,
    )

    metadata = {"environment": "production"}
    if orchestration:
        metadata.update(orchestration.model_dump(exclude_none=True))

    return Event(
        event_id=f"evt_{uuid.uuid4()}",
        event_type=event_type if isinstance(event_type, str) else event_type.value,
        event_source=event_source,
        event_time=datetime.utcnow(),
        correlation_id=correlation_id,
        entity_id=agent_name,
        schema_version=SCHEMA_VERSION,
        payload=payload.model_dump(),
        metadata=metadata,
    ).model_dump()


def create_scale_request_event(
    event_source: str,
    agent_name: str,
    current_replicas: int,
    desired_replicas: int,
    max_replicas: int,
    reason: str,
    agent_type: Optional[str] = None,
    trigger_metric: Optional[str] = None,
    trigger_value: Optional[float] = None,
    decision_rule: Optional[str] = None,
    decision_score: Optional[float] = None,
    correlation_id: Optional[str] = None,
    orchestration: Optional[OrchestrationMetadata] = None,
) -> Dict[str, Any]:
    """Factory to create scaling request events."""
    import uuid

    scale_direction = "up" if desired_replicas > current_replicas else "down"

    payload = AgentScaleRequestPayload(
        agent_type=agent_type,
        agent_name=agent_name,
        current_replicas=current_replicas,
        desired_replicas=desired_replicas,
        max_replicas=max_replicas,
        scale_direction=scale_direction,
        reason=reason,
        requester=event_source,
        trigger_metric=trigger_metric,
        trigger_value=trigger_value,
        decision_rule=decision_rule,
        decision_score=decision_score,
    )

    metadata = {"environment": "production"}
    if orchestration:
        metadata.update(orchestration.model_dump(exclude_none=True))

    return Event(
        event_id=f"evt_{uuid.uuid4()}",
        event_type=SystemEventType.AGENT_SCALE_REQUESTED.value,
        event_source=event_source,
        event_time=datetime.utcnow(),
        correlation_id=correlation_id,
        entity_id=agent_name,
        schema_version=SCHEMA_VERSION,
        payload=payload.model_dump(),
        metadata=metadata,
    ).model_dump()


def create_spawn_request_event(
    event_source: str,
    agent_name: str,
    reason: str,
    agent_type: Optional[str] = None,
    instance_id: Optional[str] = None,
    host: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    priority: str = "normal",
    source_agent_id: Optional[str] = None,
    source_instance_id: Optional[str] = None,
    replica_count: Optional[int] = None,
    max_replicas: Optional[int] = None,
    decision_rule: Optional[str] = None,
    decision_score: Optional[float] = None,
    correlation_id: Optional[str] = None,
    orchestration: Optional[OrchestrationMetadata] = None,
) -> Dict[str, Any]:
    """Factory to create spawn request events."""
    import uuid

    payload = AgentSpawnRequestPayload(
        agent_type=agent_type,
        agent_name=agent_name,
        instance_id=instance_id,
        host=host,
        config=config,
        reason=reason,
        requester=event_source,
        priority=priority,
        source_agent_id=source_agent_id,
        source_instance_id=source_instance_id,
        replica_count=replica_count,
        max_replicas=max_replicas,
        decision_rule=decision_rule,
        decision_score=decision_score,
    )

    metadata = {"environment": "production"}
    if orchestration:
        metadata.update(orchestration.model_dump(exclude_none=True))

    return Event(
        event_id=f"evt_{uuid.uuid4()}",
        event_type=SystemEventType.AGENT_SPAWN_REQUESTED.value,
        event_source=event_source,
        event_time=datetime.utcnow(),
        correlation_id=correlation_id,
        entity_id=agent_name,
        schema_version=SCHEMA_VERSION,
        payload=payload.model_dump(),
        metadata=metadata,
    ).model_dump()


def create_restart_request_event(
    event_source: str,
    agent_id: str,
    agent_name: str,
    instance_id: str,
    reason: str,
    agent_type: Optional[str] = None,
    graceful: bool = True,
    timeout_seconds: int = 30,
    restart_count: Optional[int] = None,
    max_restarts: int = 3,
    decision_rule: Optional[str] = None,
    decision_score: Optional[float] = None,
    correlation_id: Optional[str] = None,
    orchestration: Optional[OrchestrationMetadata] = None,
) -> Dict[str, Any]:
    """Factory to create restart request events."""
    import uuid

    payload = AgentRestartRequestPayload(
        agent_id=agent_id,
        agent_type=agent_type,
        agent_name=agent_name,
        instance_id=instance_id,
        reason=reason,
        requester=event_source,
        graceful=graceful,
        timeout_seconds=timeout_seconds,
        restart_count=restart_count,
        max_restarts=max_restarts,
        decision_rule=decision_rule,
        decision_score=decision_score,
    )

    metadata = {"environment": "production"}
    if orchestration:
        metadata.update(orchestration.model_dump(exclude_none=True))

    return Event(
        event_id=f"evt_{uuid.uuid4()}",
        event_type=SystemEventType.AGENT_RESTART_REQUESTED.value,
        event_source=event_source,
        event_time=datetime.utcnow(),
        correlation_id=correlation_id,
        entity_id=agent_name,
        schema_version=SCHEMA_VERSION,
        payload=payload.model_dump(),
        metadata=metadata,
    ).model_dump()


# =============================================================================
# Validation Helpers
# =============================================================================

def validate_event(data: Dict[str, Any]) -> Event:
    """Validate raw data as an Event."""
    return Event.model_validate(data)


def validate_health_payload(data: Dict[str, Any]) -> AgentHealthPayload:
    """Validate agent health payload."""
    return AgentHealthPayload.model_validate(data)


def validate_metrics(data: Dict[str, Any]) -> AgentMetrics:
    """Validate agent metrics."""
    return AgentMetrics.model_validate(data)


def validate_scale_request(data: Dict[str, Any]) -> AgentScaleRequestPayload:
    """Validate scale request payload."""
    return AgentScaleRequestPayload.model_validate(data)


def validate_spawn_request(data: Dict[str, Any]) -> AgentSpawnRequestPayload:
    """Validate spawn request payload."""
    return AgentSpawnRequestPayload.model_validate(data)


# =============================================================================
# Export all
# =============================================================================

__all__ = [
    # Version
    "SCHEMA_VERSION",

    # Enums
    "SystemEventType",
    "RegistryEventType",
    "AgentStatus",

    # Orchestration
    "OrchestrationMetadata",

    # Base schemas
    "Event",
    "DLQEvent",

    # Metrics
    "AgentMetrics",
    "AgentInstanceInfo",

    # Health payloads
    "AgentHealthPayload",

    # Lifecycle payloads
    "AgentSpawnRequestPayload",
    "AgentSpawnedPayload",
    "AgentShutdownRequestPayload",
    "AgentRestartRequestPayload",
    "AgentRestartCompletedPayload",

    # Scaling payloads
    "AgentScaleRequestPayload",
    "AgentScaleCompletedPayload",

    # Overload
    "AgentOverloadedPayload",

    # Placement
    "PlacementRequestPayload",
    "PlacementCompletedPayload",

    # Metrics payloads
    "AgentMetricsUpdatePayload",
    "SystemMetricsPayload",

    # DLQ
    "DLQErrorInfo",
    "DLQEventPayload",

    # Factories
    "create_agent_health_event",
    "create_scale_request_event",
    "create_spawn_request_event",
    "create_restart_request_event",

    # Validators
    "validate_event",
    "validate_health_payload",
    "validate_metrics",
    "validate_scale_request",
    "validate_spawn_request",
]
