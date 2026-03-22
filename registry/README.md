# ACIS-X Registry Service

Central agent registry and service discovery for ACIS-X.

## Overview

The `RegistryService` maintains an in-memory registry of all running agents in the system, providing:
- Agent registration and deregistration
- Service discovery by capability, name, or status
- Real-time topology tracking
- Automatic stale agent cleanup
- Health monitoring integration

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     RegistryService                         │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │    In-Memory Registry                                │  │
│  │    agent_id -> RegisteredAgent                       │  │
│  │    - agent_id, agent_name, status                    │  │
│  │    - capabilities, topics, group_id                  │  │
│  │    - host, port, version                             │  │
│  │    - last_heartbeat, metrics                         │  │
│  │    - replica_index, replica_count, max_replicas      │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  Subscribes to:                                             │
│  ├─ acis.agent.health (heartbeats)                         │
│  ├─ acis.registry (registration events)                    │
│  └─ acis.system (spawned, stopped, restart events)         │
│                                                             │
│  Publishes to:                                              │
│  ├─ registry.agent.registered                              │
│  ├─ registry.agent.updated                                 │
│  ├─ registry.agent.deregistered                            │
│  └─ registry.topology.changed                              │
└─────────────────────────────────────────────────────────────┘
```

## Usage

### Initialization

```python
from runtime.kafka_client import KafkaClient, KafkaConfig
from registry.registry_service import RegistryService

# Create Kafka client
kafka_config = KafkaConfig(
    bootstrap_servers=["localhost:9092"],
)
kafka_client = KafkaClient(kafka_config, backend="confluent")

# Create registry service
registry = RegistryService(
    kafka_client=kafka_client,
    service_id="registry-1",  # Optional
)

# Start service
registry.start()
```

### Query Methods

**Find agents by capability:**
```python
# Find all agents with payment prediction capability
agents = registry.find_by_capability("payment_prediction")
for agent in agents:
    print(f"{agent.agent_id} on {agent.host}")
```

**Find agents by name (all replicas):**
```python
# Find all PaymentPredictionAgent replicas
replicas = registry.find_by_name("PaymentPredictionAgent")
print(f"Found {len(replicas)} replicas")
```

**Find agent by ID:**
```python
agent = registry.find_by_id("agent_paymentprediction_abc123")
if agent:
    print(f"Status: {agent.status}")
    print(f"Healthy: {agent.is_healthy()}")
```

**Find healthy agents:**
```python
healthy_agents = registry.find_healthy()
print(f"{len(healthy_agents)} healthy agents")
```

**Get system topology:**
```python
topology = registry.get_topology()
print(f"Topology version: {topology['topology_version']}")
print(f"Total agents: {topology['total_agents']}")
print(f"Healthy agents: {topology['healthy_agents']}")
print(f"Agents by type: {topology['agents_by_type']}")
print(f"Agents by host: {topology['agents_by_host']}")
print(f"Capabilities map: {topology['capabilities_map']}")
```

### Management Methods

**Register agent manually:**
```python
from registry.registry_service import RegisteredAgent

agent = RegisteredAgent(
    agent_id="agent_custom_001",
    agent_name="CustomAgent",
    agent_type="CustomAgent",
    instance_id="instance_001",
    host="host-1.acis.local",
    port=8000,
    capabilities=["custom_processing"],
    topics_consumed=["acis.invoices"],
    topics_produced=["acis.risk"],
    status="healthy",
)

registry.register_agent(agent)
```

**Update agent:**
```python
registry.update_agent(
    agent_id="agent_custom_001",
    status="degraded",
    metrics={"cpu_percent": 85.0},
)
```

**Remove agent:**
```python
success = registry.remove_agent("agent_custom_001")
```

### Statistics

```python
stats = registry.get_stats()
print(f"Service ID: {stats['service_id']}")
print(f"Total agents: {stats['total_agents']}")
print(f"Healthy agents: {stats['healthy_agents']}")
print(f"Status counts: {stats['status_counts']}")
print(f"Type counts: {stats['type_counts']}")
print(f"Topology changes: {stats['topology_changes']}")
print(f"Events processed: {stats['events_processed']}")
```

### Export Registry

```python
# Export complete registry snapshot
snapshot = registry.export_registry()
with open("registry_snapshot.json", "w") as f:
    json.dump(snapshot, f, indent=2)
```

## Event Handling

### Incoming Events

| Event Type | Action |
|------------|--------|
| `agent.heartbeat` | Update last_heartbeat, metrics, status |
| `agent.spawned` | Register new agent |
| `agent.stopped` | Remove agent from registry |
| `agent.restart.completed` | Update status, restart_count |
| `registry.agent.registered` | Register or update agent |
| `registry.agent.deregistered` | Remove agent |
| `registry.agent.updated` | Update agent metadata |
| `registry.discovery.request` | Respond with matching agents |

### Outgoing Events

| Event Type | When | Payload |
|------------|------|---------|
| `registry.agent.registered` | New agent registered | Full agent details |
| `registry.agent.updated` | Agent metadata changed | Changed fields + reason |
| `registry.agent.deregistered` | Agent removed | Agent ID + deregistration time |
| `registry.topology.changed` | Agents added/removed | Topology version, counts |
| `registry.discovery.response` | Discovery request received | Matching agents |

## Features

### Auto-Registration from Heartbeat

If an agent sends a heartbeat but isn't registered, RegistryService automatically registers it:

```python
# Agent sends heartbeat -> auto-registered
{
  "event_type": "agent.heartbeat",
  "payload": {
    "agent_id": "agent_new_abc123",
    "agent_name": "NewAgent",
    ...
  }
}
```

### Stale Agent Cleanup

Automatically removes agents with heartbeat age > 180 seconds (2x timeout):
- Runs every 60 seconds
- Publishes `registry.agent.deregistered` for each removed agent
- Triggers `registry.topology.changed` event

### Topology Change Detection

Publishes `registry.topology.changed` when:
- Agents are added (register/spawn)
- Agents are removed (deregister/stop/cleanup)
- Cooldown: 30 seconds between topology events (prevents spam)

### Service Discovery

Supports discovery requests:

```python
# Publish discovery request
{
  "event_type": "registry.discovery.request",
  "payload": {
    "capability": "payment_prediction",
    "agent_name": "PaymentPredictionAgent",  # Optional
    "status": "healthy",  # Optional
  }
}

# Registry responds with discovery.response
{
  "event_type": "registry.discovery.response",
  "payload": {
    "requester": "...",
    "results": [...],  # List of matching agents
    "result_count": 3,
  }
}
```

## RegisteredAgent Model

```python
@dataclass
class RegisteredAgent:
    # Identity
    agent_id: str
    agent_name: str
    agent_type: Optional[str]
    instance_id: Optional[str]

    # Network
    host: Optional[str]
    port: Optional[int]

    # Configuration
    version: str
    group_id: Optional[str]
    capabilities: List[str]

    # Topics
    topics_consumed: List[str]
    topics_produced: List[str]

    # State
    status: str  # healthy, degraded, critical, etc.
    registered_at: Optional[datetime]
    last_heartbeat: Optional[datetime]
    last_updated: Optional[datetime]

    # Replica info
    replica_index: Optional[int]
    replica_count: Optional[int]
    max_replicas: Optional[int]

    # Metrics (latest)
    metrics: Dict[str, Any]

    def is_healthy(self, timeout: int = 90) -> bool:
        """Check if agent is healthy."""
        ...
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `HEARTBEAT_TIMEOUT_SECONDS` | 60 | Heartbeat age threshold for health check |
| `CLEANUP_INTERVAL_SECONDS` | 60 | How often to run stale agent cleanup |
| `TOPOLOGY_CHANGE_COOLDOWN_SECONDS` | 30 | Cooldown between topology events |

## Integration with ACIS-X

### SelfHealingAgent Integration

SelfHealingAgent subscribes to `acis.registry` to track agent registration/deregistration and detect missing agents.

### PlacementAgent Integration

PlacementAgent publishes to `acis.registry` when spawning new agents.

### BaseAgent Integration

BaseAgent publishes to `acis.registry` on startup/shutdown.

## Thread Safety

All registry operations are thread-safe:
- `_registry_lock` protects registry dictionary
- Query methods return copies/snapshots
- Concurrent reads and writes are safe

## Performance

- **O(1)** lookups by agent_id
- **O(n)** queries by capability/name/status
- **In-memory** storage (no database)
- Typical registry size: 10-100 agents
- Memory footprint: ~1KB per agent

## Limitations

- No persistence (in-memory only)
- Lost on restart (agents re-register via heartbeat)
- Single instance (no HA yet)
- No historical tracking (current state only)
