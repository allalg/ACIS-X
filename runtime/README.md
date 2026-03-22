# ACIS-X Runtime Infrastructure

Production-ready Kafka abstraction layer for ACIS-X event-driven system.

## Components

### 1. KafkaClient (`kafka_client.py`)

Main Kafka producer/consumer abstraction with:

**Features:**
- ✅ Event schema integration (Pydantic validation)
- ✅ Publish with partition key and headers
- ✅ Subscribe with consumer groups
- ✅ Manual offset commit (exactly-once semantics)
- ✅ Correlation ID propagation via headers
- ✅ DLQ (Dead Letter Queue) support
- ✅ Consumer lag tracking
- ✅ Backend flexibility (confluent-kafka or kafka-python)
- ✅ Serialization support (JSON, Avro*, Protobuf*)

**Producer API:**
```python
client.publish(
    topic="acis.invoices",
    event=event.model_dump(),
    key="customer_123",  # Partition key (optional, defaults to entity_id)
    headers={"trace_id": "abc"},  # Custom headers
    partition=2,  # Specific partition (optional)
)
```

**Consumer API:**
```python
client.subscribe(
    topics=["acis.invoices", "acis.payments"],
    group_id="my-consumer-group",
    handler=message_callback,  # Optional handler
)

messages = client.poll(timeout_ms=1000, max_messages=100)
for msg in messages:
    # Process message
    client.commit(msg)  # Commit offset
```

**DLQ Handling:**
```python
client.send_to_dlq(
    original_topic="acis.invoices",
    event=failed_event,
    error=exception,
    retry_count=3,
    agent_name="InvoiceMonitor",
)
# Publishes to: acis.invoices.dlq
```

**Lag Tracking:**
```python
# Total lag across all topics
total_lag = client.get_consumer_lag()

# Lag for specific topic
topic_lag = client.get_consumer_lag(topic="acis.invoices")

# Lag for specific partition
partition_lag = client.get_consumer_lag(topic="acis.invoices", partition=0)
```

### 2. RetryStrategy (`retry_strategy.py`)

Configurable retry logic with backoff:

**Strategies:**
- `IMMEDIATE` - No delay
- `LINEAR_BACKOFF` - base × attempt
- `EXPONENTIAL_BACKOFF` - base × 2^attempt (default)
- `FIBONACCI_BACKOFF` - base × fib(attempt)

**Usage:**
```python
config = RetryConfig(
    max_retries=3,
    strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    base_delay_ms=100,
    max_delay_ms=30000,
    jitter=True,
)

result = retry_with_backoff(
    func=risky_operation,
    config=config,
    arg1="value",
)
```

**Circuit Breaker:**
```python
breaker = CircuitBreaker(
    CircuitBreakerConfig(
        failure_threshold=5,
        success_threshold=2,
        timeout_seconds=60,
    )
)

result = breaker.call(risky_operation, arg1="value")
```

### 3. TopicManager (`topic_manager.py`)

Kafka topic administration:

**Standard Topics:**
| Topic | Partitions | Retention | Cleanup |
|-------|------------|-----------|---------|
| `acis.invoices` | 6 | 7 days | delete |
| `acis.payments` | 6 | 7 days | delete |
| `acis.customers` | 3 | 30 days | compact |
| `acis.risk` | 4 | 14 days | delete |
| `acis.policy` | 2 | 30 days | delete |
| `acis.external` | 3 | 7 days | delete |
| `acis.commands` | 3 | 1 day | delete |
| `acis.system` | 4 | 30 days | delete |
| `acis.agent.health` | 2 | 7 days | delete |
| `acis.registry` | 1 | ∞ | compact |
| `*.dlq` | 1 | 90 days | delete |

**Usage:**
```python
admin = TopicAdmin(
    bootstrap_servers=["localhost:9092"],
    backend="confluent",
)

# Create all ACIS topics
results = admin.create_all_acis_topics()

# Create custom topic
admin.create_topic(
    TopicConfig(
        name="my.topic",
        partitions=6,
        replication_factor=3,
        retention_ms=7 * 24 * 3600 * 1000,
    )
)

# List topics
topics = admin.list_topics()

# Delete topic
admin.delete_topic("old.topic")
```

## Configuration

### KafkaConfig

```python
config = KafkaConfig(
    # Brokers
    bootstrap_servers=["kafka-1:9092", "kafka-2:9092"],

    # Security
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_username="user",
    sasl_password="pass",

    # Producer
    producer_acks="all",  # all, 1, 0
    producer_retries=3,
    producer_compression="gzip",

    # Consumer
    consumer_auto_offset_reset="earliest",
    consumer_enable_auto_commit=False,  # Manual commit recommended
    consumer_max_poll_records=500,

    # DLQ
    enable_dlq=True,
    dlq_suffix=".dlq",
    max_retries=3,

    # Serialization
    serialization_format=SerializationFormat.JSON,
)

client = KafkaClient(config, backend="confluent")
```

## Integration with BaseAgent

BaseAgent expects this interface:

```python
class KafkaClient:
    def subscribe(self, topics: List[str], group_id: str) -> None
    def poll(self, timeout_ms: int) -> List[KafkaMessage]
    def publish(self, topic: str, event: Dict[str, Any]) -> None
    def commit(self, message: KafkaMessage) -> None
    def close(self) -> None
    def get_consumer_lag(self) -> int  # Optional
```

All methods implemented ✅

## Dependencies

**confluent-kafka-python (recommended):**
```bash
pip install confluent-kafka
```

**OR kafka-python:**
```bash
pip install kafka-python
```

**For Avro support:**
```bash
pip install confluent-kafka[avro]
```

## Notes

- `KafkaMessage.high_watermark` may not be available on all backends
- Avro/Protobuf serialization requires schema registry setup
- Circuit breaker is stateful (one instance per operation)
- DLQ topics are auto-created if `enable_dlq=True`
