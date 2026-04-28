import pytest
from unittest.mock import MagicMock, patch
from typing import Generator, Dict, Any
import tempfile
import os


# =====================================================================
# Kafka Mocking Fixtures
# =====================================================================

@pytest.fixture
def mock_kafka_config():
    """Mock KafkaConfig for testing without broker."""
    from runtime.kafka_client import KafkaConfig

    return KafkaConfig(
        bootstrap_servers=["localhost:9092"],
        client_id="test-client",
        sasl_mechanism=None,
    )


@pytest.fixture
def mock_kafka_client(mock_kafka_config):
    """Mock KafkaClient that doesn't require a running Kafka broker.

    Returns a KafkaClient with mocked producer and consumer.
    Each event published through mock_publish is validated against
    EventEnvelope before being stored.  Invalid events go to
    client.dlq_events instead of client.published_events so tests
    can assert_no_dlq_events() to catch schema violations early.
    """
    from unittest.mock import MagicMock
    from runtime.kafka_client import KafkaClient
    from schemas.event_envelope import EventEnvelope

    # Create real client but patch the internal producer/consumer
    client = KafkaClient(config=mock_kafka_config, backend="kafka-python")

    # Mock the producer and consumer so they don't try to connect
    client._producer = MagicMock()
    client._consumer = MagicMock()

    # Mock publish: validate against EventEnvelope before accepting
    published_events: list = []
    dlq_events: list = []

    def mock_publish(topic, event, key=None, partition=None, headers=None):
        try:
            EventEnvelope(**event)
            published_events.append({
                "topic": topic,
                "event": event,
                "key": key,
                "partition": partition,
            })
        except Exception as e:
            dlq_events.append({
                "topic": topic,
                "event": event,
                "error": str(e),
            })
        return True

    client.publish = mock_publish
    client.published_events = published_events
    client.dlq_events = dlq_events

    return client


# =====================================================================
# Test Helpers
# =====================================================================

def assert_no_dlq_events(client) -> None:
    """Assert that no events failed schema validation during the test.

    Call this at the end of any test that publishes events to verify
    the pipeline never produced a malformed EventEnvelope.

    Args:
        client: A mock_kafka_client fixture instance.

    Raises:
        AssertionError: If any DLQ events are present.
    """
    assert len(client.dlq_events) == 0, (
        f"Unexpected DLQ events ({len(client.dlq_events)} total): "
        f"{client.dlq_events}"
    )



# =====================================================================
# Database Fixtures
# =====================================================================

@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    """Create a temporary SQLite database for testing.

    Yields the path to the database, cleans up after test.
    """
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    yield path

    # Cleanup
    if os.path.exists(path):
        os.remove(path)

    # Also clean up WAL files if they exist
    for suffix in ["-wal", "-shm"]:
        wal_path = path + suffix
        if os.path.exists(wal_path):
            os.remove(wal_path)


@pytest.fixture
def db_agent(mock_kafka_client, temp_db_path):
    """Create a DBAgent with temporary database for testing."""
    from agents.storage.db_agent import DBAgent

    agent = DBAgent(
        kafka_client=mock_kafka_client,
        db_path=temp_db_path
    )
    return agent


@pytest.fixture
def query_agent(mock_kafka_client, temp_db_path):
    """Create a QueryAgent with temporary database for testing."""
    from agents.storage.query_agent import QueryAgent

    agent = QueryAgent(
        kafka_client=mock_kafka_client,
        db_path=temp_db_path
    )
    return agent


# =====================================================================
# Event Schema Fixtures
# =====================================================================

@pytest.fixture
def sample_customer_event() -> Dict[str, Any]:
    """Sample customer.profile.created event."""
    from datetime import datetime

    return {
        "event_id": "evt_001",
        "event_type": "customer.profile.created",
        "event_source": "test",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "cust_00001",
        "schema_version": "1.1",
        "payload": {
            "customer_id": "cust_00001",
            "name": "ACME Corp",
            "credit_limit": 100000,
        },
        "metadata": {},
    }


@pytest.fixture
def sample_invoice_event() -> Dict[str, Any]:
    """Sample invoice.created event."""
    from datetime import datetime

    return {
        "event_id": "evt_002",
        "event_type": "invoice.created",
        "event_source": "test",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "inv_00001",
        "schema_version": "1.1",
        "payload": {
            "invoice_id": "inv_00001",
            "customer_id": "cust_00001",
            "total_amount": 50000,
            "issued_date": datetime.utcnow().isoformat(),
            "due_date": datetime.utcnow().isoformat(),
            "status": "issued",
        },
        "metadata": {},
    }


# =====================================================================
# Markers
# =====================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "unit: Unit tests (no Kafka)")
    config.addinivalue_line("markers", "integration: Integration tests (needs Kafka)")
    config.addinivalue_line("markers", "schema: Database schema tests")
    config.addinivalue_line("markers", "slow: Slow tests")


# =====================================================================
# Test Collection Hooks
# =====================================================================

def pytest_collection_modifyitems(config, items):
    """Auto-mark tests based on names and paths."""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid or "broker" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)
        # Mark unit tests by default if not marked
        elif not any(marker.name in ["integration", "slow"] for marker in item.iter_markers()):
            item.add_marker(pytest.mark.unit)

        # Mark schema tests
        if "schema" in item.nodeid:
            item.add_marker(pytest.mark.schema)
