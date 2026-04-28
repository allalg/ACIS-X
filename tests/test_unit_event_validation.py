import pytest
from datetime import datetime
from unittest.mock import MagicMock
from schemas.event_envelope import EventEnvelope
from runtime.kafka_client import KafkaClient, KafkaConfig

@pytest.fixture
def mock_kafka_client():
    config = KafkaConfig(
        bootstrap_servers=["localhost:9092"],
        client_id="test_client"
    )
    client = KafkaClient(config=config, backend="kafka-python")
    client._producer = MagicMock()
    return client

@pytest.mark.unit
def test_valid_event_passes_validation(mock_kafka_client):
    event = {
        "event_id": "123",
        "event_type": "customer.created",
        "event_source": "test_agent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "cust_1",
        "schema_version": "1.1",
        "payload": {},
        "metadata": {}
    }
    
    mock_kafka_client.publish("acis.customers", event)
    args, kwargs = mock_kafka_client._producer.send.call_args
    assert args[0] == "acis.customers"
    assert b"customer.created" in kwargs["value"]

@pytest.mark.unit
def test_wrong_schema_version_goes_to_dlq(mock_kafka_client):
    event = {
        "event_id": "123",
        "event_type": "customer.created",
        "event_source": "test_agent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "cust_1",
        "schema_version": "1.0",  # Invalid
        "payload": {}
    }
    
    mock_kafka_client.publish("acis.customers", event)
    args, kwargs = mock_kafka_client._producer.send.call_args
    assert args[0] == "acis.dlq"
    assert b"publish_validation_failed" in kwargs["value"]

@pytest.mark.unit
def test_invalid_event_type_goes_to_dlq(mock_kafka_client):
    event = {
        "event_id": "123",
        "event_type": "INVALID_FORMAT",  # Invalid format
        "event_source": "test_agent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "cust_1",
        "schema_version": "1.1",
        "payload": {}
    }
    
    mock_kafka_client.publish("acis.customers", event)
    args, kwargs = mock_kafka_client._producer.send.call_args
    assert args[0] == "acis.dlq"

@pytest.mark.unit
def test_missing_required_field_goes_to_dlq(mock_kafka_client):
    event = {
        "event_type": "customer.created",
        "event_source": "test_agent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "cust_1",
        "schema_version": "1.1",
        "payload": {}
    }
    
    mock_kafka_client.publish("acis.customers", event)
    args, kwargs = mock_kafka_client._producer.send.call_args
    assert args[0] == "acis.dlq"
