import pytest
from unittest.mock import patch, MagicMock
from runtime.placement_engine import PlacementEngine
from schemas.event_schema import Event
from datetime import datetime

@pytest.fixture
def mock_kafka_client():
    client = MagicMock()
    return client

def test_placement_routing_round_robin(mock_kafka_client):
    with patch("runtime.placement_engine.requests.get") as mock_get, \
         patch("threading.Thread.start") as mock_thread_start:
        
        # Mock registry response
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"instance_id": "inst_1", "capabilities": ["risk_scoring"]},
            {"instance_id": "inst_2", "capabilities": ["risk_scoring"]},
            {"instance_id": "inst_3", "capabilities": ["risk_scoring"]},
        ]
        mock_get.return_value = mock_resp
        
        engine = PlacementEngine(kafka_client=mock_kafka_client)
        engine.publish_event = MagicMock()
        
        # Call the logic directly to avoid waiting
        mock_resp = mock_get()
        new_table = {}
        for a in mock_resp.json():
            for c in a.get("capabilities", []):
                new_table.setdefault(c, []).append(a["instance_id"])
        with engine._routing_lock:
            engine._routing_table = new_table

        # Send 4 requests
        for i in range(4):
            event = Event(
                event_id=f"req_{i}",
                event_type="placement.routing.request",
                event_source="test",
                event_time=datetime.utcnow(),
                entity_id="test",
                payload={"required_capability": "risk_scoring", "message_key": f"key_{i}", "original_topic": "input_topic"}
            )
            engine.process_event(event)

        # Assert round-robin
        assert engine.publish_event.call_count == 4
        calls = engine.publish_event.call_args_list
        assigned_instances = [call.kwargs["payload"]["instance_id"] for call in calls]
        
        # Should be inst_1, inst_2, inst_3, inst_1
        assert assigned_instances == ["inst_1", "inst_2", "inst_3", "inst_1"]

def test_placement_routing_no_capable_agent(mock_kafka_client):
    with patch("runtime.placement_engine.requests.get") as mock_get, \
         patch("threading.Thread.start") as mock_thread_start:
        engine = PlacementEngine(kafka_client=mock_kafka_client)
        engine.publish_event = MagicMock()
        
        # Empty routing table
        with engine._routing_lock:
            engine._routing_table = {}
            
        event = Event(
            event_id="req_empty",
            event_type="placement.routing.request",
            event_source="test",
            event_time=datetime.utcnow(),
            entity_id="test",
            payload={"required_capability": "risk_scoring", "message_key": "key_empty", "original_topic": "input_topic"}
        )
        engine.process_event(event)
        
        assert engine.publish_event.call_count == 1
        call = engine.publish_event.call_args_list[0]
        assert call.kwargs["topic"] == "acis.alerts"
        assert call.kwargs["event_type"] == "placement.failed"
        assert call.kwargs["payload"]["reason"] == "no_capable_agent"

def test_placement_routing_registry_update(mock_kafka_client):
    with patch("runtime.placement_engine.requests.get") as mock_get, \
         patch("threading.Thread.start") as mock_thread_start:
        engine = PlacementEngine(kafka_client=mock_kafka_client)
        engine.publish_event = MagicMock()
        
        # Initial routing table with 3 agents
        with engine._routing_lock:
            engine._routing_table = {"risk_scoring": ["inst_1", "inst_2", "inst_3"]}
            
        # Registry poll returns only 2 agents now (one went DEGRADED)
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {"instance_id": "inst_1", "capabilities": ["risk_scoring"]},
            {"instance_id": "inst_2", "capabilities": ["risk_scoring"]},
        ]
        mock_get.return_value = mock_resp
        
        # Simulate the poll update
        resp = mock_get()
        new_table = {}
        for a in resp.json():
            for c in a.get("capabilities", []):
                new_table.setdefault(c, []).append(a["instance_id"])
        with engine._routing_lock:
            engine._routing_table = new_table
            
        event = Event(
            event_id="req_upd",
            event_type="placement.routing.request",
            event_source="test",
            event_time=datetime.utcnow(),
            entity_id="test",
            payload={"required_capability": "risk_scoring", "message_key": "key", "original_topic": "t"}
        )
        engine.process_event(event)
        
        assert engine.publish_event.call_count == 1
        call = engine.publish_event.call_args_list[0]
        assert call.kwargs["payload"]["instance_id"] in ["inst_1", "inst_2"]
        
        # Verify routing table has exactly 2 agents
        assert len(engine._routing_table["risk_scoring"]) == 2
        assert "inst_3" not in engine._routing_table["risk_scoring"]
