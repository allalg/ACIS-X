"""
Unit tests: MonitoringAgent → SelfHealingAgent health event routing.

Verifies that:
1. MonitoringAgent publishes agent.health.degraded and agent.health.critical
   to self.HEALTH_TOPIC (acis.agent.health).
2. SelfHealingAgent subscribes to acis.agent.health.
3. SelfHealingAgent.process_event() routes agent.health.degraded to
   _handle_degraded() and agent.health.critical to _handle_critical().
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call

from schemas.event_schema import Event, SystemEventType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_health_event(event_type: str, agent_name: str = "TestAgent") -> Event:
    """Build a minimal health event matching what MonitoringAgent publishes."""
    return Event(
        event_id=f"evt_{event_type.replace('.', '_')}_001",
        event_type=event_type,
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=agent_name,
        payload={
            "agent_id": f"agent_{agent_name}",
            "agent_name": agent_name,
            "agent_type": agent_name,
            "instance_id": f"inst_{agent_name}_0",
            "host": "localhost",
            "status": event_type.split(".")[-1],   # "degraded" or "critical"
            "error_code": "TEST_CONDITION",
            "error_message": "test condition",
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "cpu_percent": None,
                "memory_percent": None,
                "consumer_lag": 0,
                "error_count": 0,
                "events_per_second": 0.0,
                "latency_ms": None,
                "events_processed": 0,
                "queue_depth": 0,
            },
        },
        schema_version="1.1",
        metadata={},
    )


# ---------------------------------------------------------------------------
# Routing contract: MonitoringAgent publishes to HEALTH_TOPIC
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_monitoring_agent_health_topic_is_acis_agent_health(mock_kafka_client):
    """MonitoringAgent.HEALTH_TOPIC (inherited from BaseAgent) must be acis.agent.health."""
    from agents.base.base_agent import BaseAgent

    assert BaseAgent.HEALTH_TOPIC == "acis.agent.health", (
        "BaseAgent.HEALTH_TOPIC changed — MonitoringAgent will publish health events "
        "to the wrong topic and SelfHealingAgent will never receive them."
    )


@pytest.mark.unit
def test_monitoring_agent_publishes_degraded_to_health_topic(mock_kafka_client):
    """
    _publish_health_event_if_needed must use self.HEALTH_TOPIC for degraded events.

    Verifies the publish call at the code level so the test does not require
    a live Kafka broker.
    """
    from monitoring.monitoring_agent import MonitoringAgent
    import inspect

    source = inspect.getsource(MonitoringAgent._publish_health_event_if_needed)

    assert "self.HEALTH_TOPIC" in source, (
        "_publish_health_event_if_needed should publish via self.HEALTH_TOPIC; "
        "if it uses a hardcoded string or a different attribute the route is broken."
    )
    assert "AGENT_HEALTH_DEGRADED" in source or "agent.health.degraded" in source, (
        "_publish_health_event_if_needed should reference AGENT_HEALTH_DEGRADED "
        "or the literal string 'agent.health.degraded'."
    )
    assert "AGENT_HEALTH_CRITICAL" in source or "agent.health.critical" in source, (
        "_publish_health_event_if_needed should reference AGENT_HEALTH_CRITICAL "
        "or the literal string 'agent.health.critical'."
    )


# ---------------------------------------------------------------------------
# Routing contract: SelfHealingAgent subscribes to acis.agent.health
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_self_healing_agent_subscribes_to_health_topic(mock_kafka_client):
    """SelfHealingAgent must declare acis.agent.health in its subscribed_topics."""
    from self_healing.core.self_healing_agent import SelfHealingAgent

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)
    topics = agent.subscribe()

    assert "acis.agent.health" in topics, (
        "SelfHealingAgent.subscribe() does not include 'acis.agent.health'; "
        "it will never receive health events from MonitoringAgent."
    )


@pytest.mark.unit
def test_self_healing_agent_subscribed_topics_init_matches_subscribe(mock_kafka_client):
    """subscribed_topics passed to BaseAgent __init__ must match subscribe() return value."""
    from self_healing.core.self_healing_agent import SelfHealingAgent

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)

    # Both should agree
    assert set(agent.subscribed_topics) == set(agent.subscribe()), (
        "SelfHealingAgent.subscribed_topics (passed to BaseAgent) and subscribe() "
        "return different topic sets — Kafka subscription and routing are mismatched."
    )


# ---------------------------------------------------------------------------
# Core assertion: process_event routes agent.health.degraded → _handle_degraded
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_self_healing_process_event_routes_degraded_to_handle_degraded(mock_kafka_client):
    """
    process_event() must call _handle_degraded when it receives an
    agent.health.degraded event.

    Uses a mock SelfHealingAgent with _handle_degraded replaced by a spy so
    the test is independent of any Kafka, DB, or registry dependency.
    """
    from self_healing.core.self_healing_agent import SelfHealingAgent

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)

    # Patch the target handler AND the evaluate helper so we don't need
    # an actual recovery state machine to run.
    with patch.object(agent, "_handle_degraded", wraps=agent._handle_degraded) as spy_degraded, \
         patch.object(agent, "_evaluate_state"):          # suppress side-effects

        event = _make_health_event(SystemEventType.AGENT_HEALTH_DEGRADED.value)
        agent.process_event(event)

    spy_degraded.assert_called_once_with(event), (
        "process_event() did not call _handle_degraded for an agent.health.degraded event."
    )


@pytest.mark.unit
def test_self_healing_process_event_routes_critical_to_handle_critical(mock_kafka_client):
    """
    process_event() must call _handle_critical when it receives an
    agent.health.critical event.
    """
    from self_healing.core.self_healing_agent import SelfHealingAgent

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)

    with patch.object(agent, "_handle_critical", wraps=agent._handle_critical) as spy_critical, \
         patch.object(agent, "_evaluate_state"):

        event = _make_health_event(SystemEventType.AGENT_HEALTH_CRITICAL.value)
        agent.process_event(event)

    spy_critical.assert_called_once_with(event), (
        "process_event() did not call _handle_critical for an agent.health.critical event."
    )


# ---------------------------------------------------------------------------
# Stale-event guard: events predating agent start must be silently dropped
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_self_healing_drops_stale_health_events(mock_kafka_client):
    """
    Events with event_time < agent._start_time must be discarded before
    reaching _handle_degraded (prevents leftover events from a previous run
    from triggering spurious recovery actions on startup).
    """
    from self_healing.core.self_healing_agent import SelfHealingAgent
    from datetime import timedelta

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)
    # Manually set a start_time in the future relative to the event
    agent._start_time = datetime.utcnow() + timedelta(seconds=60)

    with patch.object(agent, "_handle_degraded") as spy:
        stale_event = _make_health_event(SystemEventType.AGENT_HEALTH_DEGRADED.value)
        agent.process_event(stale_event)

    spy.assert_not_called(), (
        "_handle_degraded was called for a stale event — the startup guard is not working."
    )


# ---------------------------------------------------------------------------
# State update: _handle_degraded must record last_degraded_at
# ---------------------------------------------------------------------------

@pytest.mark.unit
def test_handle_degraded_sets_last_degraded_at(mock_kafka_client):
    """
    After _handle_degraded processes an event the tracked AgentRecoveryState
    must have last_degraded_at set to a non-None value.
    """
    from self_healing.core.self_healing_agent import SelfHealingAgent

    agent = SelfHealingAgent(kafka_client=mock_kafka_client)

    with patch.object(agent, "_evaluate_state"):   # suppress recovery commands
        event = _make_health_event(SystemEventType.AGENT_HEALTH_DEGRADED.value, "TargetAgent")
        agent._handle_degraded(event)

    with agent._state_lock:
        state = agent._states.get("agent_TargetAgent")

    assert state is not None, "No state entry created for TargetAgent"
    assert state.last_degraded_at is not None, (
        "_handle_degraded did not update last_degraded_at on AgentRecoveryState"
    )
    assert state.status == "degraded", (
        f"Expected status='degraded', got '{state.status}'"
    )
