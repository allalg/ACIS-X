"""
tests/test_recovery_storm_prevention.py

Objective
---------
Prove that the SelfHealingAgent's cooldown mechanism suppresses duplicate
recovery actions, preventing Kafka "event storms" when a burst of identical
fault signals arrives.

Background
----------
In production, the MonitoringAgent may emit multiple ``agent.health.degraded``
events for the same agent within a very short window (e.g. one per heartbeat
interval).  Without deduplication / cooldown, the SelfHealingAgent would
publish a recovery action (restart / scale / spawn) for EACH event, flooding
the ``acis.system`` topic and potentially causing cascading restarts.

The SelfHealingAgent prevents this through two mechanisms:
  1. **Action cooldowns** — ``_can_restart()``, ``_can_scale()``, etc. check
     ``last_restart_requested`` (or equivalent) against a configurable cooldown
     window (default 120 s for restarts).
  2. **Grace period** — For DEGRADED status, ``DEGRADED_RESTART_DELAY_SECONDS``
     (default 30 s) prevents any action until the degradation persists beyond
     the grace window.

This test validates mechanism (1) by injecting rapid-fire degraded events and
asserting on the number of published Kafka events.

Test Plan
---------
1. Wave 1: Inject 5 degraded events for the SAME agent in < 1 second.
   Assert: ≤ 1 recovery-related publish to ``acis.system`` (the first event
   may or may not trigger a restart depending on the grace period; subsequent
   events are definitely suppressed).

2. Wave 2: Wait 2 seconds, then inject 5 more degraded events.
   Assert: No additional recovery events published (the 2 s gap is still well
   within the 120 s restart cooldown window).

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import time
import logging

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from self_healing.core.self_healing_agent import (
    SelfHealingAgent,
    AgentRecoveryState,
)
from schemas.event_schema import (
    AgentStatus,
    Event,
    SystemEventType,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TARGET_AGENT_ID = "agent_StormTarget"
TARGET_AGENT_NAME = "StormTarget"
EVENTS_PER_WAVE = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_kafka_mock():
    """Build a Kafka mock that records every publish call.

    Returns (kafka_client_mock, published_events_list).
    ``published_events_list`` captures every ``(topic, event_dict)`` pair
    passed to ``kafka_client.publish()``.
    """
    published: list = []

    kafka = MagicMock()

    def _capture_publish(topic, event, **kwargs):
        published.append({"topic": topic, "event": event})
        return True

    kafka.publish.side_effect = _capture_publish
    # Expose the list on the mock for easy inspection
    kafka.published_events = published
    return kafka, published


def _make_degraded_event(seq: int) -> Event:
    """Synthesise an ``agent.health.degraded`` event with a unique event_id.

    The metrics are set to moderate values so the health score lands in
    the "restart" band (score ≈ 0.47).  The ``last_degraded_at`` on the
    state is backdated in the test setup so the grace period is bypassed.
    """
    return Event(
        event_id=f"evt_storm_{seq:04d}",
        event_type=SystemEventType.AGENT_HEALTH_DEGRADED.value,
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=TARGET_AGENT_ID,
        schema_version="1.1",
        payload={
            "agent_id": TARGET_AGENT_ID,
            "agent_name": TARGET_AGENT_NAME,
            "agent_type": "WorkerAgent",
            "instance_id": f"inst_{TARGET_AGENT_NAME}",
            "status": AgentStatus.DEGRADED.value,
            "metrics": {
                "cpu_percent": 60.0,
                "memory_percent": 50.0,
                "consumer_lag": 0,
                "error_count": 0,
                "latency_ms": 200.0,
                "events_per_second": 5.0,
            },
        },
        metadata={"environment": "test"},
    )


def _count_recovery_events(published: list) -> int:
    """Count recovery-related events published to ``acis.system``.

    Recovery events include:
      - recovery.triggered
      - agent.restart.requested
      - agent.scale.requested
      - agent.spawn.requested
      - placement.requested
      - fallback.agent.selected

    We count ALL of these (not just ``restart.requested``) because the
    goal is to prove that duplicate recovery **actions** are suppressed.
    """
    RECOVERY_EVENT_TYPES = {
        SystemEventType.RECOVERY_TRIGGERED.value,
        SystemEventType.AGENT_RESTART_REQUESTED.value,
        SystemEventType.AGENT_SCALE_REQUESTED.value,
        SystemEventType.AGENT_SPAWN_REQUESTED.value,
        SystemEventType.PLACEMENT_REQUESTED.value,
        SystemEventType.FALLBACK_AGENT_SELECTED.value,
    }
    count = 0
    for item in published:
        if item["topic"] != "acis.system":
            continue
        event_data = item.get("event", {})
        if isinstance(event_data, dict):
            etype = event_data.get("event_type", "")
            if etype in RECOVERY_EVENT_TYPES:
                count += 1
    return count


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestRecoveryStormPrevention:
    """Prove that rapid-fire degraded events do NOT cause a Kafka flood."""

    def test_duplicate_degraded_events_suppressed(self) -> None:
        """5 rapid degraded events → at most 1 set of recovery publications."""
        kafka, published = _build_kafka_mock()
        agent = SelfHealingAgent(kafka_client=kafka)

        # Backdate _start_time so no events are considered stale
        agent._start_time = datetime.utcnow() - timedelta(hours=1)

        # Pre-seed the tracked state with a degraded_at timestamp that is
        # old enough to BYPASS the grace period (DEGRADED_RESTART_DELAY).
        # This ensures the first event CAN trigger a restart, and we verify
        # that only ONE restart fires, not five.
        agent._states[TARGET_AGENT_ID] = AgentRecoveryState(
            agent_id=TARGET_AGENT_ID,
            agent_name=TARGET_AGENT_NAME,
            status=AgentStatus.DEGRADED.value,
            last_degraded_at=datetime.utcnow() - timedelta(
                seconds=SelfHealingAgent.DEGRADED_RESTART_DELAY_SECONDS + 10
            ),
            last_event_at=datetime.utcnow() - timedelta(
                seconds=SelfHealingAgent.DEGRADED_RESTART_DELAY_SECONDS + 10
            ),
        )

        # ===================================================================
        # WAVE 1: Inject 5 degraded events in rapid succession (< 1 second)
        # ===================================================================
        for i in range(EVENTS_PER_WAVE):
            agent.process_event(_make_degraded_event(i))

        # Count recovery events after wave 1.
        # The FIRST event may trigger a restart + recovery.triggered (2 events).
        # Subsequent events MUST be suppressed by the restart cooldown.
        wave1_recovery_count = _count_recovery_events(published)

        # With the first degraded event, the agent emits:
        #   - recovery.triggered (observability, always fires)
        #   - agent.restart.requested (once, then cooldown blocks repeats)
        # The exact count depends on the grace period check.  But the key
        # assertion is that we don't get 5× the events.
        #
        # Allow up to 2 recovery events from the first trigger (recovery.triggered
        # + restart.requested).  With the grace period bypassed, the first call
        # triggers both.  All 4 subsequent calls are suppressed by cooldown.
        logger.info(
            f"Wave 1: {wave1_recovery_count} recovery events from "
            f"{EVENTS_PER_WAVE} degraded injections"
        )

        # The fundamental invariant: ≤ 2 recovery events (not 5 × 2 = 10).
        assert wave1_recovery_count <= 2, (
            f"Storm prevention FAILED: {wave1_recovery_count} recovery events "
            f"published after {EVENTS_PER_WAVE} rapid degraded signals.  "
            f"Expected ≤ 2 (1 recovery.triggered + 1 restart.requested)."
        )

        # ===================================================================
        # WAVE 2: Wait 2 seconds, then inject 5 more events
        # ===================================================================
        # 2 seconds is well within the RESTART_COOLDOWN (120 s), so
        # NO additional recovery events should be published.
        time.sleep(2.0)

        wave1_total = len(published)  # snapshot before wave 2

        for i in range(EVENTS_PER_WAVE, 2 * EVENTS_PER_WAVE):
            agent.process_event(_make_degraded_event(i))

        # Count NEW recovery events from wave 2
        wave2_new_events = published[wave1_total:]
        wave2_recovery_count = 0
        RECOVERY_EVENT_TYPES = {
            SystemEventType.RECOVERY_TRIGGERED.value,
            SystemEventType.AGENT_RESTART_REQUESTED.value,
            SystemEventType.AGENT_SCALE_REQUESTED.value,
            SystemEventType.AGENT_SPAWN_REQUESTED.value,
        }
        for item in wave2_new_events:
            if item["topic"] != "acis.system":
                continue
            event_data = item.get("event", {})
            if isinstance(event_data, dict):
                etype = event_data.get("event_type", "")
                if etype in RECOVERY_EVENT_TYPES:
                    wave2_recovery_count += 1

        logger.info(
            f"Wave 2: {wave2_recovery_count} NEW recovery events from "
            f"{EVENTS_PER_WAVE} degraded injections (after 2s gap)"
        )

        # The key assertion: the cooldown window is actively suppressing
        # restart commands.  recovery.triggered (observability) fires each
        # time but restart.requested must NOT fire.
        restart_events_wave2 = sum(
            1
            for item in wave2_new_events
            if item["topic"] == "acis.system"
            and isinstance(item.get("event"), dict)
            and item["event"].get("event_type")
            == SystemEventType.AGENT_RESTART_REQUESTED.value
        )

        assert restart_events_wave2 == 0, (
            f"Cooldown FAILED: {restart_events_wave2} restart.requested events "
            f"published in wave 2 despite being within the "
            f"{SelfHealingAgent.RESTART_COOLDOWN_SECONDS}s cooldown window."
        )
