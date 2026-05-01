"""
tests/test_unit_self_healing.py

Tests for SelfHealingAgent lock-safety: _handle_degraded -> _evaluate_state
-> _publish_restart all acquire self._state_lock.  Because _state_lock is an
RLock (re-entrant), the same thread can re-acquire it without deadlocking.
"""

import threading
import time
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from self_healing.core.self_healing_agent import (
    SelfHealingAgent,
    AgentRecoveryState,
    AgentStatus,
)
from schemas.event_schema import Event


DEADLOCK_TIMEOUT = 3.0   # seconds — exceeded → deadlock


def _make_agent() -> SelfHealingAgent:
    kc = MagicMock()
    kc.publish.return_value = None
    return SelfHealingAgent(kafka_client=kc)


def _make_degraded_event(
    agent_id: str = "agent_TestWorker",
    agent_name: str = "TestWorker",
) -> Event:
    return Event(
        event_id="evt_degraded_001",
        event_type="agent.health.degraded",
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=agent_id,
        schema_version="1.1",
        payload={
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_type": "WorkerAgent",
            "instance_id": f"inst_{agent_name}",
            "status": "DEGRADED",
            "cpu_percent": 85.0,
            "memory_percent": 70.0,
            "consumer_lag": 0,
            "error_count": 0,
        },
        metadata={"environment": "test"},
    )


class TestSelfHealingLockSafety:

    def test_handle_degraded_completes_without_deadlock(self):
        """Full chain _handle_degraded -> _evaluate_state -> _publish_restart
        must finish within DEADLOCK_TIMEOUT; a plain Lock would deadlock here."""
        agent = _make_agent()
        state = AgentRecoveryState(
            agent_id="agent_TestWorker",
            agent_name="TestWorker",
            status=AgentStatus.DEGRADED.value,
            last_degraded_at=datetime.utcnow() - timedelta(seconds=999),
        )
        agent._states["agent_TestWorker"] = state

        done = threading.Event()
        exc_box: list = []

        def _run():
            try:
                agent._handle_degraded(_make_degraded_event())
            except Exception as e:
                exc_box.append(e)
            finally:
                done.set()

        threading.Thread(target=_run, daemon=True).start()
        assert done.wait(DEADLOCK_TIMEOUT), (
            f"_handle_degraded deadlocked (>{DEADLOCK_TIMEOUT}s)"
        )
        assert not exc_box, f"_handle_degraded raised: {exc_box[0]}"

    def test_publish_restart_reacquires_rlock_while_held(self):
        """_publish_restart() re-acquires _state_lock (RLock) while a caller
        already holds it on the same thread — must not deadlock."""
        agent = _make_agent()
        state = AgentRecoveryState(
            agent_id="agent_Alpha",
            agent_name="Alpha",
            status=AgentStatus.ERROR.value,
        )
        agent._states["agent_Alpha"] = state

        done = threading.Event()
        exc_box: list = []

        def _run():
            try:
                with agent._state_lock:          # outer lock acquisition
                    agent._publish_restart(state, "test reason", "TEST_RULE")
            except Exception as e:
                exc_box.append(e)
            finally:
                done.set()

        threading.Thread(target=_run, daemon=True).start()
        assert done.wait(DEADLOCK_TIMEOUT), (
            f"_publish_restart deadlocked when called while _state_lock held "
            f"(>{DEADLOCK_TIMEOUT}s) — RLock re-entrance is broken"
        )
        assert not exc_box, f"Unexpected exception: {exc_box[0]}"

    def test_evaluate_state_sets_last_restart_requested(self):
        """After restart decision, last_restart_requested must be stamped."""
        agent = _make_agent()
        state = AgentRecoveryState(
            agent_id="agent_Beta",
            agent_name="Beta",
            status=AgentStatus.ERROR.value,
            last_event_at=datetime.utcnow(),
        )
        agent._states["agent_Beta"] = state

        agent._evaluate_state("agent_Beta", trigger="error")

        updated = agent._states.get("agent_Beta")
        assert updated is not None
        assert updated.last_restart_requested is not None, (
            "_publish_restart must set last_restart_requested"
        )

    def test_handle_degraded_within_grace_period_skips_restart(self):
        """Agent degraded < DEGRADED_RESTART_DELAY_SECONDS ago → no restart."""
        agent = _make_agent()
        state = AgentRecoveryState(
            agent_id="agent_Gamma",
            agent_name="Gamma",
            status=AgentStatus.DEGRADED.value,
            last_degraded_at=datetime.utcnow() - timedelta(seconds=1),
            last_event_at=datetime.utcnow(),
        )
        agent._states["agent_Gamma"] = state

        agent._handle_degraded(_make_degraded_event("agent_Gamma", "Gamma"))

        updated = agent._states["agent_Gamma"]
        assert updated.last_restart_requested is None, (
            "Restart triggered within grace period"
        )

    def test_concurrent_handle_degraded_does_not_deadlock(self):
        """Two threads firing _handle_degraded concurrently must both complete."""
        agent = _make_agent()

        done = [False, False]
        errors: list = []

        def _run(idx, eid, ename):
            try:
                agent._handle_degraded(_make_degraded_event(eid, ename))
                done[idx] = True
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=_run, args=(0, "agent_CA", "ConcurrentA"), daemon=True)
        t2 = threading.Thread(target=_run, args=(1, "agent_CB", "ConcurrentB"), daemon=True)
        t1.start(); t2.start()
        t1.join(DEADLOCK_TIMEOUT); t2.join(DEADLOCK_TIMEOUT)

        assert not t1.is_alive(), "Thread A deadlocked"
        assert not t2.is_alive(), "Thread B deadlocked"
        assert not errors, f"Exceptions: {errors}"
