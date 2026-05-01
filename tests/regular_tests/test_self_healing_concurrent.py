"""
tests/test_self_healing_concurrent.py

Objective
---------
Empirically prove that the ``threading.RLock`` implementation inside
``SelfHealingAgent`` prevents thread deadlocks under high-concurrency
contention.

Background
----------
``SelfHealingAgent._state_lock`` is an ``RLock`` (re-entrant lock) because the
event-handling call chain re-acquires the lock on the same thread:

    _handle_degraded            ← acquires lock for state update
      └─ _evaluate_state        ← acquires lock for snapshot copy
           └─ _publish_restart  ← re-acquires lock for timestamp update

A plain ``threading.Lock`` would deadlock on the second acquisition within the
same thread.  An ``RLock`` allows the same thread to re-acquire without
blocking.

Test Plan
---------
For 10 outer repetitions (to catch intermittent race conditions):
  1. Create a ``threading.Barrier(10)`` to synchronise 10 threads.
  2. Spawn 10 ``threading.Thread`` instances, each calling
     ``SelfHealingAgent._handle_degraded()`` with a UNIQUE agent event.
  3. All threads wait on the barrier so they enter the critical section at
     the exact same moment.
  4. Use ``threading.Event(timeout=3.0)`` — if any thread fails to complete
     within 3 seconds, the test declares a deadlock and fails.
  5. Assert all threads joined successfully; assert no exceptions.

The 10 × 10 = 100 concurrent executions provide strong empirical evidence
that the RLock implementation is correct.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
import threading
from datetime import datetime, timedelta

import pytest
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

# Number of concurrent threads per round.
NUM_THREADS = 10

# Number of outer repetitions to catch intermittent races.
NUM_ROUNDS = 10

# Maximum wall-clock time (seconds) to wait for all threads to complete.
# If breached, we declare a deadlock.
DEADLOCK_TIMEOUT_S = 3.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_agent() -> SelfHealingAgent:
    """Create a SelfHealingAgent with a no-op Kafka client."""
    kafka = MagicMock()
    kafka.publish.return_value = None
    agent = SelfHealingAgent(kafka_client=kafka)
    # Backdate start_time so injected events aren't rejected as stale
    agent._start_time = datetime.utcnow() - timedelta(hours=1)
    return agent


def _make_degraded_event(thread_idx: int, round_idx: int) -> Event:
    """Build a unique degraded event for each thread × round combination.

    Each thread targets a DIFFERENT agent_id so they don't contend on the
    same ``AgentRecoveryState`` object (we want to stress the ``_state_lock``
    guarding the dictionary, not a single entry).
    """
    agent_id = f"agent_Thread{thread_idx}_Round{round_idx}"
    agent_name = f"Thread{thread_idx}_Round{round_idx}"

    return Event(
        event_id=f"evt_concurrent_{round_idx}_{thread_idx}",
        event_type=SystemEventType.AGENT_HEALTH_DEGRADED.value,
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=agent_id,
        schema_version="1.1",
        payload={
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_type": "WorkerAgent",
            "instance_id": f"inst_{agent_name}",
            "status": AgentStatus.DEGRADED.value,
            "metrics": {
                "cpu_percent": 50.0 + thread_idx,
                "memory_percent": 40.0 + thread_idx,
                "consumer_lag": 0,
                "error_count": 0,
                "latency_ms": 100.0 + thread_idx * 10,
                "events_per_second": 5.0,
            },
        },
        metadata={"environment": "test"},
    )


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestSelfHealingConcurrent:
    """Prove RLock prevents deadlocks under 10-thread concurrency."""

    def test_rlock_no_deadlock_10x10(self) -> None:
        """10 rounds × 10 threads: all must complete within 3 s each round.

        This test structure is deliberately repetitive (100 total concurrent
        invocations) to maximise the probability of triggering a deadlock
        if the lock implementation is incorrect.
        """
        for round_idx in range(NUM_ROUNDS):
            # Create a FRESH agent per round to avoid cooldown state leaking
            # between rounds.
            agent = _make_agent()

            # Barrier ensures all threads start at the same instant.
            barrier = threading.Barrier(NUM_THREADS, timeout=DEADLOCK_TIMEOUT_S)

            # Shared state for tracking thread outcomes.
            completion_event = threading.Event()
            exceptions: list = []
            completed_count = 0
            count_lock = threading.Lock()

            def _worker(tidx: int, ridx: int) -> None:
                """Thread target: wait on barrier, then call _handle_degraded."""
                nonlocal completed_count
                try:
                    # All threads block here until all NUM_THREADS arrive.
                    barrier.wait()

                    # Execute the critical section under contention.
                    event = _make_degraded_event(tidx, ridx)
                    agent._handle_degraded(event)

                except threading.BrokenBarrierError:
                    # Barrier broke (e.g. timeout) — record the failure.
                    exceptions.append(
                        RuntimeError(
                            f"Round {ridx}, Thread {tidx}: "
                            f"Barrier broke (likely timeout — deadlock?)"
                        )
                    )
                except Exception as e:
                    exceptions.append(e)
                finally:
                    with count_lock:
                        completed_count += 1
                        if completed_count == NUM_THREADS:
                            completion_event.set()

            # Spawn all threads.
            threads = []
            for tidx in range(NUM_THREADS):
                t = threading.Thread(
                    target=_worker,
                    args=(tidx, round_idx),
                    daemon=True,
                    name=f"TestWorker-{round_idx}-{tidx}",
                )
                threads.append(t)
                t.start()

            # Wait for all threads to finish (with timeout).
            all_finished = completion_event.wait(timeout=DEADLOCK_TIMEOUT_S)

            # ------ Assertions ------

            if not all_finished:
                # Identify which threads are still alive (potential deadlock).
                alive = [t.name for t in threads if t.is_alive()]
                pytest.fail(
                    f"DEADLOCK DETECTED in round {round_idx}: "
                    f"{len(alive)}/{NUM_THREADS} threads still alive after "
                    f"{DEADLOCK_TIMEOUT_S}s timeout.\n"
                    f"Stuck threads: {alive}"
                )

            # Join all threads (should be instant since completion_event fired).
            for t in threads:
                t.join(timeout=1.0)
                assert not t.is_alive(), (
                    f"Round {round_idx}: Thread {t.name} failed to join "
                    f"after completion_event was set."
                )

            # No exceptions should have been raised in any thread.
            assert not exceptions, (
                f"Round {round_idx}: {len(exceptions)} thread(s) raised "
                f"exceptions: {exceptions}"
            )

            # Verify all unique agent states were created.
            for tidx in range(NUM_THREADS):
                expected_id = f"agent_Thread{tidx}_Round{round_idx}"
                assert expected_id in agent._states, (
                    f"Round {round_idx}: State for {expected_id} not found; "
                    f"_get_or_create_state may have lost a write under contention."
                )

            logger.info(
                f"Round {round_idx + 1}/{NUM_ROUNDS}: "
                f"{NUM_THREADS} threads completed successfully"
            )

        logger.info(
            f"\n"
            f"╔══════════════════════════════════════════════════╗\n"
            f"║     RLock Concurrency Test PASSED                ║\n"
            f"║     {NUM_ROUNDS} rounds × {NUM_THREADS} threads = "
            f"{NUM_ROUNDS * NUM_THREADS} total invocations  ║\n"
            f"╚══════════════════════════════════════════════════╝"
        )
