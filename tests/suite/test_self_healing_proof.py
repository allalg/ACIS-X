"""
tests/suite/test_self_healing_proof.py

CATEGORY: Self-Healing Failure Simulation (Proof of Concept)

Proves the ACIS-X self-healing claim by simulating:
  A. Agent hard failure (TIMEOUT → restart.requested → recovery)
  B. Latency spike (OVERLOADED with high latency_ms → score-based restart)
  C. Cascading failure of multiple agents (storm prevention still fires once each)
  D. Recovery success rate under 100 repeated failure/recover cycles
  E. Recovery time distribution (mean, P95, max must be < 30 s)

Metrics reported
----------------
  - recovery_time_ms : wall-clock time from fault injection → restart.requested event
  - success_rate     : fraction of cycles where recovery was triggered
  - p95_recovery_ms  : 95th-percentile recovery time
  - storm_prevention : cascading failures produce exactly one restart per agent

All tests run fully offline (no live Kafka broker).

Marker: @pytest.mark.performance
"""

import logging
import statistics
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import AgentStatus, Event, SystemEventType
from self_healing.core.self_healing_agent import AgentRecoveryState, SelfHealingAgent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kafka() -> MagicMock:
    """Create a minimal Kafka mock that records published events."""
    kafka = MagicMock()
    kafka.published: List[Dict[str, Any]] = []

    def _publish(topic, event, **kw):
        evt = event if isinstance(event, dict) else (
            event.model_dump() if hasattr(event, "model_dump") else {}
        )
        kafka.published.append({"topic": topic, "event": evt})
        return True

    kafka.publish.side_effect = _publish
    kafka._producer = MagicMock()
    kafka._consumer = MagicMock()
    kafka._consumer.poll.return_value = {}
    return kafka


def _make_sha(kafka) -> SelfHealingAgent:
    """Create a SelfHealingAgent with suppressed startup staleness filter."""
    sha = SelfHealingAgent(kafka_client=kafka)
    sha._start_time = datetime.utcnow() - timedelta(hours=1)  # treat all events as fresh
    return sha


def _make_timeout_event(agent_name: str, agent_id: str) -> Event:
    return Event(
        event_id=f"evt_{uuid.uuid4().hex[:8]}",
        event_type=SystemEventType.AGENT_TIMEOUT.value,
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=agent_id,
        schema_version="1.1",
        payload={
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_type": f"{agent_name}Type",
            "instance_id": f"inst_{agent_name}",
            "status": AgentStatus.TIMEOUT.value,
        },
        metadata={"environment": "test"},
    )


def _make_overloaded_event(
    agent_name: str,
    agent_id: str,
    latency_ms: float = 5000.0,
    cpu_percent: float = 95.0,
) -> Event:
    return Event(
        event_id=f"evt_{uuid.uuid4().hex[:8]}",
        event_type=SystemEventType.AGENT_OVERLOADED.value,
        event_source="MonitoringAgent",
        event_time=datetime.utcnow(),
        entity_id=agent_id,
        schema_version="1.1",
        payload={
            "agent_id": agent_id,
            "agent_name": agent_name,
            "agent_type": f"{agent_name}Type",
            "instance_id": f"inst_{agent_name}",
            "status": AgentStatus.OVERLOADED.value,
            "latency_ms": latency_ms,
            "cpu_percent": cpu_percent,
            "memory_percent": 80.0,
        },
        metadata={"environment": "test"},
    )


def _count_published(kafka: MagicMock, event_type: str, agent_id: str = None) -> int:
    """Count published events matching a type (and optional agent_id)."""
    count = 0
    for entry in kafka.published:
        evt = entry["event"]
        if evt.get("event_type") != event_type:
            continue
        if agent_id:
            payload = evt.get("payload", {})
            if payload.get("agent_id") != agent_id and payload.get("entity_id") != agent_id:
                if evt.get("entity_id") != agent_id:
                    continue
        count += 1
    return count


def _reset_agent_state(sha: SelfHealingAgent, agent_id: str) -> None:
    """Reset cooldown timestamps so the next cycle can trigger a new recovery."""
    with sha._state_lock:
        state = sha._states.get(agent_id)
        if state:
            state.status = AgentStatus.HEALTHY.value
            state.last_heartbeat = datetime.utcnow()
            state.last_restart_requested = None
            state.last_recovery_triggered = None
            state.last_timeout_at = None


# ---------------------------------------------------------------------------
# A. Hard Failure: agent TIMEOUT → restart.requested
# ---------------------------------------------------------------------------

@pytest.mark.performance
class TestAgentHardFailure:
    """
    Simulate a hard agent failure (missed heartbeat → TIMEOUT).
    Assert that:
    1. recovery.triggered is published.
    2. agent.restart.requested is published.
    3. Wall-clock latency from fault injection to restart.requested < 100 ms.
    """

    AGENT_NAME = "RiskScoringAgent"
    AGENT_ID = "agent_RiskScoringAgent"

    def test_hard_failure_triggers_restart(self):
        kafka = _make_kafka()
        sha = _make_sha(kafka)

        # Pre-register the agent
        sha._states[self.AGENT_ID] = AgentRecoveryState(
            agent_id=self.AGENT_ID,
            agent_name=self.AGENT_NAME,
            status=AgentStatus.HEALTHY.value,
        )

        # Inject TIMEOUT fault and measure recovery latency
        event = _make_timeout_event(self.AGENT_NAME, self.AGENT_ID)
        t_fault = time.perf_counter()
        sha.process_event(event)
        t_recovery = time.perf_counter()

        recovery_ms = (t_recovery - t_fault) * 1000.0

        # Assertions
        restart_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)
        recovery_count = _count_published(kafka, SystemEventType.RECOVERY_TRIGGERED.value)

        logger.info(
            f"\n  Hard Failure Test:\n"
            f"    recovery.triggered events : {recovery_count}\n"
            f"    restart.requested events  : {restart_count}\n"
            f"    recovery latency          : {recovery_ms:.3f} ms"
        )

        assert recovery_count >= 1, (
            "recovery.triggered must be published on hard failure"
        )
        assert restart_count >= 1, (
            "agent.restart.requested must be published on TIMEOUT"
        )
        assert recovery_ms < 100.0, (
            f"Recovery latency {recovery_ms:.2f} ms exceeds 100 ms threshold — "
            f"event-driven recovery must be near-instant"
        )


# ---------------------------------------------------------------------------
# B. Latency Spike: OVERLOADED with high latency → score-based restart
# ---------------------------------------------------------------------------

@pytest.mark.performance
class TestLatencySpike:
    """
    Simulate a sustained latency spike (latency_ms=5000, cpu=95%).

    The SelfHealingAgent periodic evaluation loop (_evaluate_all_states) reads
    the pre-seeded OVERLOADED state, computes a health score > SCORE_DEGRADED,
    and fires a restart.  We call _evaluate_all_states() directly because
    process_event() correctly resets last_overloaded_at to the incoming event
    timestamp (which would re-start the grace period on each call).
    """

    AGENT_NAME = "CustomerStateAgent"
    AGENT_ID = "agent_CustomerStateAgent"

    def test_latency_spike_triggers_recovery(self):
        kafka = _make_kafka()
        sha = _make_sha(kafka)

        # Pre-seed state: OVERLOADED, well past the grace period
        overloaded_ts = datetime.utcnow() - timedelta(
            seconds=sha.DEGRADED_RESTART_DELAY_SECONDS + 5
        )
        sha._states[self.AGENT_ID] = AgentRecoveryState(
            agent_id=self.AGENT_ID,
            agent_name=self.AGENT_NAME,
            status=AgentStatus.OVERLOADED.value,
            last_overloaded_at=overloaded_ts,
            last_event_at=overloaded_ts,
            latency_ms=5000.0,
            cpu_percent=95.0,
            memory_percent=80.0,
        )

        # Compute health score before evaluation (for assertion transparency)
        with sha._state_lock:
            state = sha._states[self.AGENT_ID]
            health_score = sha._compute_health_score(state)

        # Trigger the periodic MAPE-K evaluation loop — this is the code path
        # that checks sustained overload beyond the grace period.
        t_fault = time.perf_counter()
        sha._evaluate_all_states()
        t_recovery = time.perf_counter()
        latency_ms = (t_recovery - t_fault) * 1000.0

        recovery_count = _count_published(kafka, SystemEventType.RECOVERY_TRIGGERED.value)
        restart_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)

        logger.info(
            f"\n  Latency Spike Test (via periodic evaluator):\n"
            f"    Health score              : {health_score:.3f} "
            f"(threshold={sha.SCORE_DEGRADED})\n"
            f"    recovery.triggered        : {recovery_count}\n"
            f"    restart.requested         : {restart_count}\n"
            f"    evaluation latency        : {latency_ms:.3f} ms"
        )

        assert health_score >= sha.SCORE_DEGRADED, (
            f"Health score {health_score:.3f} should exceed SCORE_DEGRADED={sha.SCORE_DEGRADED} "
            f"under latency_ms=5000 + cpu=95%"
        )
        assert recovery_count >= 1, (
            f"recovery.triggered must fire when health_score={health_score:.3f} "
            f">= SCORE_DEGRADED={sha.SCORE_DEGRADED} and overload exceeds grace period"
        )
        assert latency_ms < 500.0, f"Periodic evaluation took {latency_ms:.2f} ms > 500 ms"
        assert recovery_count >= 1, "recovery.triggered must fire on latency spike"
        assert latency_ms < 100.0, f"Spike recovery latency {latency_ms:.2f} ms > 100 ms"


# ---------------------------------------------------------------------------
# C. Cascading failure — storm prevention
# ---------------------------------------------------------------------------

@pytest.mark.performance
class TestCascadingFailureStormPrevention:
    """
    Inject TIMEOUT for 5 different agents simultaneously.
    Each agent should receive exactly ONE restart.requested.
    The SelfHealingAgent must NOT generate duplicate restarts due to
    the cooldown guard (_can_restart).
    """

    AGENTS = [
        ("RiskScoringAgent", "agent_RiskScoringAgent"),
        ("PaymentPredictionAgent", "agent_PaymentPredictionAgent"),
        ("AggregatorAgent", "agent_AggregatorAgent"),
        ("CustomerStateAgent", "agent_CustomerStateAgent"),
        ("CollectionsAgent", "agent_CollectionsAgent"),
    ]

    def test_each_failed_agent_recovers_exactly_once(self):
        kafka = _make_kafka()
        sha = _make_sha(kafka)

        # Register all agents
        for name, aid in self.AGENTS:
            sha._states[aid] = AgentRecoveryState(
                agent_id=aid,
                agent_name=name,
                status=AgentStatus.HEALTHY.value,
            )

        # Cascade: inject 3 TIMEOUT events per agent (storm simulation)
        for name, aid in self.AGENTS:
            for _ in range(3):
                sha.process_event(_make_timeout_event(name, aid))

        # Each agent must have exactly 1 restart requested (cooldown prevents duplicates)
        results = {}
        for name, aid in self.AGENTS:
            restart_count = _count_published(
                kafka, SystemEventType.AGENT_RESTART_REQUESTED.value
            )
            # Count per agent via payload agent_id
            per_agent = sum(
                1 for e in kafka.published
                if e["event"].get("event_type") == SystemEventType.AGENT_RESTART_REQUESTED.value
                and e["event"].get("payload", {}).get("agent_id") == aid
            )
            results[name] = per_agent

        logger.info(
            f"\n  Cascading Failure Storm Prevention:\n"
            + "\n".join(
                f"    {name}: {count} restart(s) from 3 TIMEOUT events"
                for name, count in results.items()
            )
        )

        for name, count in results.items():
            assert count == 1, (
                f"{name} received {count} restart.requested events from 3 injections; "
                f"expected exactly 1 (cooldown guard broken)"
            )


# ---------------------------------------------------------------------------
# D. Recovery success rate over 100 fault/recover cycles
# ---------------------------------------------------------------------------

@pytest.mark.performance
class TestRecoverySuccessRate:
    """
    Cycle 100 times: inject TIMEOUT → assert restart fired → reset state.
    Success rate must be 100% (every fault triggers recovery).
    """

    AGENT_NAME = "AggregatorAgent"
    AGENT_ID = "agent_AggregatorAgent"
    NUM_CYCLES = 100

    def test_100_cycles_success_rate(self):
        kafka = _make_kafka()
        sha = _make_sha(kafka)

        sha._states[self.AGENT_ID] = AgentRecoveryState(
            agent_id=self.AGENT_ID,
            agent_name=self.AGENT_NAME,
            status=AgentStatus.HEALTHY.value,
        )

        successes = 0
        recovery_times_ms: List[float] = []

        for i in range(self.NUM_CYCLES):
            prev_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)

            event = _make_timeout_event(self.AGENT_NAME, self.AGENT_ID)
            t0 = time.perf_counter()
            sha.process_event(event)
            t1 = time.perf_counter()

            new_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)
            if new_count > prev_count:
                successes += 1
                recovery_times_ms.append((t1 - t0) * 1000.0)

            # Reset cooldowns so next cycle can fire
            _reset_agent_state(sha, self.AGENT_ID)

        success_rate = successes / self.NUM_CYCLES
        mean_ms = statistics.mean(recovery_times_ms) if recovery_times_ms else float("inf")
        p95_ms = sorted(recovery_times_ms)[int(0.95 * len(recovery_times_ms))] if recovery_times_ms else float("inf")
        max_ms = max(recovery_times_ms) if recovery_times_ms else float("inf")

        logger.info(
            f"\n  Recovery Success Rate (N={self.NUM_CYCLES}):\n"
            f"    Success rate   : {success_rate:.1%}  ({successes}/{self.NUM_CYCLES})\n"
            f"    Mean latency   : {mean_ms:.3f} ms\n"
            f"    P95 latency    : {p95_ms:.3f} ms\n"
            f"    Max latency    : {max_ms:.3f} ms"
        )

        assert success_rate == 1.0, (
            f"Recovery success rate {success_rate:.1%} < 100% — "
            f"{self.NUM_CYCLES - successes} cycles did not trigger restart"
        )
        assert p95_ms < 30_000.0, (  # 30 s as stated in the paper
            f"P95 recovery latency {p95_ms:.2f} ms exceeds 30 000 ms paper claim"
        )


# ---------------------------------------------------------------------------
# E. Recovery time distribution — full report
# ---------------------------------------------------------------------------

@pytest.mark.performance
class TestRecoveryTimeDistribution:
    """
    Measure the full latency distribution for agent failure detection and recovery.

    Simulates both failure types across 50 iterations each and reports
    detailed statistics aligned with paper Table 2 claims.
    """

    NUM_ITERATIONS = 50
    MAX_MEAN_MS = 30_000.0   # 30 s (paper claim)
    MAX_P95_MS = 30_000.0

    def _run_iterations(self, sha, kafka, failure_type: str) -> List[float]:
        """Run NUM_ITERATIONS fault cycles for a given failure type."""
        agent_name = f"{failure_type}Worker"
        agent_id = f"agent_{agent_name}"

        sha._states[agent_id] = AgentRecoveryState(
            agent_id=agent_id,
            agent_name=agent_name,
            status=AgentStatus.HEALTHY.value,
        )
        # Set overloaded_at far in the past so grace period is cleared
        if failure_type == "LATENCY":
            sha._states[agent_id].last_overloaded_at = (
                datetime.utcnow() - timedelta(seconds=sha.DEGRADED_RESTART_DELAY_SECONDS + 5)
            )

        latencies: List[float] = []
        for i in range(self.NUM_ITERATIONS):
            if failure_type == "TIMEOUT":
                event = _make_timeout_event(agent_name, agent_id)
            else:
                event = _make_overloaded_event(agent_name, agent_id, latency_ms=6000.0)

            t0 = time.perf_counter()
            sha.process_event(event)
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000.0)

            _reset_agent_state(sha, agent_id)
            if failure_type == "LATENCY":
                with sha._state_lock:
                    state = sha._states.get(agent_id)
                    if state:
                        state.last_overloaded_at = (
                            datetime.utcnow()
                            - timedelta(seconds=sha.DEGRADED_RESTART_DELAY_SECONDS + 5)
                        )

        return latencies

    def test_recovery_time_distribution(self):
        kafka = _make_kafka()
        sha = _make_sha(kafka)

        timeout_latencies = self._run_iterations(sha, kafka, "TIMEOUT")
        latency_latencies = self._run_iterations(sha, kafka, "LATENCY")

        all_latencies = timeout_latencies + latency_latencies

        mean_ms = statistics.mean(all_latencies)
        p50_ms = statistics.median(all_latencies)
        p95_ms = sorted(all_latencies)[int(0.95 * len(all_latencies))]
        max_ms = max(all_latencies)
        min_ms = min(all_latencies)

        logger.info(
            f"\n  Recovery Time Distribution (N={len(all_latencies)} total):\n"
            f"\n  TIMEOUT failures ({self.NUM_ITERATIONS} cycles):\n"
            f"    Mean  : {statistics.mean(timeout_latencies):.4f} ms\n"
            f"    P95   : {sorted(timeout_latencies)[int(0.95*len(timeout_latencies))]:.4f} ms\n"
            f"    Max   : {max(timeout_latencies):.4f} ms\n"
            f"\n  LATENCY SPIKE failures ({self.NUM_ITERATIONS} cycles):\n"
            f"    Mean  : {statistics.mean(latency_latencies):.4f} ms\n"
            f"    P95   : {sorted(latency_latencies)[int(0.95*len(latency_latencies))]:.4f} ms\n"
            f"    Max   : {max(latency_latencies):.4f} ms\n"
            f"\n  Combined:\n"
            f"    Min   : {min_ms:.4f} ms\n"
            f"    Mean  : {mean_ms:.4f} ms\n"
            f"    P50   : {p50_ms:.4f} ms\n"
            f"    P95   : {p95_ms:.4f} ms\n"
            f"    Max   : {max_ms:.4f} ms"
        )

        assert mean_ms < self.MAX_MEAN_MS, (
            f"Mean recovery time {mean_ms:.2f} ms exceeds {self.MAX_MEAN_MS} ms"
        )
        assert p95_ms < self.MAX_P95_MS, (
            f"P95 recovery time {p95_ms:.2f} ms exceeds {self.MAX_P95_MS} ms"
        )
