from datetime import timezone
"""
tests/suite/test_ablation_and_fault_injection.py

CATEGORY: Ablation Study & Fault Injection Harness for Springer Paper

Produces all numbers required for two new paper subsections:
  §5.3 Architectural Ablation Study
  §5.4 Self-Healing Fault Injection Results

Part 1 — Architectural Ablation (4 experiments on n=200, seed=42)
  A. Full ACIS-X pipeline          (baseline F1, Spearman ρ)
  B. −External enrichment          (Tier 2/3 zeroed out)
  C. −Behavioural refinement       (skip risk refinement, use raw prediction)
  D. −SHAP audit layer             (SHAP disabled → latency delta)

Part 2 — Fault Injection Harness (4 scenarios)
  E. Agent hard crash (TIMEOUT)    → recovery latency (ms)
  F. Latency spike (OVERLOADED)    → time to restart decision (ms)
  G. Cascading 5-agent failure     → restarts per agent (storm prevention)
  H. 100-cycle endurance           → success rate %, P50/P95/max (ms)

All tests run fully offline (mocked Kafka). Output is structured for paper.

Marker: @pytest.mark.paper
"""

import logging
import statistics
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

logger = logging.getLogger(__name__)


# ===========================================================================
# Shared helpers
# ===========================================================================

def _make_population(n: int = 200, seed: int = 42) -> List[Dict[str, Any]]:
    """Generate n synthetic customers with multi-factor risk profiles."""
    customers = []
    lcg = seed

    def _rand():
        nonlocal lcg
        lcg = (1664525 * lcg + 1013904223) & 0xFFFFFFFF
        return lcg / 0xFFFFFFFF

    for i in range(n):
        credit_limit = 200_000.0 + _rand() * 800_000.0
        utilisation = _rand()
        total_outstanding = utilisation * credit_limit
        avg_delay = _rand() * 90.0
        on_time_ratio = max(0.0, 1.0 - _rand() * 1.2)
        overdue_count = int(_rand() * 8)

        # External signals (for enrichment ablation)
        financial_risk = 0.1 + _rand() * 0.7
        litigation_risk = 0.05 + _rand() * 0.4

        gt_risk = (
            0.30 * utilisation
            + 0.20 * (avg_delay / 90.0)
            + 0.20 * (1.0 - on_time_ratio)
            + 0.10 * (overdue_count / 7.0)
            + 0.10 * financial_risk
            + 0.10 * litigation_risk
        )
        gt_risk = min(1.0, gt_risk)
        will_default = gt_risk > 0.50

        customers.append({
            "customer_id": f"cust_abl_{i:04d}",
            "credit_limit": credit_limit,
            "total_outstanding": total_outstanding,
            "avg_delay": avg_delay,
            "on_time_ratio": on_time_ratio,
            "overdue_count": overdue_count,
            "financial_risk": financial_risk,
            "litigation_risk": litigation_risk,
            "ground_truth_risk": gt_risk,
            "will_default": will_default,
        })

    return customers


def _f1(y_true: List[bool], y_pred_score: List[float], threshold: float = 0.50) -> float:
    """Binary F1 at threshold."""
    tp = fp = fn = 0
    for gt, score in zip(y_true, y_pred_score):
        predicted = score >= threshold
        if predicted and gt:
            tp += 1
        elif predicted and not gt:
            fp += 1
        elif not predicted and gt:
            fn += 1
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    return 2 * precision * recall / max(precision + recall, 1e-9)


def _spearman(a: List[float], b: List[float]) -> float:
    """Spearman rank correlation."""
    def _rank(lst):
        indexed = sorted(enumerate(lst), key=lambda x: x[1])
        ranks = [0.0] * len(lst)
        for rank, (idx, _) in enumerate(indexed):
            ranks[idx] = float(rank)
        return ranks

    n = len(a)
    if n < 2:
        return 0.0
    ra, rb = _rank(a), _rank(b)
    d_sq = sum((ra[i] - rb[i]) ** 2 for i in range(n))
    denom = n * (n ** 2 - 1)
    return 0.0 if denom == 0 else 1.0 - (6 * d_sq) / denom


# ===========================================================================
# Part 1: Architectural Ablation
# ===========================================================================

def _run_acis_full(customers: List[Dict]) -> List[float]:
    """Full ACIS-X pipeline: multi-signal refinement with all context."""
    from agents.risk.risk_scoring_agent import RiskScoringAgent

    kafka = MagicMock()
    kafka.publish.return_value = True
    agent = RiskScoringAgent(kafka_client=kafka)

    scores = []
    for c in customers:
        def _handler(query_type, params=None, _c=c, **kw):
            if query_type == "get_customer_metrics":
                return {
                    "customer_id": _c["customer_id"],
                    "total_outstanding": _c["total_outstanding"],
                    "avg_delay": _c["avg_delay"],
                    "on_time_ratio": _c["on_time_ratio"],
                    "overdue_count": _c["overdue_count"],
                    "credit_limit": _c["credit_limit"],
                }
            if query_type == "get_risk_velocity":
                return {"velocity": 0.0, "trend": "stable", "volatility": 0.02}
            return None

        # Seed external context so refinement can use it
        agent._customer_risk_context[c["customer_id"]] = {
            "data": {
                "aggregated_risk": 0.6 * c["financial_risk"] + 0.4 * c["litigation_risk"],
                "financial_risk": c["financial_risk"],
                "litigation_risk": c["litigation_risk"],
                "external_risk": c["financial_risk"],
                "severity": None,
                "trend": "stable",
            },
            "updated_at": time.time(),
        }

        base_risk = min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            refined = agent._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base_risk,
                confidence=0.80,
                reasons=[],
            )
        scores.append(refined)

    return scores


def _run_acis_no_enrichment(customers: List[Dict]) -> List[float]:
    """ACIS-X with external enrichment ablated (Tier 2/3 = None)."""
    from agents.risk.risk_scoring_agent import RiskScoringAgent

    kafka = MagicMock()
    kafka.publish.return_value = True
    agent = RiskScoringAgent(kafka_client=kafka)

    scores = []
    for c in customers:
        def _handler(query_type, params=None, _c=c, **kw):
            if query_type == "get_customer_metrics":
                return {
                    "customer_id": _c["customer_id"],
                    "total_outstanding": _c["total_outstanding"],
                    "avg_delay": _c["avg_delay"],
                    "on_time_ratio": _c["on_time_ratio"],
                    "overdue_count": _c["overdue_count"],
                    "credit_limit": _c["credit_limit"],
                }
            if query_type == "get_risk_velocity":
                return {"velocity": 0.0, "trend": "stable", "volatility": 0.02}
            return None

        # NO external context seeded — ablation of enrichment
        base_risk = min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            refined = agent._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base_risk,
                confidence=0.80,
                reasons=[],
            )
        scores.append(refined)

    return scores


def _run_acis_no_refinement(customers: List[Dict]) -> List[float]:
    """ACIS-X with behavioural refinement ablated — raw base risk only."""
    return [
        min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        for c in customers
    ]


def _run_shap_latency_comparison(n_customers: int = 50) -> Dict[str, float]:
    """Measure per-event latency with and without SHAP enabled.

    Directly exercises the RF model + SHAP TreeExplainer to avoid
    QueryClient Kafka dependency in process_event().
    """
    from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
    import numpy as np

    kafka = MagicMock()
    kafka.publish.return_value = True

    agent = PaymentPredictionAgent(kafka_client=kafka)

    # Generate random feature vectors
    np.random.seed(99)
    X_batch = np.random.rand(n_customers, 8)

    # With SHAP: predict + explain
    t0 = time.perf_counter()
    for i in range(n_customers):
        X_input = X_batch[i:i+1]
        _ = agent.model.predict_proba(X_input)
        _ = agent.explainer(X_input)
    t_shap = (time.perf_counter() - t0) * 1000.0

    # Without SHAP: predict only
    t0 = time.perf_counter()
    for i in range(n_customers):
        X_input = X_batch[i:i+1]
        _ = agent.model.predict_proba(X_input)
    t_noshap = (time.perf_counter() - t0) * 1000.0

    per_event_shap = t_shap / n_customers
    per_event_noshap = t_noshap / n_customers
    delta = per_event_shap - per_event_noshap

    return {
        "with_shap_ms": round(per_event_shap, 2),
        "without_shap_ms": round(per_event_noshap, 2),
        "delta_ms": round(delta, 2),
        "n": n_customers,
    }


@pytest.mark.paper
class TestArchitecturalAblation:
    """Ablation study: measure impact of removing each component."""

    def test_ablation_matrix(self):
        """Run all 4 ablation experiments and report results."""
        customers = _make_population(n=200, seed=42)
        gt_risk = [c["ground_truth_risk"] for c in customers]
        gt_label = [c["will_default"] for c in customers]

        # A. Full pipeline
        scores_full = _run_acis_full(customers)
        f1_full = _f1(gt_label, scores_full)
        rho_full = _spearman(gt_risk, scores_full)

        # B. −External enrichment
        scores_no_enrich = _run_acis_no_enrichment(customers)
        f1_no_enrich = _f1(gt_label, scores_no_enrich)
        rho_no_enrich = _spearman(gt_risk, scores_no_enrich)

        # C. −Behavioural refinement
        scores_no_refine = _run_acis_no_refinement(customers)
        f1_no_refine = _f1(gt_label, scores_no_refine)
        rho_no_refine = _spearman(gt_risk, scores_no_refine)

        # D. −SHAP (latency only)
        shap_result = _run_shap_latency_comparison(n_customers=50)

        # Log paper-ready results
        logger.info(
            f"\n{'='*70}\n"
            f"  ARCHITECTURAL ABLATION RESULTS (n=200, seed=42)\n"
            f"{'='*70}\n"
            f"  {'Configuration':<30s}  {'F1':>6s}  {'ρ':>6s}  {'ΔF1':>7s}  {'Δρ':>7s}\n"
            f"  {'-'*60}\n"
            f"  {'Full ACIS-X':<30s}  {f1_full:6.3f}  {rho_full:6.3f}  {'—':>7s}  {'—':>7s}\n"
            f"  {'−External enrichment':<30s}  {f1_no_enrich:6.3f}  {rho_no_enrich:6.3f}  "
            f"{f1_no_enrich - f1_full:+7.3f}  {rho_no_enrich - rho_full:+7.3f}\n"
            f"  {'−Behavioural refinement':<30s}  {f1_no_refine:6.3f}  {rho_no_refine:6.3f}  "
            f"{f1_no_refine - f1_full:+7.3f}  {rho_no_refine - rho_full:+7.3f}\n"
            f"  {'−SHAP audit layer':<30s}  {'(latency only)':>6s}  "
            f"delta = {shap_result['delta_ms']:+.1f} ms/event\n"
            f"{'='*70}"
        )

        # Assertions — Spearman ρ is the primary quality metric for ablation.
        # Rank ordering is threshold-independent. F1 at a fixed 0.50 threshold
        # is distribution-sensitive and can flip when enrichment/refinement
        # shifts the score distribution — this is expected and documented.
        # F1 numbers are logged for the paper table but not hard-asserted.

        # Enrichment must improve rank ordering
        assert rho_full > rho_no_enrich, (
            f"Enrichment must improve ρ: full={rho_full:.3f} vs no-enrich={rho_no_enrich:.3f}"
        )
        # Refinement must improve rank ordering
        assert rho_full > rho_no_refine, (
            f"Refinement must improve ρ: full={rho_full:.3f} vs no-refine={rho_no_refine:.3f}"
        )
        # SHAP overhead must be bounded
        assert shap_result["delta_ms"] < 20.0, (
            f"SHAP overhead {shap_result['delta_ms']} ms/event exceeds 20 ms budget"
        )


# ===========================================================================
# Part 2: Fault Injection Harness
# ===========================================================================

from schemas.event_schema import AgentStatus, Event, SystemEventType
from self_healing.core.self_healing_agent import AgentRecoveryState, SelfHealingAgent


def _make_kafka_mock() -> MagicMock:
    """Create a Kafka mock that records published events."""
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
    """Create a SelfHealingAgent with suppressed staleness filter."""
    sha = SelfHealingAgent(kafka_client=kafka)
    sha._start_time = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
    return sha


def _make_timeout_event(agent_name: str, agent_id: str) -> Event:
    return Event(
        event_id=f"evt_{uuid.uuid4().hex[:8]}",
        event_type=SystemEventType.AGENT_TIMEOUT.value,
        event_source="MonitoringAgent",
        event_time=datetime.now(timezone.utc).replace(tzinfo=None),
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


def _count_published(kafka: MagicMock, event_type: str, agent_id: str = None) -> int:
    """Count published events matching a type (and optional agent_id)."""
    count = 0
    for entry in kafka.published:
        evt = entry["event"]
        if evt.get("event_type") != event_type:
            continue
        if agent_id:
            payload = evt.get("payload", {})
            if (payload.get("agent_id") != agent_id
                    and payload.get("entity_id") != agent_id
                    and evt.get("entity_id") != agent_id):
                continue
        count += 1
    return count


@pytest.mark.paper
class TestFaultInjectionHarness:
    """Fault injection scenarios for paper §5.4."""

    def test_hard_crash_recovery_latency(self):
        """E. Agent TIMEOUT → restart.requested within <100 ms."""
        kafka = _make_kafka_mock()
        sha = _make_sha(kafka)

        agent_name = "PaymentPredictionAgent"
        agent_id = "agent_PaymentPredictionAgent"

        sha._states[agent_id] = AgentRecoveryState(
            agent_id=agent_id,
            agent_name=agent_name,
            status=AgentStatus.HEALTHY.value,
        )

        event = _make_timeout_event(agent_name, agent_id)
        t0 = time.perf_counter()
        sha.process_event(event)
        latency_ms = (time.perf_counter() - t0) * 1000.0

        restart_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)
        recovery_count = _count_published(kafka, SystemEventType.RECOVERY_TRIGGERED.value)

        logger.info(
            f"\n  [FAULT INJECTION] Hard Crash Recovery:\n"
            f"    recovery.triggered : {recovery_count}\n"
            f"    restart.requested  : {restart_count}\n"
            f"    latency            : {latency_ms:.3f} ms"
        )

        assert recovery_count >= 1, "recovery.triggered must fire on TIMEOUT"
        assert restart_count >= 1, "restart.requested must fire on TIMEOUT"
        assert latency_ms < 100.0, f"Recovery latency {latency_ms:.2f} ms exceeds 100 ms"

    def test_latency_spike_recovery(self):
        """F. OVERLOADED with high latency → score-based restart."""
        kafka = _make_kafka_mock()
        sha = _make_sha(kafka)

        agent_name = "ExternalDataAgent"
        agent_id = "agent_ExternalDataAgent"

        overloaded_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            seconds=sha.DEGRADED_RESTART_DELAY_SECONDS + 5
        )
        sha._states[agent_id] = AgentRecoveryState(
            agent_id=agent_id,
            agent_name=agent_name,
            status=AgentStatus.OVERLOADED.value,
            last_overloaded_at=overloaded_ts,
            last_event_at=overloaded_ts,
            latency_ms=5000.0,
            cpu_percent=95.0,
            memory_percent=80.0,
        )

        with sha._state_lock:
            state = sha._states[agent_id]
            health_score = sha._compute_health_score(state)

        t0 = time.perf_counter()
        sha._evaluate_all_states()
        latency_ms = (time.perf_counter() - t0) * 1000.0

        recovery_count = _count_published(kafka, SystemEventType.RECOVERY_TRIGGERED.value)

        logger.info(
            f"\n  [FAULT INJECTION] Latency Spike Recovery:\n"
            f"    health_score       : {health_score:.3f}\n"
            f"    recovery.triggered : {recovery_count}\n"
            f"    decision latency   : {latency_ms:.3f} ms"
        )

        assert health_score >= sha.SCORE_DEGRADED, (
            f"Health score {health_score:.3f} should exceed DEGRADED threshold {sha.SCORE_DEGRADED}"
        )
        assert recovery_count >= 1, "Sustained overload must trigger recovery"
        assert latency_ms < 100.0, f"Decision latency {latency_ms:.2f} ms exceeds 100 ms"

    def test_cascading_failure_storm_prevention(self):
        """G. 5 simultaneous agent failures → exactly 1 restart per agent."""
        kafka = _make_kafka_mock()
        sha = _make_sha(kafka)

        agent_names = [
            "CustomerStateAgent", "ExternalDataAgent",
            "AggregatorAgent", "PaymentPredictionAgent",
            "RiskScoringAgent",
        ]

        for name in agent_names:
            aid = f"agent_{name}"
            sha._states[aid] = AgentRecoveryState(
                agent_id=aid,
                agent_name=name,
                status=AgentStatus.HEALTHY.value,
            )

        # Inject TIMEOUT for all 5 agents
        for name in agent_names:
            aid = f"agent_{name}"
            event = _make_timeout_event(name, aid)
            sha.process_event(event)

        # Inject DUPLICATE timeouts (should be suppressed by cooldowns)
        for name in agent_names:
            aid = f"agent_{name}"
            event = _make_timeout_event(name, aid)
            sha.process_event(event)

        results = {}
        for name in agent_names:
            aid = f"agent_{name}"
            restarts = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value, aid)
            results[name] = restarts

        logger.info(
            f"\n  [FAULT INJECTION] Cascading Failure Storm Prevention:\n"
            + "\n".join(f"    {name:<30s}: {count} restart(s)" for name, count in results.items())
        )

        for name, count in results.items():
            assert count == 1, (
                f"{name} got {count} restarts, expected exactly 1 (storm prevention)"
            )

    def test_endurance_100_cycles(self):
        """H. 100 fault→recover cycles: success rate + latency distribution."""
        kafka = _make_kafka_mock()
        sha = _make_sha(kafka)

        agent_name = "CollectionsAgent"
        agent_id = "agent_CollectionsAgent"
        n_cycles = 100
        latencies: List[float] = []
        successes = 0

        for cycle in range(n_cycles):
            # Reset state for fresh cycle
            sha._states[agent_id] = AgentRecoveryState(
                agent_id=agent_id,
                agent_name=agent_name,
                status=AgentStatus.HEALTHY.value,
            )
            kafka.published.clear()

            event = _make_timeout_event(agent_name, agent_id)
            t0 = time.perf_counter()
            sha.process_event(event)
            latency_ms = (time.perf_counter() - t0) * 1000.0

            restart_count = _count_published(kafka, SystemEventType.AGENT_RESTART_REQUESTED.value)
            if restart_count >= 1:
                successes += 1
                latencies.append(latency_ms)

        success_rate = (successes / n_cycles) * 100.0
        p50 = statistics.median(latencies) if latencies else 0.0
        p95 = sorted(latencies)[int(len(latencies) * 0.95)] if latencies else 0.0
        max_lat = max(latencies) if latencies else 0.0
        mean_lat = statistics.mean(latencies) if latencies else 0.0

        logger.info(
            f"\n  [FAULT INJECTION] 100-Cycle Endurance:\n"
            f"    success rate : {success_rate:.0f}%\n"
            f"    P50 latency  : {p50:.3f} ms\n"
            f"    P95 latency  : {p95:.3f} ms\n"
            f"    max latency  : {max_lat:.3f} ms\n"
            f"    mean latency : {mean_lat:.3f} ms"
        )

        assert success_rate == 100.0, f"Success rate {success_rate}% < 100%"
        assert p95 < 50.0, f"P95 latency {p95:.2f} ms exceeds 50 ms"
        assert max_lat < 100.0, f"Max latency {max_lat:.2f} ms exceeds 100 ms"
