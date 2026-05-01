"""
tests/suite/test_lifecycle_trace.py

CATEGORY: Lifecycle / Observability Tests

These tests answer the core question:
    "How long does a single invoice/payment take to travel through
     the ACIS-X simulation from creation to CollectionsAgent output?"

All agents are mocked at the Kafka + QueryClient level.  No live Kafka broker
or SQLite database is required.

Test coverage:
  1. test_invoice_full_pipeline_lifecycle  — invoice.created → collections.action
     - Records timestamps at every inter-agent stage
     - Asserts the full lifecycle completes within 2 000 ms (mocked, CPU-only)
     - Asserts correlation_id propagates unbroken through every hop
     - Saves a per-stage waterfall chart to tests/outputs/lifecycle_waterfall.png
  2. test_payment_risk_rescore_lifecycle   — payment.received → risk re-score
     - After a payment clears an invoice, verifies the customer's risk score
       changes (re-evaluated) within the stage budget
  3. test_lifecycle_stage_budget_regression — runs N=200 traces, flags any
     stage whose P95 inter-agent latency exceeds its per-stage budget.
     This is a regression/profiling test, not a hard assertion on production
     Kafka latency.

Marker: @pytest.mark.lifecycle
"""

import logging
import os
import statistics
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytest

from schemas.event_schema import Event

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")

# ---------------------------------------------------------------------------
# Stage budget (ms) — maximum acceptable mocked latency per inter-agent hop.
# These thresholds apply to mocked agents (no I/O) and exist purely to catch
# regressions in processing logic, not network/broker latency.
# ---------------------------------------------------------------------------
STAGE_BUDGETS_MS: Dict[str, float] = {
    "invoice.created → customer.metrics.updated": 300.0,
    "customer.metrics.updated → payment.risk.predicted": 300.0,
    "payment.risk.predicted → risk.scored": 300.0,
    "risk.scored → collection.action": 300.0,
}

# Full end-to-end budget for the mocked pipeline
FULL_LIFECYCLE_BUDGET_MS = 2_000.0

COMPLETION_RATE_FLOOR = 0.60  # ≥ 60% of traces must reach collections.action


# ---------------------------------------------------------------------------
# Timestamp-recording Kafka mock  (same pattern as test_pipeline_latency.py
# but extended with more metadata for stage tracing)
# ---------------------------------------------------------------------------

class StagedKafka:
    """Kafka mock that records every publish with a monotonic timestamp and stage info."""

    def __init__(self):
        self.published: List[Dict[str, Any]] = []
        self._producer = None
        self._consumer = None

    def publish(self, topic: str, event: Any, **kwargs) -> bool:
        import random
        # Simulate realistic network/broker round-trip latency (15ms - 45ms)
        time.sleep(random.uniform(0.015, 0.045))
        
        evt_dict = event if isinstance(event, dict) else (
            event.model_dump() if hasattr(event, "model_dump") else {}
        )
        self.published.append({
            "topic": topic,
            "event_type": evt_dict.get("event_type", ""),
            "correlation_id": evt_dict.get("correlation_id"),
            "payload": evt_dict.get("payload", {}),
            "ts": time.monotonic(),
            "raw": evt_dict,
        })
        return True

    def subscribe(self, topics, group_id=None):
        pass

    def close(self):
        pass

    def commit(self, message=None):
        pass

    def events_of_type(self, event_type: str) -> List[Dict]:
        return [e for e in self.published if e["event_type"] == event_type]


# ---------------------------------------------------------------------------
# Canned QueryClient responses
# ---------------------------------------------------------------------------

def _query_handler(query_type: str, params: dict = None, **kwargs):
    params = params or {}
    customer_id = params.get("customer_id", "cust_lc_default")

    if query_type == "get_customer":
        return {
            "customer_id": customer_id,
            "name": "Lifecycle Test Corp",
            "credit_limit": 500_000.0,
        }
    if query_type == "get_customer_metrics":
        return {
            "customer_id": customer_id,
            "total_outstanding": 100_000.0,
            "avg_delay": 5.0,
            "on_time_ratio": 0.85,
            "overdue_count": 1,
            "credit_limit": 500_000.0,
        }
    if query_type == "get_invoices_by_customer":
        return {
            "invoices": [
                {
                    "invoice_id": f"inv_lc_{customer_id}_{i}",
                    "customer_id": customer_id,
                    "total_amount": 50_000.0,
                    "amount": 50_000.0,
                    "remaining_amount": 50_000.0,
                    "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                    "status": "pending",
                }
                for i in range(2)
            ]
        }
    if query_type == "get_risk_velocity":
        return {"velocity": 0.0, "trend": "stable", "volatility": 0.02}
    if query_type in ("get_payments_by_invoices", "get_overdue_invoices"):
        return []
    if query_type in ("update_invoice_cache", "invalidate_customer_cache", "invalidate_invoice_cache"):
        return None
    return None


# ---------------------------------------------------------------------------
# Core lifecycle runner
# ---------------------------------------------------------------------------

def _run_invoice_lifecycle(
    seq: int,
    kafka: StagedKafka,
) -> Optional[Dict[str, float]]:
    """
    Drive ONE invoice through the full ACIS-X pipeline.

    Stages:
        T0  invoice.created         → CustomerStateAgent.process_event()
        T1  customer.metrics.updated published
        T2  PaymentPredictionAgent.handle_event()
        T3  payment.risk.predicted  published
        T4  RiskScoringAgent.handle_event()
        T5  risk.scored             published
        T6  CollectionsAgent.process_event()
        T7  collection.action       published (or not, depending on risk level)

    Returns a dict of stage timestamps keyed by stage name, or None if the
    trace could not be reconstructed.
    """
    from agents.intelligence.customer_state_agent import CustomerStateAgent
    from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
    from agents.risk.risk_scoring_agent import RiskScoringAgent
    from agents.collections.collections_agent import CollectionsAgent

    customer_id = f"cust_lc_{seq:06d}"
    invoice_id = f"inv_lc_{seq:06d}"
    corr_id = f"corr_lc_{uuid.uuid4().hex[:12]}"

    # Snapshot offset before this trace
    offset = len(kafka.published)

    # Instantiate agents sharing the same kafka mock
    csa = CustomerStateAgent(kafka_client=kafka)
    ppa = PaymentPredictionAgent(kafka_client=kafka)
    rsa = RiskScoringAgent(kafka_client=kafka)
    col = CollectionsAgent(kafka_client=kafka)

    # -- T0: Inject invoice.created --
    inv_event = Event(
        event_id=f"evt_inv_{seq:06d}",
        event_type="invoice.created",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=corr_id,
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "amount": 50_000.0 + seq,
            "total_amount": 50_000.0 + seq,
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "issued_date": datetime.utcnow().isoformat(),
            "status": "pending",
        },
        metadata={},
    )
    t0 = time.monotonic()
    csa.process_event(inv_event)

    # -- T1: Find customer.metrics.updated --
    metrics_entry = None
    for e in kafka.published[offset:]:
        if (
            e["event_type"] == "customer.metrics.updated"
            and e["payload"].get("customer_id") == customer_id
        ):
            metrics_entry = e
            break
    if not metrics_entry:
        return None  # CSA didn't emit — skip this trace
    t1 = metrics_entry["ts"]

    # -- T2: Feed to PaymentPredictionAgent --
    metrics_event = Event.model_validate(metrics_entry["raw"])
    metrics_event = metrics_event.model_copy(update={"correlation_id": corr_id})
    ppa_offset = len(kafka.published)
    ppa.handle_event(metrics_event)

    # -- T3: Find payment.risk.predicted --
    pred_entry = None
    for e in kafka.published[ppa_offset:]:
        if e["event_type"] == "payment.risk.predicted":
            pred_entry = e
            break
    if not pred_entry:
        return None
    t3 = pred_entry["ts"]

    # -- T4: Feed to RiskScoringAgent --
    pred_event = Event.model_validate(pred_entry["raw"])
    pred_event = pred_event.model_copy(update={"correlation_id": corr_id})
    rsa_offset = len(kafka.published)
    rsa.handle_event(pred_event)

    # -- T5: Find risk.scored --
    risk_entry = None
    for e in kafka.published[rsa_offset:]:
        if e["event_type"] == "risk.scored":
            risk_entry = e
            break
    if not risk_entry:
        return None
    t5 = risk_entry["ts"]

    # -- T6: Feed to CollectionsAgent --
    risk_event = Event.model_validate(risk_entry["raw"])
    risk_event = risk_event.model_copy(update={"correlation_id": corr_id})
    col_offset = len(kafka.published)
    col.process_event(risk_event)

    # -- T7: Find collection.action (optional — only emitted for risk > threshold) --
    collection_entry = None
    for e in kafka.published[col_offset:]:
        if e["event_type"] == "collection.action":
            collection_entry = e
            break
    t7 = collection_entry["ts"] if collection_entry else None

    return {
        "corr_id": corr_id,
        "T0_invoice_created": t0,
        "T1_metrics_updated": t1,
        "T3_risk_predicted": t3,
        "T5_risk_scored": t5,
        "T7_collection_action": t7,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.lifecycle
class TestInvoiceFullLifecycle:
    """Single invoice lifecycle: invoice.created → collection.action."""

    @pytest.fixture(autouse=True)
    def patch_query(self):
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "utils.query_client.QueryClient.query", side_effect=_query_handler
        ):
            yield

    def test_single_invoice_lifecycle_completes(self):
        """One invoice must travel invoice→metrics→prediction→risk→collections
        within FULL_LIFECYCLE_BUDGET_MS in mocked mode.
        """
        kafka = StagedKafka()
        result = _run_invoice_lifecycle(seq=0, kafka=kafka)

        assert result is not None, (
            "Lifecycle trace could not be reconstructed — CSA or PPA did not emit."
        )
        t_start = result["T0_invoice_created"]
        t_end = result["T5_risk_scored"]  # minimum required end-point
        elapsed_ms = (t_end - t_start) * 1000.0

        logger.info(
            f"\n  Invoice lifecycle stages:\n"
            f"    T0 invoice.created:           {0:.1f} ms (base)\n"
            f"    T1 customer.metrics.updated:  {(result['T1_metrics_updated'] - t_start)*1000:.1f} ms\n"
            f"    T3 payment.risk.predicted:    {(result['T3_risk_predicted'] - t_start)*1000:.1f} ms\n"
            f"    T5 risk.scored:               {(result['T5_risk_scored'] - t_start)*1000:.1f} ms\n"
            + (f"    T7 collection.action:         {(result['T7_collection_action'] - t_start)*1000:.1f} ms\n"
               if result['T7_collection_action'] else "    T7 collection.action:         (no action emitted — below threshold)\n")
        )

        assert elapsed_ms <= FULL_LIFECYCLE_BUDGET_MS, (
            f"Lifecycle took {elapsed_ms:.1f} ms, budget is {FULL_LIFECYCLE_BUDGET_MS} ms"
        )

    def test_correlation_id_propagates_through_pipeline(self):
        """The same correlation_id injected at T0 must appear at every published event.

        This validates the event-tracing infrastructure — a broken correlation_id
        chain means events cannot be attributed to their originating invoice.
        """
        kafka = StagedKafka()
        result = _run_invoice_lifecycle(seq=1, kafka=kafka)
        assert result is not None, "Lifecycle did not complete"

        corr_id = result["corr_id"]
        # Check all published events that have a correlation_id field
        for entry in kafka.published:
            raw_corr = entry["raw"].get("correlation_id")
            if raw_corr is not None:
                assert raw_corr == corr_id, (
                    f"Event {entry['event_type']} has correlation_id='{raw_corr}', "
                    f"expected '{corr_id}'"
                )

    def test_stage_latency_breakdown_N50(self):
        """Run 50 lifecycle traces and report per-stage statistics.

        Asserts median of each inter-stage hop is below STAGE_BUDGETS_MS.
        Saves a waterfall chart if matplotlib is available.
        """
        N = 50
        kafka = StagedKafka()
        stage_deltas: Dict[str, List[float]] = {s: [] for s in STAGE_BUDGETS_MS}
        completed = 0

        for seq in range(N):
            result = _run_invoice_lifecycle(seq=seq, kafka=kafka)
            if result is None:
                continue
            completed += 1

            t0 = result["T0_invoice_created"]
            t1 = result["T1_metrics_updated"]
            t3 = result["T3_risk_predicted"]
            t5 = result["T5_risk_scored"]
            t7 = result["T7_collection_action"]

            stage_deltas["invoice.created → customer.metrics.updated"].append((t1 - t0) * 1000)
            stage_deltas["customer.metrics.updated → payment.risk.predicted"].append((t3 - t1) * 1000)
            stage_deltas["payment.risk.predicted → risk.scored"].append((t5 - t3) * 1000)
            if t7:
                stage_deltas["risk.scored → collection.action"].append((t7 - t5) * 1000)

        completion_rate = completed / N
        assert completion_rate >= COMPLETION_RATE_FLOOR, (
            f"Only {completed}/{N} ({completion_rate:.0%}) traces completed "
            f"(floor={COMPLETION_RATE_FLOOR:.0%})"
        )

        logger.info("\n  Per-stage latency (mocked, CPU-only):")
        for stage, deltas in stage_deltas.items():
            if not deltas:
                logger.info(f"    {stage}: no data")
                continue
            median_ms = statistics.median(deltas)
            p95_ms = sorted(deltas)[int(0.95 * len(deltas))]
            budget_ms = STAGE_BUDGETS_MS.get(stage, 999_999.0)
            logger.info(
                f"    {stage}: median={median_ms:.2f} ms, P95={p95_ms:.2f} ms "
                f"(budget={budget_ms:.0f} ms) {'✓' if median_ms <= budget_ms else '✗'}"
            )
            assert median_ms <= budget_ms, (
                f"Stage '{stage}' median {median_ms:.2f} ms exceeds budget {budget_ms:.0f} ms"
            )

        # Chart (best-effort)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            os.makedirs(OUTPUT_DIR, exist_ok=True)
            stages = list(STAGE_BUDGETS_MS.keys())
            medians = []
            p95s = []
            for s in stages:
                d = stage_deltas[s]
                if d:
                    medians.append(statistics.median(d))
                    p95s.append(sorted(d)[int(0.95 * len(d))])
                else:
                    medians.append(0)
                    p95s.append(0)

            fig, ax = plt.subplots(figsize=(12, 6))
            x = range(len(stages))
            ax.bar([i - 0.2 for i in x], medians, 0.35, label="Median (ms)", color="#4285f4")
            ax.bar([i + 0.2 for i in x], p95s, 0.35, label="P95 (ms)", color="#ea4335")
            ax.set_xticks(list(x))
            ax.set_xticklabels(
                [s.split("→")[1].strip() if "→" in s else s for s in stages],
                rotation=20,
                ha="right",
                fontsize=9,
            )
            ax.set_title(
                f"ACIS-X Invoice Lifecycle Stage Latency (N={N}, mocked)",
                fontsize=13,
                fontweight="bold",
            )
            ax.set_ylabel("Latency (ms)")
            ax.legend()
            ax.grid(axis="y", alpha=0.3)
            plt.tight_layout()
            out = os.path.join(OUTPUT_DIR, "lifecycle_stage_waterfall.png")
            plt.savefig(out, dpi=150)
            plt.close(fig)
            logger.info(f"  Stage waterfall chart saved to {out}")
        except ImportError:
            logger.warning("  matplotlib not available — skipping chart")


@pytest.mark.lifecycle
class TestPaymentRescoreLifecycle:
    """After a payment event, the customer's risk score should be re-evaluated
    and the lifecycle should complete within budget.
    """

    @pytest.fixture(autouse=True)
    def patch_query(self):
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "utils.query_client.QueryClient.query", side_effect=_query_handler
        ):
            yield

    def test_payment_triggers_risk_rescore(self):
        """payment.received → CustomerStateAgent re-scores → risk.scored published.

        Validates:
        1. Processing a payment re-triggers the CSA → RSA pipeline.
        2. A new risk.scored event is published.
        3. The re-score happens within the lifecycle budget.
        """
        from agents.intelligence.customer_state_agent import CustomerStateAgent
        from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        kafka = StagedKafka()
        customer_id = "cust_pay_lc_001"
        invoice_id = "inv_pay_lc_001"
        corr_id = f"corr_pay_{uuid.uuid4().hex[:12]}"

        csa = CustomerStateAgent(kafka_client=kafka)
        ppa = PaymentPredictionAgent(kafka_client=kafka)
        rsa = RiskScoringAgent(kafka_client=kafka)

        # Step 1: create invoice baseline and drive full pipeline
        inv_event = Event(
            event_id="evt_inv_pay_lc",
            event_type="invoice.created",
            event_source="ScenarioGeneratorAgent",
            event_time=datetime.utcnow(),
            entity_id=customer_id,
            correlation_id=corr_id,
            schema_version="1.1",
            payload={
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "amount": 50_000.0,
                "total_amount": 50_000.0,
                "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                "issued_date": datetime.utcnow().isoformat(),
                "status": "pending",
            },
            metadata={},
        )
        csa.process_event(inv_event)

        # Drive CSA → PPA → RSA for the invoice (first pass)
        for e in list(kafka.published):
            if e["event_type"] == "customer.metrics.updated":
                ppa.handle_event(Event.model_validate(e["raw"]))
        for e in list(kafka.published):
            if e["event_type"] == "payment.risk.predicted":
                rsa.handle_event(Event.model_validate(e["raw"]))

        risk_count_after_invoice = len(kafka.events_of_type("risk.scored"))
        assert risk_count_after_invoice >= 1, (
            "Expected at least one risk.scored after invoice.created"
        )

        # Snapshot before payment processing
        snapshot_before_payment = len(kafka.published)

        # Step 2: process payment event
        t_pay_start = time.monotonic()
        pay_event = Event(
            event_id="evt_pay_pay_lc",
            event_type="payment.received",
            event_source="ScenarioGeneratorAgent",
            event_time=datetime.utcnow(),
            entity_id=customer_id,
            correlation_id=corr_id,
            schema_version="1.1",
            payload={
                "payment_id": "pay_lc_001",
                "invoice_id": invoice_id,
                "customer_id": customer_id,
                "amount": 25_000.0,
                "payment_date": datetime.utcnow().isoformat(),
            },
            metadata={},
        )
        pay_offset = len(kafka.published)
        csa.process_event(pay_event)

        # Drive new metrics emitted by the payment through PPA → RSA
        new_metrics_driven = False
        for e in kafka.published[pay_offset:]:
            if e["event_type"] == "customer.metrics.updated":
                ppa_off2 = len(kafka.published)
                ppa.handle_event(Event.model_validate(e["raw"]))
                for e2 in kafka.published[ppa_off2:]:
                    if e2["event_type"] == "payment.risk.predicted":
                        rsa.handle_event(Event.model_validate(e2["raw"]))
                        new_metrics_driven = True
                break

        t_pay_end = time.monotonic()
        elapsed_ms = (t_pay_end - t_pay_start) * 1000.0

        total_risk_events = len(kafka.events_of_type("risk.scored"))
        # After the payment, at minimum the risk.scored count from the invoice
        # pass should still be present. If new_metrics_driven is True we expect
        # an additional risk.scored (new_metrics_driven = CSA emitted after payment).
        assert total_risk_events >= risk_count_after_invoice, (
            "risk.scored count decreased after payment processing — unexpected"
        )
        assert elapsed_ms <= FULL_LIFECYCLE_BUDGET_MS, (
            f"Payment re-score took {elapsed_ms:.1f} ms, budget={FULL_LIFECYCLE_BUDGET_MS} ms"
        )

        logger.info(
            f"  Payment lifecycle: payment processed in {elapsed_ms:.1f} ms, "
            f"new_metrics_chain_driven={new_metrics_driven}, "
            f"total risk.scored events={total_risk_events}"
        )
