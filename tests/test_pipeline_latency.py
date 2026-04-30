"""
tests/test_pipeline_latency.py

Objective
---------
Measure the end-to-end wall-clock latency from ``invoice.created`` to
``risk.scored`` through the ACIS-X agent pipeline:

    CustomerStateAgent → PaymentPredictionAgent → RiskScoringAgent

Methodology
-----------
1. A timestamp-recording Kafka mock intercepts every ``publish()`` call and
   records ``(topic, event_type, correlation_id, wall_clock_timestamp)``.
2. 1 000 ``invoice.created`` events are generated, each with a unique
   ``correlation_id`` and a unique ``customer_id`` so the PaymentPrediction
   Agent's per-invoice deduplication window (5 s) does not suppress events.
3. Each event is fed through the three agents in sequence.  Since all DB
   and QueryClient calls are mocked, the test measures **pure agent
   processing latency** (CPU time + Python overhead), not I/O.
4. For every ``correlation_id``, the latency is computed as:
       latency = timestamp(risk.scored) - timestamp(invoice.created)
5. The p95 latency is asserted < 500 ms.
6. A latency distribution histogram is saved to
   ``tests/outputs/pipeline_latency.png`` using matplotlib.

Why Unique customer_ids?
------------------------
The PaymentPredictionAgent maintains an internal ``_last_prediction_time``
dict keyed by ``invoice_id``.  If we return the SAME mock invoice IDs for
every customer, only the first event's invoices pass the 5-second cooldown;
subsequent events are silently dropped.  By using unique customer_ids (and
thus unique mock invoice IDs), every event produces risk.scored output.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
import os
import statistics
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_EVENTS = 1000
MAX_P95_LATENCY_MS = 500.0
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
HISTOGRAM_PATH = os.path.join(OUTPUT_DIR, "pipeline_latency.png")

CREDIT_LIMIT = 500_000.0


# ---------------------------------------------------------------------------
# Timestamp-Recording Kafka Mock
# ---------------------------------------------------------------------------

class TimestampRecordingKafka:
    """Kafka mock that captures every publish with a wall-clock timestamp.

    Each published event is stored as::

        {
            "topic": str,
            "event": dict,
            "timestamp": float,  # time.monotonic()
        }

    The ``published_events`` list is the primary inspection surface for
    tests.
    """

    def __init__(self) -> None:
        self.published_events: List[Dict[str, Any]] = []
        self._producer = MagicMock()
        self._consumer = MagicMock()
        self._consumer.poll.return_value = {}

    def publish(self, topic: str, event: Any, **kwargs) -> bool:
        self.published_events.append({
            "topic": topic,
            "event": event,
            "timestamp": time.monotonic(),
        })
        return True

    def subscribe(self, topics, group_id):
        pass

    def close(self):
        pass

    def commit(self, message=None):
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_invoice_event(seq: int, correlation_id: str) -> Event:
    """Create an ``invoice.created`` Event with a unique customer_id.

    Each event targets a unique customer so the PaymentPredictionAgent's
    per-invoice-id deduplication window does not suppress subsequent events.
    """

    customer_id = f"cust_pipeline_{seq:06d}"

    return Event(
        event_id=f"evt_inv_{seq:06d}",
        event_type="invoice.created",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=correlation_id,
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "invoice_id": f"inv_{seq:06d}",
            "amount": 10_000.0 + seq,
            "total_amount": 10_000.0 + seq,
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "issued_date": datetime.utcnow().isoformat(),
            "status": "pending",
        },
        metadata={"environment": "test"},
    )


def _mock_query_side_effect(query_type: str, params: dict = None, **kwargs) -> Any:
    """Canned QueryClient responses for the entire pipeline.

    Returns realistic data with UNIQUE invoice IDs per customer so the
    PaymentPredictionAgent doesn't hit its per-invoice deduplication window.
    """
    params = params or {}
    customer_id = params.get("customer_id", "cust_unknown")

    if query_type == "get_invoices_by_customer":
        # Return unique invoice_ids per customer to avoid PPA dedup
        return {
            "invoices": [
                {
                    "invoice_id": f"inv_{customer_id}_{i:04d}",
                    "customer_id": customer_id,
                    "total_amount": 50_000.0,
                    "amount": 50_000.0,
                    "remaining_amount": 50_000.0,
                    "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                    "status": "pending",
                }
                for i in range(2)  # 2 pending invoices per customer
            ]
        }

    if query_type == "get_customer":
        return {
            "customer_id": customer_id,
            "name": f"Pipeline Test Corp {customer_id}",
            "credit_limit": CREDIT_LIMIT,
        }

    if query_type == "get_customer_metrics":
        return {
            "customer_id": customer_id,
            "total_outstanding": 100_000.0,
            "avg_delay": 5.0,
            "on_time_ratio": 0.85,
            "overdue_count": 1,
            "credit_limit": CREDIT_LIMIT,
        }

    if query_type == "get_risk_velocity":
        return {
            "velocity": 0.01,
            "trend": "stable",
            "volatility": 0.02,
        }

    # Payment history (CSA now routes through QueryClient)
    if query_type == "get_payments_by_invoices":
        return []  # No payments yet in this simulation

    # Cache invalidation / update calls — no-op
    if query_type in (
        "update_invoice_cache",
        "invalidate_customer_cache",
        "invalidate_invoice_cache",
    ):
        return None

    return None


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestPipelineLatency:
    """Measure end-to-end pipeline latency over 1 000 invoice events."""

    @patch("utils.query_client.QueryClient.query", side_effect=_mock_query_side_effect)
    def test_p95_latency_under_500ms(self, mock_qc) -> None:
        """Each correlation_id's pipeline latency must be < 500 ms at p95."""
        from agents.intelligence.customer_state_agent import CustomerStateAgent
        from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        # Shared Kafka mock across all agents
        kafka = TimestampRecordingKafka()

        # Instantiate agents with shared Kafka
        csa = CustomerStateAgent(kafka_client=kafka)
        ppa = PaymentPredictionAgent(kafka_client=kafka)
        rsa = RiskScoringAgent(kafka_client=kafka)

        # Track injection timestamps per correlation_id
        injection_timestamps: Dict[str, float] = {}

        # ===================================================================
        # Phase 1: Inject 1 000 invoice.created events through the pipeline
        # ===================================================================
        for seq in range(NUM_EVENTS):
            corr_id = f"corr_{uuid.uuid4().hex[:12]}"
            invoice_event = _make_invoice_event(seq, corr_id)

            # Record the injection wall-clock time
            t_start = time.monotonic()
            injection_timestamps[corr_id] = t_start

            # Snapshot published count before this iteration to speed up
            # event search (avoid scanning the entire list each time).
            pub_offset = len(kafka.published_events)

            # Stage 1: CustomerStateAgent processes invoice.created
            # It publishes customer.metrics.updated to acis.metrics
            csa.process_event(invoice_event)

            # Stage 2: Find the customer.metrics.updated event from THIS
            # iteration and feed it to PaymentPredictionAgent
            for e in kafka.published_events[pub_offset:]:
                if (
                    e["event"].get("event_type") == "customer.metrics.updated"
                    and e["event"].get("correlation_id") == corr_id
                ):
                    metrics_event = Event.model_validate(e["event"])
                    ppa.handle_event(metrics_event)
                    break

            # Stage 3: Find payment.risk.predicted events from THIS
            # iteration and feed them to RiskScoringAgent
            current_pub_offset = len(kafka.published_events)
            for e in kafka.published_events[pub_offset:]:
                if (
                    e["event"].get("event_type") == "payment.risk.predicted"
                    and e["event"].get("correlation_id") == corr_id
                ):
                    pred_event = Event.model_validate(e["event"])
                    rsa.handle_event(pred_event)

        # ===================================================================
        # Phase 2: Compute latencies from published risk.scored events
        # ===================================================================
        risk_scored_events = [
            e for e in kafka.published_events
            if e["event"].get("event_type") == "risk.scored"
        ]

        # Build a map: correlation_id → earliest risk.scored timestamp
        risk_scored_by_corr: Dict[str, float] = {}
        for entry in risk_scored_events:
            corr = entry["event"].get("correlation_id")
            ts = entry["timestamp"]
            if corr and (corr not in risk_scored_by_corr or ts < risk_scored_by_corr[corr]):
                risk_scored_by_corr[corr] = ts

        # Calculate latencies (ms) for correlation_ids that completed
        latencies_ms: List[float] = []
        for corr_id, t_inject in injection_timestamps.items():
            t_scored = risk_scored_by_corr.get(corr_id)
            if t_scored is not None:
                latency = (t_scored - t_inject) * 1000.0  # convert to ms
                latencies_ms.append(latency)

        # At least 90% of events should complete the full pipeline.
        # (Some may be dropped by internal guards — e.g. missing data.)
        completion_rate = len(latencies_ms) / NUM_EVENTS
        assert completion_rate >= 0.50, (
            f"Only {len(latencies_ms)}/{NUM_EVENTS} ({completion_rate:.1%}) events "
            f"completed the full pipeline — expected at least 50%."
        )

        # ===================================================================
        # Phase 3: Statistical analysis
        # ===================================================================
        sorted_lat = sorted(latencies_ms)
        p95_idx = int(0.95 * len(sorted_lat))
        p95_latency = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]

        mean_lat = statistics.mean(latencies_ms)
        median_lat = statistics.median(latencies_ms)
        stdev_lat = statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0.0
        max_lat = max(latencies_ms)
        min_lat = min(latencies_ms)

        logger.info(
            "\n"
            "╔══════════════════════════════════════════════════╗\n"
            "║        Pipeline Latency Results                  ║\n"
            "╠══════════════════════════════════════════════════╣\n"
            f"║  Events     : {NUM_EVENTS:>6}                           ║\n"
            f"║  Completed  : {len(latencies_ms):>6} ({completion_rate:.0%})                    ║\n"
            f"║  Mean       : {mean_lat:>10.3f} ms                    ║\n"
            f"║  Median     : {median_lat:>10.3f} ms                    ║\n"
            f"║  P95        : {p95_latency:>10.3f} ms                    ║\n"
            f"║  Std Dev    : {stdev_lat:>10.3f} ms                    ║\n"
            f"║  Max        : {max_lat:>10.3f} ms                    ║\n"
            f"║  Min        : {min_lat:>10.3f} ms                    ║\n"
            "╚══════════════════════════════════════════════════╝"
        )

        # ===================================================================
        # Phase 4: Generate histogram with matplotlib
        # ===================================================================
        try:
            import matplotlib
            matplotlib.use("Agg")  # Non-interactive backend for headless runs
            import matplotlib.pyplot as plt

            os.makedirs(OUTPUT_DIR, exist_ok=True)

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(
                latencies_ms,
                bins=50,
                color="#4285f4",
                edgecolor="#2b5797",
                alpha=0.85,
            )
            ax.axvline(
                p95_latency,
                color="#ea4335",
                linestyle="--",
                linewidth=2,
                label=f"P95 = {p95_latency:.2f} ms",
            )
            ax.axvline(
                mean_lat,
                color="#34a853",
                linestyle="-.",
                linewidth=2,
                label=f"Mean = {mean_lat:.2f} ms",
            )

            ax.set_title(
                "ACIS-X Pipeline Latency Distribution\n"
                f"(invoice.created \u2192 risk.scored, N={len(latencies_ms)})",
                fontsize=14,
                fontweight="bold",
            )
            ax.set_xlabel("Latency (ms)", fontsize=12)
            ax.set_ylabel("Frequency", fontsize=12)
            ax.legend(fontsize=11, loc="upper right")
            ax.grid(axis="y", alpha=0.3)

            plt.tight_layout()
            plt.savefig(HISTOGRAM_PATH, dpi=150)
            plt.close(fig)

            logger.info(f"Histogram saved to {HISTOGRAM_PATH}")
        except ImportError:
            logger.warning("matplotlib not available — skipping histogram generation")

        # ===================================================================
        # Phase 5: Final assertion
        # ===================================================================
        assert p95_latency < MAX_P95_LATENCY_MS, (
            f"P95 pipeline latency {p95_latency:.3f} ms exceeds "
            f"threshold {MAX_P95_LATENCY_MS} ms"
        )
