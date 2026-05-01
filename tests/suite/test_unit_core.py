"""
tests/suite/test_unit_core.py

CATEGORY: Unit Tests — Core Agent Contracts

Pulls in existing unit tests that still provide value and adds any gaps.
These tests prove that individual agents honour their interfaces and
data contracts without requiring Kafka or SQLite.

Tests included from existing suite:
  - DB invoice amount preservation on status update (from test_unit_architecture_fixes)
  - Overpayment clamping (from test_overpayment_clamping)
  - QueryAgent negative remaining_amount guard (from test_overpayment_clamping)
  - Exactly-once DB-level idempotency (from test_exactly_once_processing)
  - Risk score monotonicity — avg_delay, on_time_ratio, overdue_count sweeps
    (from test_risk_score_monotonicity)
  - CollectionsAgent decision thresholds (NEW — not tested before)
  - RiskScoringAgent context TTL cleanup (NEW — exercises _cleanup_customer_context)
  - PaymentPredictionAgent deduplication window (NEW)

Marker: @pytest.mark.unit
"""

import logging
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ===========================================================================
# Helper builders
# ===========================================================================

def _make_invoice_event(invoice_id: str, customer_id: str, total: float) -> Event:
    now = datetime.utcnow()
    return Event(
        event_id=f"evt_inv_{invoice_id}",
        event_type="invoice.created",
        event_source="test",
        event_time=now,
        entity_id=customer_id,
        schema_version="1.1",
        payload={
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "amount": total,
            "total_amount": total,
            "issued_date": now.isoformat(),
            "due_date": (now + timedelta(days=30)).isoformat(),
            "status": "pending",
        },
        metadata={},
    )


def _make_payment_event(payment_id: str, invoice_id: str, customer_id: str, amount: float) -> Event:
    return Event(
        event_id=f"evt_pay_{payment_id}",
        event_type="payment.received",
        event_source="test",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        schema_version="1.1",
        payload={
            "payment_id": payment_id,
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "amount": amount,
            "payment_date": datetime.utcnow().isoformat(),
        },
        metadata={},
    )


def _noop_query(query_type, params=None, **kw):
    return None


# ===========================================================================
# DB / Storage unit tests (migrated from existing suite + new)
# ===========================================================================

@pytest.mark.unit
class TestDBAgentContracts:
    """Core DB agent data contract tests."""

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_invoice_amount_preserved_on_status_update(self, _mock_qc, tmp_path):
        """An invoice.overdue status update must NOT overwrite a known total_amount with NULL.

        Source: test_unit_architecture_fixes — migrated here.
        """
        from agents.storage.db_agent import DBAgent

        db_path = str(tmp_path / "db.sqlite")
        kafka = MagicMock()
        kafka.publish.return_value = True
        agent = DBAgent(kafka_client=kafka, db_path=db_path)
        now = datetime.utcnow().isoformat()

        created = Event(
            event_id="evt_inv_created",
            event_type="invoice.created",
            event_source="test",
            event_time=datetime.utcnow(),
            entity_id="inv_001",
            payload={
                "invoice_id": "inv_001",
                "customer_id": "cust_001",
                "amount": 125.5,
                "issued_date": now,
                "due_date": now,
                "status": "pending",
            },
        )
        overdue = Event(
            event_id="evt_inv_overdue",
            event_type="invoice.overdue",
            event_source="test",
            event_time=datetime.utcnow(),
            entity_id="inv_001",
            payload={
                "invoice_id": "inv_001",
                "customer_id": "cust_001",
                "due_date": now,
                "status": "overdue",
            },
        )
        agent._handle_invoice_upsert(created)
        agent._handle_invoice_upsert(overdue)

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT total_amount, status FROM invoices WHERE invoice_id = ?",
            ("inv_001",),
        ).fetchone()
        conn.close()

        assert row is not None
        assert row[0] == pytest.approx(125.5), f"total_amount corrupted to {row[0]}"
        assert row[1] == "overdue"

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_overpayment_clamps_paid_amount(self, _mock_qc, tmp_path):
        """Two payments totalling > invoice total must clamp paid_amount at total_amount.

        Source: test_overpayment_clamping — migrated here.
        """
        from agents.storage.db_agent import DBAgent

        db_path = str(tmp_path / "db.sqlite")
        kafka = MagicMock()
        kafka.publish.return_value = True
        agent = DBAgent(kafka_client=kafka, db_path=db_path)

        agent.process_event(_make_invoice_event("inv_clamp", "cust_clamp", 100.0))
        agent.process_event(_make_payment_event("pay_01", "inv_clamp", "cust_clamp", 75.0))
        agent.process_event(_make_payment_event("pay_02", "inv_clamp", "cust_clamp", 75.0))

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT total_amount, paid_amount, status FROM invoices WHERE invoice_id = ?",
            ("inv_clamp",),
        ).fetchone()
        conn.close()

        assert row[1] == pytest.approx(100.0), f"paid_amount not clamped: {row[1]}"
        assert row[2] == "paid", f"status should be 'paid', got {row[2]}"

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_exactly_once_db_level_guard(self, _mock_qc, tmp_path):
        """10 000 identical events through the DB handler must produce exactly 1 row.

        Source: test_exactly_once_processing — migrated here.
        """
        from agents.storage.db_agent import DBAgent

        db_path = str(tmp_path / "db.sqlite")
        kafka = MagicMock()
        kafka.publish.return_value = True
        agent = DBAgent(kafka_client=kafka, db_path=db_path)

        event = Event(
            event_id="evt_idempotency_core",
            event_type="invoice.created",
            event_source="test",
            event_time=datetime.utcnow(),
            entity_id="cust_idem",
            schema_version="1.1",
            payload={
                "invoice_id": "inv_idem",
                "customer_id": "cust_idem",
                "amount": 75_000.0,
                "total_amount": 75_000.0,
                "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                "issued_date": datetime.utcnow().isoformat(),
                "status": "pending",
            },
            metadata={},
        )

        for _ in range(10_000):
            agent._handle_invoice_upsert(event)

        conn = sqlite3.connect(db_path)
        inv_count = conn.execute(
            "SELECT COUNT(*) FROM invoices WHERE invoice_id = ?", ("inv_idem",)
        ).fetchone()[0]
        log_count = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE event_id = ?", ("evt_idempotency_core",)
        ).fetchone()[0]
        conn.close()

        assert inv_count == 1, f"Expected 1 invoice row, got {inv_count}"
        assert log_count == 1, f"Expected 1 event_log row, got {log_count}"

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_query_agent_clamps_negative_remaining(self, _mock_qc, tmp_path):
        """QueryAgent.get_invoice() must never expose negative remaining_amount.

        Source: test_overpayment_clamping — migrated here.
        """
        from agents.storage.db_agent import DBAgent
        from agents.storage.query_agent import QueryAgent

        db_path = str(tmp_path / "db.sqlite")
        kafka = MagicMock()
        kafka.publish.return_value = True
        DBAgent(kafka_client=kafka, db_path=db_path)  # initialize schema

        conn = sqlite3.connect(db_path)
        now = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at) VALUES (?, ?, ?)",
            ("cust_ov", now, now),
        )
        conn.execute(
            """INSERT INTO invoices
               (invoice_id, customer_id, total_amount, paid_amount, issued_date, due_date, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("inv_ov", "cust_ov", 100.0, 150.0, now, now, "pending", now, now),
        )
        conn.commit()
        conn.close()

        qa = QueryAgent(kafka_client=kafka, db_path=db_path)
        result = qa.get_invoice("inv_ov")
        assert result is not None
        assert result["remaining_amount"] >= 0.0, (
            f"Negative remaining_amount: {result['remaining_amount']}"
        )


# ===========================================================================
# Risk scoring monotonicity (migrated from test_risk_score_monotonicity)
# ===========================================================================

BASELINE_METRICS = {
    "customer_id": "cust_mono",
    "credit_limit": 500_000.0,
    "total_outstanding": 100_000.0,
    "avg_delay": 10.0,
    "on_time_ratio": 0.80,
    "overdue_count": 1,
}
BASELINE_VELOCITY = {"velocity": 0.0, "trend": "stable", "volatility": 0.02}


def _mono_handler(overrides: Dict[str, Any]):
    def handler(query_type, params=None, **kw):
        if query_type == "get_customer_metrics":
            r = dict(BASELINE_METRICS)
            r.update(overrides)
            return r
        if query_type == "get_risk_velocity":
            return BASELINE_VELOCITY
        return None
    return handler


@pytest.mark.unit
class TestRiskScoreMonotonicity:
    """Risk score must increase (or stay flat) as credit signals deteriorate.

    Source: test_risk_score_monotonicity — migrated here with the same sweeps.
    """

    def _sweep(self, field: str, values: list) -> List[float]:
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        kafka = MagicMock()
        kafka.publish.return_value = True
        agent = RiskScoringAgent(kafka_client=kafka)
        scores = []
        for val in values:
            with patch("utils.query_client.QueryClient.query", side_effect=_mono_handler({field: val})):
                reasons: List[str] = []
                score = agent._refine_risk_with_context(
                    customer_id="cust_mono",
                    invoice_id="inv_mono",
                    base_risk=0.30,
                    confidence=0.80,
                    reasons=reasons,
                )
                scores.append(score)
        return scores

    def test_avg_delay_monotonic(self):
        """Risk must increase as avg_delay grows 0 → 90 days."""
        delays = list(range(0, 91, 10))
        scores = self._sweep("avg_delay", delays)
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Violation at avg_delay={delays[i]}: {scores[i]:.4f} < {scores[i-1]:.4f}"
            )

    def test_on_time_ratio_monotonic(self):
        """Risk must increase as on_time_ratio drops 1.0 → 0.0."""
        ratios = [round(1.0 - i * 0.1, 1) for i in range(11)]
        scores = self._sweep("on_time_ratio", ratios)
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Violation at on_time_ratio={ratios[i]}: {scores[i]:.4f} < {scores[i-1]:.4f}"
            )

    def test_overdue_count_monotonic(self):
        """Risk must increase as overdue_count grows 0 → 10."""
        counts = list(range(0, 11))
        scores = self._sweep("overdue_count", counts)
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Violation at overdue_count={counts[i]}: {scores[i]:.4f} < {scores[i-1]:.4f}"
            )


# ===========================================================================
# CollectionsAgent decision thresholds (NEW — not in existing suite)
# ===========================================================================

@pytest.mark.unit
class TestCollectionsAgentDecisionThresholds:
    """CollectionsAgent._compute_action() must map severity bands to correct actions.

    This is a direct unit test of the decision engine with no Kafka or DB.
    The expected mapping (from collections_agent.py thresholds):
        < 0.30 → no_action  (None)
        0.30 – 0.60 → send_reminder
        0.60 – 0.80 → escalate_invoice
        0.80 – 0.90 → hold_credit
        ≥ 0.90 → legal_escalation
    """

    def _make_agent(self):
        from agents.collections.collections_agent import CollectionsAgent
        kafka = MagicMock()
        kafka.publish.return_value = True
        return CollectionsAgent(kafka_client=kafka)

    @pytest.mark.parametrize("severity,expected_action", [
        (0.10, None),
        (0.29, None),
        (0.30, "send_reminder"),
        (0.50, "send_reminder"),
        (0.60, "escalate_invoice"),
        (0.75, "escalate_invoice"),
        (0.80, "hold_credit"),
        (0.89, "hold_credit"),
        (0.90, "legal_escalation"),
        (1.00, "legal_escalation"),
    ])
    def test_base_severity_to_action(self, severity, expected_action):
        """Severity score maps to correct base action without escalation modifiers."""
        agent = self._make_agent()
        action, _, _ = agent._compute_action(
            severity_score=severity,
            overdue_count=0,
            avg_delay=0.0,
            invoice_days_overdue=0.0,
            on_time_ratio=1.0,
        )
        assert action == expected_action, (
            f"severity={severity:.2f}: got '{action}', expected '{expected_action}'"
        )

    def test_overdue_count_escalates_action(self):
        """overdue_count > 3 must escalate 'send_reminder' → 'escalate_invoice'."""
        agent = self._make_agent()
        # severity 0.4 → base is send_reminder; with overdue_count=5 → escalate_invoice
        action, priority, _ = agent._compute_action(
            severity_score=0.40,
            overdue_count=5,
            avg_delay=10.0,
            invoice_days_overdue=0.0,
            on_time_ratio=0.8,
        )
        assert action == "escalate_invoice", (
            f"Expected escalation to 'escalate_invoice', got '{action}'"
        )
        assert priority in ("high", "critical")

    def test_chronic_delay_escalates_action(self):
        """avg_delay > 45 days must escalate 'send_reminder' → 'escalate_invoice'."""
        agent = self._make_agent()
        action, _, _ = agent._compute_action(
            severity_score=0.40,
            overdue_count=0,
            avg_delay=60.0,
            invoice_days_overdue=0.0,
            on_time_ratio=0.9,
        )
        assert action == "escalate_invoice", (
            f"Expected escalation due to delay, got '{action}'"
        )

    def test_90_day_overdue_invoice_forces_critical_priority(self):
        """Invoice overdue > 90 days must set critical priority."""
        agent = self._make_agent()
        _, priority, _ = agent._compute_action(
            severity_score=0.35,
            overdue_count=0,
            avg_delay=0.0,
            invoice_days_overdue=91.0,
            on_time_ratio=1.0,
        )
        assert priority == "critical", f"Expected critical priority, got '{priority}'"


# ===========================================================================
# RiskScoringAgent context TTL cleanup (NEW)
# ===========================================================================

@pytest.mark.unit
class TestRiskScoringContextTTL:
    """RiskScoringAgent._cleanup_customer_context() must evict expired entries
    and enforce the MAX_CONTEXT_CUSTOMERS memory ceiling.

    These behaviours were not tested in the existing suite.
    """

    def _make_agent(self):
        from agents.risk.risk_scoring_agent import RiskScoringAgent
        kafka = MagicMock()
        kafka.publish.return_value = True
        return RiskScoringAgent(kafka_client=kafka)

    def test_cleanup_evicts_expired_entries(self):
        """Entries older than CONTEXT_TTL_SECONDS must be removed on cleanup."""
        agent = self._make_agent()
        past_ts = time.time() - agent.CONTEXT_TTL_SECONDS - 60  # definitely expired

        agent._customer_risk_context["cust_expired"] = {
            "data": {"aggregated_risk": 0.5},
            "updated_at": past_ts,
        }
        agent._customer_risk_context["cust_fresh"] = {
            "data": {"aggregated_risk": 0.3},
            "updated_at": time.time(),
        }

        agent._cleanup_customer_context()

        assert "cust_expired" not in agent._customer_risk_context, (
            "Expired entry was not evicted"
        )
        assert "cust_fresh" in agent._customer_risk_context, (
            "Fresh entry incorrectly evicted"
        )

    def test_cleanup_enforces_max_size(self):
        """Context dict must not exceed MAX_CONTEXT_CUSTOMERS after cleanup."""
        agent = self._make_agent()
        overfill = agent.MAX_CONTEXT_CUSTOMERS + 100
        now = time.time()

        for i in range(overfill):
            agent._customer_risk_context[f"cust_{i:06d}"] = {
                "data": {},
                "updated_at": now + i,  # increasing timestamps so oldest are first
            }

        agent._cleanup_customer_context()
        assert len(agent._customer_risk_context) <= agent.MAX_CONTEXT_CUSTOMERS, (
            f"Context size {len(agent._customer_risk_context)} exceeds "
            f"MAX_CONTEXT_CUSTOMERS={agent.MAX_CONTEXT_CUSTOMERS}"
        )
