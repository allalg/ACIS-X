"""
tests/suite/test_integration_contracts.py

CATEGORY: Integration Tests — Agent-to-Agent Contracts

Validates the data contracts between agents (event_type strings, payload fields,
persistence) across the full write-path without a live Kafka broker.

Tests included:
  - Risk profile persistence (from test_integration_pipeline — migrated)
  - AggregatorAgent ↔ DBAgent event_type contract (from test_integration_pipeline)
  - CustomerStateAgent → PaymentPredictionAgent payload contract (NEW)
  - RiskScoringAgent → CollectionsAgent payload contract (NEW)
  - Correlation ID survives the full mocked pipeline (NEW)

Marker: @pytest.mark.integration
"""

import inspect
import logging
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

def _make_kafka():
    client = MagicMock()
    client.subscribe.return_value = None
    client.publish.return_value = None
    client.poll.return_value = []
    client.commit.return_value = None
    client.close.return_value = None
    return client


def _noop_query(query_type, params=None, **kw):
    return None


# ---------------------------------------------------------------------------
# Risk profile persistence (migrated from test_integration_pipeline)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestRiskProfilePersistence:
    """DBAgent must correctly persist risk.profile.updated events."""

    @pytest.fixture
    def db_agent(self, tmp_path):
        from agents.storage.db_agent import DBAgent
        db_path = str(tmp_path / "integration.db")
        kafka = _make_kafka()
        return DBAgent(kafka_client=kafka, db_path=db_path), db_path

    def _make_risk_event(self, customer_id: str, **payload_overrides) -> Event:
        payload = {
            "customer_id": customer_id,
            "company_name": "Integration Corp",
            "financial_risk": 0.65,
            "litigation_risk": 0.30,
            "combined_risk": 0.51,
            "severity": "medium",
            "confidence": 0.85,
            "financial_source": "NSE",
            "litigation_source": "NCLT",
            "generated_at": datetime.utcnow().isoformat(),
        }
        payload.update(payload_overrides)
        return Event(
            event_id=f"evt_{uuid.uuid4()}",
            event_type="risk.profile.updated",
            event_source="AggregatorAgent",
            event_time=datetime.utcnow(),
            entity_id=customer_id,
            schema_version="1.1",
            correlation_id=f"corr_{uuid.uuid4()}",
            payload=payload,
            metadata={"environment": "test"},
        )

    def test_persists_correct_values(self, db_agent):
        agent, db_path = db_agent
        cid = "cust_integ_001"

        conn = sqlite3.connect(db_path)
        now = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO customers (customer_id, name, created_at, updated_at) VALUES (?,?,?,?)",
            (cid, "Integration Corp", now, now),
        )
        conn.commit()
        conn.close()

        event = self._make_risk_event(cid, financial_risk=0.65, litigation_risk=0.30, combined_risk=0.51)
        with patch("agents.storage.db_agent.QueryClient.query", return_value=None):
            agent._handle_customer_risk_profile(event)

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT financial_risk, litigation_risk, combined_risk FROM customer_risk_profile WHERE customer_id=?",
            (cid,),
        ).fetchone()
        conn.close()

        assert row is not None, "Row not persisted"
        assert abs(row[0] - 0.65) < 1e-9
        assert abs(row[1] - 0.30) < 1e-9
        assert abs(row[2] - 0.51) < 1e-9

    def test_idempotent_double_write(self, db_agent):
        """Same event twice must not create duplicate rows."""
        agent, db_path = db_agent
        cid = "cust_integ_002"
        fixed_event_id = f"evt_{uuid.uuid4()}"

        conn = sqlite3.connect(db_path)
        now = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at) VALUES (?,?,?)",
            (cid, now, now),
        )
        conn.commit()
        conn.close()

        event = self._make_risk_event(cid)
        event = event.model_copy(update={"event_id": fixed_event_id})

        with patch("agents.storage.db_agent.QueryClient.query", return_value=None):
            agent._handle_customer_risk_profile(event)
            agent._handle_customer_risk_profile(event)

        conn = sqlite3.connect(db_path)
        count = conn.execute(
            "SELECT COUNT(*) FROM customer_risk_profile WHERE customer_id=?", (cid,)
        ).fetchone()[0]
        conn.close()
        assert count == 1, f"Duplicate write created {count} rows"

    def test_event_type_contract_aggregator_to_db_agent(self):
        """AggregatorAgent must publish exactly the event_type DBAgent routes on."""
        from agents.intelligence import aggregator_agent as agg_mod
        from agents.storage import db_agent as db_mod

        EXPECTED = "risk.profile.updated"
        agg_src = inspect.getsource(agg_mod)
        db_src = inspect.getsource(db_mod)

        assert f'"{EXPECTED}"' in agg_src or f"'{EXPECTED}'" in agg_src, (
            f"AggregatorAgent source does not publish event_type='{EXPECTED}'"
        )
        assert f'"{EXPECTED}"' in db_src or f"'{EXPECTED}'" in db_src, (
            f"DBAgent source does not route on event_type='{EXPECTED}'"
        )


# ---------------------------------------------------------------------------
# CSA → PPA payload contract (NEW)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestCSAtoPPAContract:
    """CustomerStateAgent must publish customer.metrics.updated with the fields
    that PaymentPredictionAgent reads: customer_id, avg_delay, on_time_ratio,
    total_outstanding.

    This test injects an invoice.created event and inspects the CSA output.
    """

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_metrics_event_contains_required_fields(self, _mock_qc):
        from agents.intelligence.customer_state_agent import CustomerStateAgent

        published = []
        kafka = MagicMock()
        kafka.publish.side_effect = lambda t, e, **kw: published.append({"topic": t, "event": e}) or True
        kafka._producer = MagicMock()
        kafka._consumer = MagicMock()
        kafka._consumer.poll.return_value = {}

        csa = CustomerStateAgent(kafka_client=kafka)
        now = datetime.utcnow()

        inv_event = Event(
            event_id="evt_contract_inv",
            event_type="invoice.created",
            event_source="ScenarioGeneratorAgent",
            event_time=now,
            entity_id="cust_contract_001",
            correlation_id="corr_contract_001",
            schema_version="1.1",
            payload={
                "customer_id": "cust_contract_001",
                "invoice_id": "inv_contract_001",
                "amount": 50_000.0,
                "total_amount": 50_000.0,
                "due_date": (now + timedelta(days=30)).isoformat(),
                "issued_date": now.isoformat(),
                "status": "pending",
            },
            metadata={},
        )
        csa.process_event(inv_event)

        # Find customer.metrics.updated
        metrics_events = [
            e for e in published
            if isinstance(e.get("event"), dict)
            and e["event"].get("event_type") == "customer.metrics.updated"
        ]

        assert len(metrics_events) >= 1, (
            "CustomerStateAgent did not publish customer.metrics.updated after invoice.created"
        )

        payload = metrics_events[-1]["event"].get("payload", {})
        required_fields = ["customer_id", "avg_delay", "on_time_ratio", "total_outstanding"]
        for field in required_fields:
            assert field in payload, (
                f"customer.metrics.updated payload is missing field '{field}'. "
                f"PaymentPredictionAgent will break. Present fields: {list(payload.keys())}"
            )


# ---------------------------------------------------------------------------
# RSA → CollectionsAgent payload contract (NEW)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestRSAtoCollectionsContract:
    """RiskScoringAgent must publish risk.scored with the fields CollectionsAgent reads:
    customer_id, invoice_id, risk_score, amount, days_overdue.
    """

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_risk_scored_contains_required_fields(self, _mock_qc):
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        published = []
        kafka = MagicMock()
        kafka.publish.side_effect = lambda t, e, **kw: published.append({"topic": t, "event": e}) or True
        kafka._producer = MagicMock()
        kafka._consumer = MagicMock()
        kafka._consumer.poll.return_value = {}

        rsa = RiskScoringAgent(kafka_client=kafka)

        pred_event = Event(
            event_id="evt_contract_pred",
            event_type="payment.risk.predicted",
            event_source="PaymentPredictionAgent",
            event_time=datetime.utcnow(),
            entity_id="cust_rsa_contract",
            correlation_id="corr_rsa_contract",
            schema_version="1.1",
            payload={
                "customer_id": "cust_rsa_contract",
                "invoice_id": "inv_rsa_contract",
                "risk_score": 0.65,
                "confidence": 0.80,
                "risk_category": "medium",
                "amount": 40_000.0,
                "days_overdue": 5.0,
                "reasons": ["test"],
            },
            metadata={},
        )
        rsa.handle_event(pred_event)

        risk_events = [
            e for e in published
            if isinstance(e.get("event"), dict)
            and e["event"].get("event_type") == "risk.scored"
        ]

        assert len(risk_events) >= 1, "RiskScoringAgent did not publish risk.scored"

        payload = risk_events[-1]["event"].get("payload", {})
        required = ["customer_id", "invoice_id", "risk_score", "amount", "days_overdue"]
        for field in required:
            assert field in payload, (
                f"risk.scored payload missing field '{field}'. "
                f"CollectionsAgent will fail. Present: {list(payload.keys())}"
            )


# ---------------------------------------------------------------------------
# Correlation ID end-to-end (NEW)
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestCorrelationIdPropagation:
    """A correlation_id injected at invoice.created must appear in every
    downstream event: customer.metrics.updated, payment.risk.predicted, risk.scored.
    """

    @patch("utils.query_client.QueryClient.query")
    def test_correlation_id_flows_through_csa_ppa_rsa(self, mock_qc):
        from agents.intelligence.customer_state_agent import CustomerStateAgent
        from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        customer_id = "cust_corr_001"
        corr_id = f"corr_{uuid.uuid4().hex}"

        # Set up mock query responses
        def _handler(query_type, params=None, **kw):
            params = params or {}
            cid = params.get("customer_id", customer_id)
            if query_type == "get_customer_metrics":
                return {
                    "customer_id": cid,
                    "total_outstanding": 100_000.0,
                    "avg_delay": 5.0,
                    "on_time_ratio": 0.85,
                    "overdue_count": 1,
                    "credit_limit": 500_000.0,
                }
            if query_type == "get_invoices_by_customer":
                return {
                    "invoices": [{
                        "invoice_id": f"inv_{cid}",
                        "customer_id": cid,
                        "total_amount": 50_000.0,
                        "amount": 50_000.0,
                        "remaining_amount": 50_000.0,
                        "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                        "status": "pending",
                    }]
                }
            if query_type == "get_risk_velocity":
                return {"velocity": 0.0, "trend": "stable", "volatility": 0.01}
            if query_type in ("get_payments_by_invoices", "update_invoice_cache",
                              "invalidate_customer_cache", "invalidate_invoice_cache"):
                return [] if "get_" in query_type else None
            return None

        mock_qc.side_effect = _handler

        published = []
        kafka = MagicMock()
        kafka.publish.side_effect = lambda t, e, **kw: published.append({"topic": t, "event": e}) or True
        kafka._producer = MagicMock()
        kafka._consumer = MagicMock()
        kafka._consumer.poll.return_value = {}

        csa = CustomerStateAgent(kafka_client=kafka)
        ppa = PaymentPredictionAgent(kafka_client=kafka)
        rsa = RiskScoringAgent(kafka_client=kafka)

        now = datetime.utcnow()
        inv_event = Event(
            event_id="evt_corr_prop",
            event_type="invoice.created",
            event_source="test",
            event_time=now,
            entity_id=customer_id,
            correlation_id=corr_id,
            schema_version="1.1",
            payload={
                "customer_id": customer_id,
                "invoice_id": f"inv_{customer_id}",
                "amount": 50_000.0,
                "total_amount": 50_000.0,
                "due_date": (now + timedelta(days=30)).isoformat(),
                "issued_date": now.isoformat(),
                "status": "pending",
            },
            metadata={},
        )
        csa.process_event(inv_event)

        # Pass CSA output through PPA
        for e in list(published):
            if e["event"].get("event_type") == "customer.metrics.updated":
                me = Event.model_validate(e["event"])
                me = me.model_copy(update={"correlation_id": corr_id})
                ppa.handle_event(me)

        # Pass PPA output through RSA
        for e in list(published):
            if e["event"].get("event_type") == "payment.risk.predicted":
                pe = Event.model_validate(e["event"])
                pe = pe.model_copy(update={"correlation_id": corr_id})
                rsa.handle_event(pe)

        # Check correlation_id in all downstream events
        target_types = {
            "customer.metrics.updated",
            "payment.risk.predicted",
            "risk.scored",
        }
        found_types = set()
        for e in published:
            etype = e["event"].get("event_type")
            if etype in target_types:
                found_types.add(etype)
                evt_corr = e["event"].get("correlation_id")
                assert evt_corr == corr_id, (
                    f"Event {etype} has correlation_id='{evt_corr}', "
                    f"expected '{corr_id}'. Correlation chain broken."
                )

        assert "customer.metrics.updated" in found_types, (
            "CustomerStateAgent did not emit customer.metrics.updated"
        )
        assert "risk.scored" in found_types, (
            "RiskScoringAgent did not emit risk.scored"
        )
