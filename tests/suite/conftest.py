"""
tests/suite/conftest.py

Shared fixtures for the ACIS-X proper test suite.
All agents are mocked at the Kafka + QueryClient level so that no live
broker or DB is needed.
"""

import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import Event


# ---------------------------------------------------------------------------
# Kafka mock that records every publish() call with a wall-clock timestamp.
# Reused by both lifecycle and intelligence tests.
# ---------------------------------------------------------------------------

class RecordingKafka:
    """Minimal Kafka mock that captures publish calls with monotonic timestamps."""

    def __init__(self):
        self.published: List[Dict[str, Any]] = []
        self._producer = MagicMock()
        self._consumer = MagicMock()
        self._consumer.poll.return_value = {}

    def publish(self, topic: str, event: Any, **kwargs) -> bool:
        self.published.append({
            "topic": topic,
            "event": event if isinstance(event, dict) else (event.model_dump() if hasattr(event, "model_dump") else vars(event)),
            "ts": time.monotonic(),
        })
        return True

    def subscribe(self, topics, group_id=None):
        pass

    def close(self):
        pass

    def commit(self, message=None):
        pass


# ---------------------------------------------------------------------------
# Canned QueryClient responses used across multiple test files
# ---------------------------------------------------------------------------

def make_query_handler(
    *,
    overdue_count: int = 1,
    avg_delay: float = 10.0,
    on_time_ratio: float = 0.8,
    total_outstanding: float = 100_000.0,
    credit_limit: float = 500_000.0,
    customer_name: str = "Test Corp",
):
    """Return a QueryClient.query side_effect with configurable metrics."""

    def handler(query_type: str, params: dict = None, **kwargs):
        params = params or {}
        customer_id = params.get("customer_id", "cust_default")

        if query_type == "get_customer":
            return {
                "customer_id": customer_id,
                "name": customer_name,
                "credit_limit": credit_limit,
            }
        if query_type == "get_customer_metrics":
            return {
                "customer_id": customer_id,
                "total_outstanding": total_outstanding,
                "avg_delay": avg_delay,
                "on_time_ratio": on_time_ratio,
                "overdue_count": overdue_count,
                "credit_limit": credit_limit,
            }
        if query_type == "get_invoices_by_customer":
            return {
                "invoices": [
                    {
                        "invoice_id": f"inv_{customer_id}_{i}",
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
        if query_type in (
            "get_payments_by_invoices",
            "get_overdue_invoices",
            "update_invoice_cache",
            "invalidate_customer_cache",
            "invalidate_invoice_cache",
        ):
            return [] if "get_" in query_type else None
        return None

    return handler


# ---------------------------------------------------------------------------
# Event builder helpers
# ---------------------------------------------------------------------------

def make_invoice_event(seq: int, customer_id: Optional[str] = None) -> Event:
    cid = customer_id or f"cust_{seq:06d}"
    return Event(
        event_id=f"evt_inv_{seq:06d}_{uuid.uuid4().hex[:8]}",
        event_type="invoice.created",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=cid,
        correlation_id=f"corr_{uuid.uuid4().hex[:12]}",
        schema_version="1.1",
        payload={
            "customer_id": cid,
            "invoice_id": f"inv_{seq:06d}",
            "amount": 50_000.0 + seq,
            "total_amount": 50_000.0 + seq,
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "issued_date": datetime.utcnow().isoformat(),
            "status": "pending",
        },
        metadata={"environment": "test"},
    )


def make_payment_event(seq: int, customer_id: str, invoice_id: str, amount: float) -> Event:
    return Event(
        event_id=f"evt_pay_{seq:06d}_{uuid.uuid4().hex[:8]}",
        event_type="payment.received",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=f"corr_pay_{uuid.uuid4().hex[:12]}",
        schema_version="1.1",
        payload={
            "payment_id": f"pay_{seq:06d}",
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "amount": amount,
            "payment_date": datetime.utcnow().isoformat(),
        },
        metadata={"environment": "test"},
    )


def make_risk_predicted_event(
    seq: int,
    customer_id: str,
    invoice_id: str,
    risk_score: float,
    confidence: float = 0.8,
    correlation_id: Optional[str] = None,
) -> Event:
    return Event(
        event_id=f"evt_pred_{seq:06d}_{uuid.uuid4().hex[:8]}",
        event_type="payment.risk.predicted",
        event_source="PaymentPredictionAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=correlation_id or f"corr_{uuid.uuid4().hex[:12]}",
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "risk_score": risk_score,
            "confidence": confidence,
            "risk_category": "high" if risk_score > 0.7 else "medium",
            "reasons": ["test"],
        },
        metadata={"environment": "test"},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def recording_kafka() -> RecordingKafka:
    return RecordingKafka()


@pytest.fixture
def temp_db() -> str:
    """Yield a temporary SQLite path, clean up after test."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    import gc
    gc.collect()
    time.sleep(0.05)
    for suffix in ["", "-wal", "-shm"]:
        p = path + suffix
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass
