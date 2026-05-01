"""
tests/test_overpayment_clamping.py

Objective
---------
Formally prove that the ``DBAgent`` prevents negative invoice balances
by clamping ``paid_amount`` to ``total_amount``.

Methodology
-----------
1. Initialize ``DBAgent`` with a temporary SQLite database.
2. Insert an invoice with ``total_amount=100``.
3. Process a ``payment.received`` event for 75.
   Assert ``paid_amount=75``, ``status=partial``.
4. Process a second ``payment.received`` for 75 (cumulative 150 > 100).
   Assert ``paid_amount`` is clamped to 100, ``status=paid``,
   ``remaining_amount=0.0``.
5. Assert that ``QueryAgent.get_invoice()`` never returns a negative
   ``remaining_amount``.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
import os
import sqlite3
import tempfile
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_invoice_event(invoice_id: str, customer_id: str, total_amount: float):
    """Build an invoice.created Event."""
    from schemas.event_schema import Event
    return Event(
        event_id=f"evt_inv_{invoice_id}",
        event_type="invoice.created",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=f"corr_inv_{invoice_id}",
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "customer_name": "Clamp Test Corp",
            "invoice_id": invoice_id,
            "total_amount": total_amount,
            "amount": total_amount,
            "due_date": "2026-12-31",
            "issued_date": datetime.utcnow().isoformat(),
            "status": "pending",
        },
        metadata={},
    )


def _make_payment_event(
    payment_id: str,
    invoice_id: str,
    customer_id: str,
    amount: float,
):
    """Build a payment.received Event."""
    from schemas.event_schema import Event
    return Event(
        event_id=f"evt_pay_{payment_id}",
        event_type="payment.received",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        correlation_id=f"corr_pay_{payment_id}",
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


def _read_invoice(db_path: str, invoice_id: str) -> dict:
    """Read an invoice row directly from the SQLite database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()
    conn.close()
    if row is None:
        return {}
    result = dict(row)
    # Compute remaining_amount
    total = float(result.get("total_amount", 0) or 0)
    paid = float(result.get("paid_amount", 0) or 0)
    result["remaining_amount"] = max(0.0, total - paid)
    return result


def _noop_query_side_effect(query_type: str, params=None, **kwargs):
    """No-op QueryClient.query() mock.

    DBAgent calls QueryClient.query("invalidate_invoice_cache", ...)
    and QueryClient.query("invalidate_customer_cache", ...) after
    processing payments.  We need to stub these out since there is
    no live Kafka broker in tests.
    """
    return None


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestOverpaymentClamping:
    """Prove DBAgent clamps paid_amount to total_amount."""

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query_side_effect)
    def test_overpayment_clamps_to_total(self, mock_qc) -> None:
        """Two payments totaling 150 on a 100 invoice must clamp at 100."""
        from agents.storage.db_agent import DBAgent

        # Create a temp database
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        kafka = MagicMock()
        kafka.publish.return_value = True

        try:
            agent = DBAgent(kafka_client=kafka, db_path=db_path)

            invoice_id = "inv_clamp_001"
            customer_id = "cust_clamp_001"

            # Step 1: Create the invoice (total_amount=100)
            inv_event = _make_invoice_event(invoice_id, customer_id, 100.0)
            agent.process_event(inv_event)

            inv = _read_invoice(db_path, invoice_id)
            assert inv["total_amount"] == 100.0, f"total_amount={inv['total_amount']}"
            assert inv["paid_amount"] == 0.0, f"paid_amount={inv['paid_amount']}"
            assert inv["status"] == "pending", f"status={inv['status']}"

            # Step 2: First payment of 75
            pay1 = _make_payment_event("pay_001", invoice_id, customer_id, 75.0)
            agent.process_event(pay1)

            inv = _read_invoice(db_path, invoice_id)
            assert inv["paid_amount"] == 75.0, (
                f"After first payment: paid_amount={inv['paid_amount']}, expected 75.0"
            )
            assert inv["status"] == "partial", (
                f"After first payment: status={inv['status']}, expected 'partial'"
            )
            assert inv["remaining_amount"] == 25.0, (
                f"After first payment: remaining={inv['remaining_amount']}, expected 25.0"
            )

            # Step 3: Second payment of 75 (total would be 150 > 100)
            pay2 = _make_payment_event("pay_002", invoice_id, customer_id, 75.0)
            agent.process_event(pay2)

            inv = _read_invoice(db_path, invoice_id)
            assert inv["paid_amount"] == 100.0, (
                f"After overpayment: paid_amount={inv['paid_amount']}, "
                f"expected 100.0 (clamped to total_amount)"
            )
            assert inv["status"] == "paid", (
                f"After overpayment: status={inv['status']}, expected 'paid'"
            )
            assert inv["remaining_amount"] == 0.0, (
                f"After overpayment: remaining={inv['remaining_amount']}, "
                f"expected 0.0 (never negative)"
            )

            logger.info(
                f"Overpayment clamping verified: "
                f"total=100, payments=75+75=150, clamped_paid=100, "
                f"remaining=0, status=paid"
            )

        finally:
            # Cleanup temp DB and WAL/SHM artifacts
            for suffix in ["", "-wal", "-shm"]:
                path = db_path + suffix
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query_side_effect)
    def test_query_agent_never_returns_negative_remaining(self, mock_qc) -> None:
        """QueryAgent.get_invoice() must never expose a negative
        remaining_amount to downstream consumers."""
        from agents.storage.db_agent import DBAgent
        from agents.storage.query_agent import QueryAgent

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        kafka = MagicMock()
        kafka.publish.return_value = True

        try:
            db_agent = DBAgent(kafka_client=kafka, db_path=db_path)
            qa = QueryAgent(kafka_client=kafka, db_path=db_path)

            invoice_id = "inv_clamp_qa"
            customer_id = "cust_clamp_qa"

            # Create invoice and overpay
            db_agent.process_event(
                _make_invoice_event(invoice_id, customer_id, 50.0)
            )
            db_agent.process_event(
                _make_payment_event("pay_qa_1", invoice_id, customer_id, 80.0)
            )

            # QueryAgent should return non-negative remaining_amount
            result = qa.get_invoice(invoice_id)
            assert result is not None, "Invoice not found via QueryAgent"

            remaining = float(result.get("remaining_amount", 0))
            assert remaining >= 0.0, (
                f"QueryAgent returned negative remaining_amount={remaining}"
            )

            logger.info(
                f"QueryAgent validation passed: remaining_amount={remaining}"
            )

        finally:
            for suffix in ["", "-wal", "-shm"]:
                path = db_path + suffix
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
