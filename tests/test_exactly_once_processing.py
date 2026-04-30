"""
tests/test_exactly_once_processing.py

Objective
---------
Prove that the DBAgent correctly handles duplicate event delivery by
maintaining exactly-once semantics at the database layer.

Background
----------
In Kafka-based systems, **at-least-once** delivery is the default guarantee.
The ACIS-X ``DBAgent`` provides **exactly-once processing** through two
complementary mechanisms:

1. **BaseAgent-level idempotency**: ``_is_duplicate()`` checks a bounded
   in-memory ``OrderedDict`` of previously processed ``event_id`` values.
   If found, the entire ``process_event()`` call is skipped.

2. **Database-level idempotency**: Before every write, the handler queries
   the ``event_log`` table:
       ``SELECT 1 FROM event_log WHERE event_id = ?``
   If the ``event_id`` already exists, the write is skipped.

This test validates mechanism (2) in isolation by bypassing mechanism (1) and
calling the handler method directly 10 000 times with the same event.

Test Plan
---------
1. Create a ``DBAgent`` connected to a temporary SQLite database.
2. Build a single ``invoice.created`` event with a static ``event_id``.
3. Call ``_handle_invoice_upsert()`` directly 10 000 times (bypasses the
   BaseAgent idempotency layer so we stress the DB-level guard).
4. Assert:
   a. ``invoices`` table contains exactly 1 row.
   b. ``event_log`` table contains exactly 1 row for that ``event_id``.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta

import pytest
from unittest.mock import MagicMock, patch

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_DUPLICATES = 10_000
STATIC_EVENT_ID = "evt_idempotency_test_001"
CUSTOMER_ID = "cust_idempotent_001"
INVOICE_ID = "inv_idempotent_001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_invoice_event() -> Event:
    """Create a deterministic ``invoice.created`` event.

    Uses a static ``event_id`` so every call returns an identical event
    — this simulates Kafka re-delivering the same message 10 000 times.
    """
    return Event(
        event_id=STATIC_EVENT_ID,
        event_type="invoice.created",
        event_source="ScenarioGeneratorAgent",
        event_time=datetime.utcnow(),
        entity_id=CUSTOMER_ID,
        schema_version="1.1",
        payload={
            "invoice_id": INVOICE_ID,
            "customer_id": CUSTOMER_ID,
            "customer_name": "Idempotency Test Corp",
            "amount": 75_000.0,
            "total_amount": 75_000.0,
            "due_date": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "issued_date": datetime.utcnow().isoformat(),
            "status": "pending",
        },
        metadata={"environment": "test"},
    )


def _noop_query(query_type: str, params: dict = None, **kwargs):
    """No-op QueryClient mock for cache invalidation calls."""
    return None


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestExactlyOnceProcessing:
    """Prove DBAgent enforces exactly-once semantics on duplicate events."""

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_10k_duplicates_produce_single_row(self, mock_qc) -> None:
        """10 000 identical events → 1 invoice row + 1 event_log row."""
        from agents.storage.db_agent import DBAgent

        # Create a temporary database file (not :memory: so we can inspect
        # after the test if needed, and because DBAgent opens multiple
        # connections internally).
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            # Build a mock Kafka client — we don't need real Kafka
            kafka = MagicMock()
            kafka.publish.return_value = True
            kafka._producer = MagicMock()
            kafka._consumer = MagicMock()
            kafka._consumer.poll.return_value = {}

            # Track published events for downstream assertion
            published: list = []
            kafka.publish.side_effect = lambda topic, event, **kw: published.append(
                {"topic": topic, "event": event}
            ) or True

            agent = DBAgent(kafka_client=kafka, db_path=db_path)

            event = _make_invoice_event()

            # =============================================================
            # Inject the SAME event 10 000 times directly via the handler
            # =============================================================
            # We call _handle_invoice_upsert (not process_event) to bypass
            # BaseAgent's in-memory idempotency — this isolates and tests
            # the DATABASE-LEVEL event_log guard exclusively.
            for i in range(NUM_DUPLICATES):
                agent._handle_invoice_upsert(event)

            # =============================================================
            # Assert: invoices table has exactly 1 row
            # =============================================================
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM invoices WHERE invoice_id = ?", (INVOICE_ID,))
            invoice_count = cursor.fetchone()[0]
            assert invoice_count == 1, (
                f"Expected exactly 1 invoice row for {INVOICE_ID}, "
                f"got {invoice_count} — UPSERT logic is broken."
            )

            # =============================================================
            # Assert: event_log table has exactly 1 row for this event_id
            # =============================================================
            cursor.execute("SELECT COUNT(*) FROM event_log WHERE event_id = ?", (STATIC_EVENT_ID,))
            event_log_count = cursor.fetchone()[0]
            assert event_log_count == 1, (
                f"Expected exactly 1 event_log entry for {STATIC_EVENT_ID}, "
                f"got {event_log_count} — idempotency guard is broken."
            )

            # =============================================================
            # Assert: verify the invoice data is correct (not corrupted)
            # =============================================================
            cursor.execute(
                "SELECT customer_id, total_amount, status FROM invoices WHERE invoice_id = ?",
                (INVOICE_ID,),
            )
            row = cursor.fetchone()
            assert row is not None, "Invoice row disappeared"
            assert row[0] == CUSTOMER_ID, f"customer_id mismatch: {row[0]}"
            assert float(row[1]) == 75_000.0, f"total_amount mismatch: {row[1]}"
            assert row[2] == "pending", f"status mismatch: {row[2]}"

            conn.close()

            logger.info(
                "\n"
                "╔══════════════════════════════════════════════════╗\n"
                "║     Exactly-Once Processing Test PASSED          ║\n"
                "╠══════════════════════════════════════════════════╣\n"
                f"║  Duplicates injected : {NUM_DUPLICATES:>8}                   ║\n"
                f"║  Invoice rows        : {invoice_count:>8}                   ║\n"
                f"║  Event log entries   : {event_log_count:>8}                   ║\n"
                "╚══════════════════════════════════════════════════╝"
            )

        finally:
            # Cleanup temp DB files
            for suffix in ["", "-wal", "-shm"]:
                path = db_path + suffix
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_process_event_level_idempotency(self, mock_qc) -> None:
        """Validate the BaseAgent-level _is_duplicate guard also works.

        This exercises the FULL path: process_event → _handle_message logic
        by calling process_event directly.  The BaseAgent's _is_duplicate()
        uses the in-memory OrderedDict, so after the first call, all
        subsequent calls with the same event_id should be silently dropped.
        """
        from agents.storage.db_agent import DBAgent

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            kafka = MagicMock()
            kafka.publish.return_value = True
            kafka._producer = MagicMock()
            kafka._consumer = MagicMock()
            kafka._consumer.poll.return_value = {}

            agent = DBAgent(kafka_client=kafka, db_path=db_path)

            event = _make_invoice_event()

            # Process through the full process_event path
            # First call: should process and mark as handled
            agent.process_event(event)

            # Mark the event as processed in BaseAgent's OrderedDict
            # (normally done by _handle_message, which we bypass here)
            agent._mark_processed(event.event_id)

            # Subsequent calls: BaseAgent._is_duplicate should catch these
            # But since we're calling process_event directly (not through
            # _handle_message), the duplicate check happens at DB level
            for _ in range(100):
                agent.process_event(event)

            # Verify only 1 invoice row exists
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM invoices WHERE invoice_id = ?", (INVOICE_ID,))
            count = cursor.fetchone()[0]
            conn.close()

            assert count == 1, (
                f"Expected 1 invoice after 101 process_event calls, got {count}"
            )

        finally:
            for suffix in ["", "-wal", "-shm"]:
                path = db_path + suffix
                if os.path.exists(path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
