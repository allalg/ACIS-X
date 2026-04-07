#!/usr/bin/env python3
"""Test script to validate MemoryAgent fixes for Kafka conditions."""

import os
import sqlite3
from datetime import datetime
from unittest.mock import Mock, MagicMock
from agents.storage.memory_agent import MemoryAgent
from agents.storage.query_agent import QueryAgent
from agents.storage.db_agent import DBAgent
from schemas.event_schema import Event


def create_mock_kafka():
    """Create a mock Kafka client that ignores publish calls."""
    mock = Mock()
    mock.publish = MagicMock()
    return mock


def setup_test_db():
    """Create test database and insert test data."""
    if os.path.exists("acis_test.db"):
        os.remove("acis_test.db")

    db_agent = DBAgent(kafka_client=None, db_path="acis_test.db")
    query_agent = QueryAgent(kafka_client=None, db_path="acis_test.db")

    # Insert test customer and invoices
    conn = sqlite3.connect("acis_test.db")
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Create customer
    cursor.execute("""
        INSERT INTO customers (customer_id, created_at, updated_at)
        VALUES (?, ?, ?)
    """, ("cust-1", now, now))

    # Create invoice (total=1000)
    cursor.execute("""
        INSERT INTO invoices (
            invoice_id, customer_id, total_amount, paid_amount,
            issued_date, due_date, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-1", "cust-1", 1000.0, 0, now, now, "pending", now, now))

    conn.commit()
    conn.close()

    return db_agent, query_agent


def test_deduplication():
    """Test that duplicate events are not processed twice."""
    print("\n=== Testing Deduplication ===")

    db_agent, query_agent = setup_test_db()
    memory_agent = MemoryAgent(kafka_client=create_mock_kafka(), query_agent=query_agent)

    # Create two identical events with same event_id
    event1 = Event(
        event_id="evt-1",
        event_type="invoice.created",
        event_source="test",
        event_time=datetime.utcnow(),
        correlation_id="corr-1",
        entity_id="inv-1",
        schema_version="1.1",
        payload={
            "customer_id": "cust-1",
            "invoice_id": "inv-1",
            "amount": 500.0,
        }
    )

    # Process first time
    memory_agent.process_event(event1)
    state1 = memory_agent.get_customer_state("cust-1")
    print("[OK] First event processed")
    print("     State:", state1)

    # Process duplicate
    memory_agent.process_event(event1)
    state2 = memory_agent.get_customer_state("cust-1")
    print("[OK] Duplicate event processed")
    print("     State:", state2)

    if state1 == state2:
        print("[OK] PASS: Duplicate event had no effect (states identical)")
        return True
    else:
        print("[FAIL] States differ - deduplication failed!")
        return False


def test_recompute_from_db():
    """Test that state is recomputed from DB, not incremented."""
    print("\n=== Testing Recomputation from DB ===")

    db_agent, query_agent = setup_test_db()
    memory_agent = MemoryAgent(kafka_client=create_mock_kafka(), query_agent=query_agent)

    # Insert payment into DB (bypass event, simulating out-of-order)
    conn = sqlite3.connect("acis_test.db")
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    cursor.execute("""
        INSERT INTO payments (
            payment_id, invoice_id, customer_id, amount, payment_date, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, ("pay-1", "inv-1", "cust-1", 500.0, now, now))

    # Update invoice: paid_amount = 500, status = partial
    cursor.execute("""
        UPDATE invoices
        SET paid_amount = 500, status = 'partial'
        WHERE invoice_id = ?
    """, ("inv-1",))

    conn.commit()
    conn.close()

    # Now process FAKE payment event with out-of-order delivery
    # (payment arrived before invoice, but DB is already consistent)
    event = Event(
        event_id="evt-2",
        event_type="payment.received",
        event_source="test",
        event_time=datetime.utcnow(),
        correlation_id="corr-2",
        entity_id="inv-1",
        schema_version="1.1",
        payload={
            "customer_id": "cust-1",
            "invoice_id": "inv-1",
            "payment_id": "pay-2",
            "amount": 500.0,
        }
    )

    memory_agent.process_event(event)
    state = memory_agent.get_customer_state("cust-1")

    print("[OK] Payment event processed")
    print("     State:", state)

    # Check that outstanding was recomputed from DB (not incremented)
    # remaining_amount = 1000 - 500 = 500
    if state and state["total_outstanding"] == 500.0:
        print("[OK] PASS: Outstanding correctly recomputed from DB (500.0)")
        return True
    else:
        outstanding = state["total_outstanding"] if state else "None"
        print("[FAIL] Outstanding incorrect - expected 500.0, got %s" % outstanding)
        return False


def test_idempotency_key():
    """Test that event_id prevents duplicate processing."""
    print("\n=== Testing Idempotency Key ===")

    db_agent, query_agent = setup_test_db()
    memory_agent = MemoryAgent(kafka_client=create_mock_kafka(), query_agent=query_agent)

    # Check processed_events set is initially empty
    if len(memory_agent.processed_events) == 0:
        print("[OK] Processed events set is empty")
    else:
        print("[FAIL] Processed events set not empty")
        return False

    # Create event
    event = Event(
        event_id="evt-unique-1",
        event_type="invoice.created",
        event_source="test",
        event_time=datetime.utcnow(),
        correlation_id="corr-1",
        entity_id="inv-1",
        schema_version="1.1",
        payload={"customer_id": "cust-1", "invoice_id": "inv-1", "amount": 100.0}
    )

    # Process event
    memory_agent.process_event(event)

    # Check event_id is in processed set
    if "evt-unique-1" in memory_agent.processed_events:
        print("[OK] Event ID added to processed set")
    else:
        print("[FAIL] Event ID not in processed set")
        return False

    # Try to process exact same event again
    memory_agent.process_event(event)

    if len(memory_agent.processed_events) == 1:
        print("[OK] PASS: Duplicate not added to set")
        return True
    else:
        print("[FAIL] Duplicate was added to set")
        return False


if __name__ == "__main__":
    try:
        results = [
            test_deduplication(),
            test_recompute_from_db(),
            test_idempotency_key(),
        ]

        # Cleanup
        if os.path.exists("acis_test.db"):
            os.remove("acis_test.db")

        print("\n" + "=" * 50)
        if all(results):
            print("ALL TESTS PASSED")
            print("=" * 50)
        else:
            print("SOME TESTS FAILED")
            print("=" * 50)

    except Exception as e:
        print("[FAIL] Test failed with exception: %s" % str(e))
        import traceback
        traceback.print_exc()
