#!/usr/bin/env python3
"""Test final MemoryAgent fixes: bounded deque and startup rebuild."""

import os
import sqlite3
from datetime import datetime
from unittest.mock import Mock
from agents.storage.memory_agent import MemoryAgent
from agents.storage.query_agent import QueryAgent
from agents.storage.db_agent import DBAgent


def create_mock_kafka():
    """Create a mock Kafka client."""
    mock = Mock()
    mock.publish = Mock()
    return mock


def setup_test_db():
    """Create test database with customers and invoices."""
    if os.path.exists("acis_test.db"):
        os.remove("acis_test.db")

    db_agent = DBAgent(kafka_client=None, db_path="acis_test.db")
    query_agent = QueryAgent(kafka_client=None, db_path="acis_test.db")

    conn = sqlite3.connect("acis_test.db")
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Create 3 customers
    for i in range(1, 4):
        cursor.execute("""
            INSERT INTO customers (customer_id, risk_score, created_at, updated_at)
            VALUES (?, ?, ?, ?)
        """, (f"cust-{i}", 0.1 * i, now, now))

        # Create 2 invoices per customer
        for j in range(1, 3):
            cursor.execute("""
                INSERT INTO invoices (
                    invoice_id, customer_id, total_amount, paid_amount,
                    issued_date, due_date, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (f"inv-{i}-{j}", f"cust-{i}", 1000.0, 0, now, now, "pending", now, now))

    conn.commit()
    conn.close()

    return query_agent


def test_bounded_deque():
    """Test that processed_events uses bounded deque (no memory leak)."""
    print("\n=== Testing Bounded Deque ===")

    query_agent = setup_test_db()
    memory_agent = MemoryAgent(kafka_client=create_mock_kafka(), query_agent=query_agent)

    # Verify deque has maxlen
    if hasattr(memory_agent.processed_events, 'maxlen'):
        print(f"[OK] Deque has maxlen: {memory_agent.processed_events.maxlen}")
        if memory_agent.processed_events.maxlen == 10000:
            print("[OK] PASS: maxlen = 10000 (bounded)")
            return True
        else:
            print("[FAIL] maxlen incorrect")
            return False
    else:
        print("[FAIL] processed_events is not a deque")
        return False


def test_startup_rebuild():
    """Test that rebuild_state_from_db() populates all customers."""
    print("\n=== Testing Startup Rebuild ===")

    query_agent = setup_test_db()
    memory_agent = MemoryAgent(kafka_client=create_mock_kafka(), query_agent=query_agent)

    # Check initial state is empty
    if len(memory_agent.customer_state) == 0:
        print("[OK] Initial state is empty")
    else:
        print("[FAIL] Initial state not empty")
        return False

    # Rebuild from DB
    memory_agent.rebuild_state_from_db()

    # Check state was populated
    print(f"[OK] After rebuild: {len(memory_agent.customer_state)} customers loaded")

    if len(memory_agent.customer_state) == 3:
        print("[OK] PASS: All 3 customers loaded")

        # Verify all customers have correct data
        for i in range(1, 4):
            cust_id = f"cust-{i}"
            if cust_id in memory_agent.customer_state:
                state = memory_agent.customer_state[cust_id]
                print(f"  - {cust_id}: outstanding={state['total_outstanding']}, overdue={state['overdue_count']}")

                # Each customer has 2 invoices of 1000 each = 2000 outstanding
                if state["total_outstanding"] == 2000.0 and state["overdue_count"] == 0:
                    print(f"    [OK] State is correct")
                else:
                    print(f"    [FAIL] State incorrect")
                    return False
            else:
                print(f"  [FAIL] {cust_id} not in state")
                return False

        return True
    else:
        print(f"[FAIL] Wrong count: expected 3, got {len(memory_agent.customer_state)}")
        return False


if __name__ == "__main__":
    try:
        results = [
            test_bounded_deque(),
            test_startup_rebuild(),
        ]

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
        print(f"[FAIL] Test failed: {e}")
        import traceback
        traceback.print_exc()
