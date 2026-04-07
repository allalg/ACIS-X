#!/usr/bin/env python3
"""Test script to validate database schema and query functionality."""

import sqlite3
import sys
from datetime import datetime
from agents.storage.db_agent import DBAgent
from agents.storage.query_agent import QueryAgent


def test_schema_creation():
    """Test that schema is created correctly."""
    print("\n=== Testing Schema Creation ===")

    # Clean up old db
    import os
    if os.path.exists("acis_test.db"):
        os.remove("acis_test.db")

    # Create DBAgent (which initializes schema)
    mock_kafka = None  # QueryAgent doesn't use kafka in tests
    db_agent = DBAgent(kafka_client=mock_kafka, db_path="acis_test.db")

    # Verify tables and columns exist
    conn = sqlite3.connect("acis_test.db")
    cursor = conn.cursor()

    # Check invoices table columns
    cursor.execute("PRAGMA table_info(invoices)")
    columns = {row[1] for row in cursor.fetchall()}
    expected_cols = {'invoice_id', 'customer_id', 'total_amount', 'paid_amount',
                     'issued_date', 'due_date', 'status', 'created_at', 'updated_at'}

    if expected_cols.issubset(columns):
        print("[OK] Invoice table has correct columns:", expected_cols & columns)
    else:
        print("[FAIL] Missing columns:", expected_cols - columns)
        return False

    # Check indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
    indexes = {row[0] for row in cursor.fetchall()}
    expected_indexes = {'idx_invoice_id', 'idx_invoice_customer'}

    if expected_indexes.issubset(indexes):
        print("[OK] Indexes exist:", expected_indexes & indexes)
    else:
        print("[FAIL] Missing indexes:", expected_indexes - indexes)
        return False

    conn.close()
    print("[OK] Schema creation test PASSED")
    return True


def test_queries():
    """Test that queries work correctly with new schema."""
    print("\n=== Testing Queries ===")

    import os
    if os.path.exists("acis_test.db"):
        os.remove("acis_test.db")

    db_agent = DBAgent(kafka_client=None, db_path="acis_test.db")
    query_agent = QueryAgent(kafka_client=None, db_path="acis_test.db")

    # Insert test data directly
    conn = sqlite3.connect("acis_test.db")
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Create customer
    cursor.execute("""
        INSERT INTO customers (customer_id, created_at, updated_at)
        VALUES (?, ?, ?)
    """, ("cust-1", now, now))

    # Create invoices
    cursor.execute("""
        INSERT INTO invoices (
            invoice_id, customer_id, total_amount, paid_amount,
            issued_date, due_date, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-1", "cust-1", 1000.0, 0, now, now, "pending", now, now))

    cursor.execute("""
        INSERT INTO invoices (
            invoice_id, customer_id, total_amount, paid_amount,
            issued_date, due_date, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-2", "cust-1", 500.0, 200.0, now, now, "partial", now, now))

    cursor.execute("""
        INSERT INTO invoices (
            invoice_id, customer_id, total_amount, paid_amount,
            issued_date, due_date, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-3", "cust-1", 300.0, 300.0, now, now, "paid", now, now))

    conn.commit()
    conn.close()

    # Test get_invoice
    print("\nTesting get_invoice()...")
    invoice = query_agent.get_invoice("inv-1")
    if invoice:
        print("[OK] Got invoice: %s" % invoice['invoice_id'])
        print("    - total_amount: %s" % invoice['total_amount'])
        print("    - paid_amount: %s" % invoice['paid_amount'])
        print("    - remaining_amount: %s" % invoice.get('remaining_amount'))

        if invoice.get('remaining_amount') == 1000.0:
            print("    [OK] remaining_amount computed correctly (1000 - 0 = 1000)")
        else:
            print("    [FAIL] remaining_amount incorrect: expected 1000.0, got %s" % invoice.get('remaining_amount'))
            return False
    else:
        print("  [FAIL] Failed to get invoice")
        return False

    # Test get_invoices_by_customer
    print("\nTesting get_invoices_by_customer()...")
    invoices = query_agent.get_invoices_by_customer("cust-1")
    print("[OK] Got %d non-paid invoices" % len(invoices))
    for inv in invoices:
        print("    - %s: total=%s, paid=%s, remaining=%s, status=%s" % (
            inv['invoice_id'], inv['total_amount'], inv['paid_amount'],
            inv.get('remaining_amount'), inv['status']))

    if len(invoices) == 2:  # inv-1 and inv-2 (inv-3 is paid)
        print("  [OK] Correct count (2 non-paid invoices)")
    else:
        print("  [FAIL] Wrong count: expected 2, got %d" % len(invoices))
        return False

    # Test get_all_invoices_by_customer
    print("\nTesting get_all_invoices_by_customer()...")
    all_invoices = query_agent.get_all_invoices_by_customer("cust-1")
    print("[OK] Got %d total invoices" % len(all_invoices))

    if len(all_invoices) == 3:
        print("  [OK] Correct count (3 total invoices)")
    else:
        print("  [FAIL] Wrong count: expected 3, got %d" % len(all_invoices))
        return False

    # Test get_unpaid_invoices
    print("\nTesting get_unpaid_invoices()...")
    unpaid = query_agent.get_unpaid_invoices()
    print("[OK] Got %d pending invoices" % len(unpaid))

    if len(unpaid) == 1:  # Only inv-1 is pending
        print("  [OK] Correct count (1 pending invoice)")
    else:
        print("  [FAIL] Wrong count: expected 1, got %d" % len(unpaid))
        return False

    print("\n[OK] Query tests PASSED")
    return True


if __name__ == "__main__":
    try:
        success = test_schema_creation() and test_queries()

        # Cleanup
        import os
        if os.path.exists("acis_test.db"):
            os.remove("acis_test.db")

        if success:
            print("\n" + "="*50)
            print("ALL TESTS PASSED")
            print("="*50)
            sys.exit(0)
        else:
            print("\n" + "="*50)
            print("SOME TESTS FAILED")
            print("="*50)
            sys.exit(1)
    except Exception as e:
        print("\n[FAIL] Test failed with exception: %s" % str(e))
        import traceback
        traceback.print_exc()
        sys.exit(1)
