"""Unit tests for database schema and queries (no Kafka broker needed)."""

import pytest
import sqlite3
from datetime import datetime
from agents.storage.db_agent import DBAgent
from agents.storage.query_agent import QueryAgent


@pytest.mark.unit
@pytest.mark.schema
def test_schema_creation(db_agent):
    """Test that database schema is created correctly."""
    # db_agent fixture creates the schema automatically
    conn = sqlite3.connect(db_agent._db_path)
    cursor = conn.cursor()

    # Check customers table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='customers'"
    )
    result = cursor.fetchone()
    assert result is not None, "customers table should be created"

    # Check invoices table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='invoices'"
    )
    result = cursor.fetchone()
    assert result is not None, "invoices table should be created"

    conn.close()


@pytest.mark.unit
@pytest.mark.schema
def test_customer_insertion(db_agent):
    """Test that customers can be inserted and retrieved via QueryAgent."""
    # Insert test customer via direct SQL
    conn = sqlite3.connect(db_agent._db_path)
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    cursor.execute("""
        INSERT INTO customers (customer_id, name, credit_limit, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, ("cust-1", "Test Company", 50000, now, now))
    conn.commit()
    conn.close()

    # Verify QueryAgent can retrieve it
    query_agent = QueryAgent(kafka_client=db_agent.kafka_client, db_path=db_agent._db_path)
    customer = query_agent.get_customer("cust-1")

    assert customer is not None, "Customer should be found"
    assert customer["customer_id"] == "cust-1"
    assert customer["name"] == "Test Company"
    assert customer["credit_limit"] == 50000


@pytest.mark.unit
@pytest.mark.schema
def test_invoice_queries(db_agent):
    """Test invoice query methods."""
    conn = sqlite3.connect(db_agent._db_path)
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Create test customer
    cursor.execute("""
        INSERT INTO customers (customer_id, created_at, updated_at)
        VALUES (?, ?, ?)
    """, ("cust-inv", now, now))

    # Insert invoices with different statuses
    cursor.execute("""
        INSERT INTO invoices
        (invoice_id, customer_id, total_amount, paid_amount, issued_date, due_date, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-1", "cust-inv", 1000.0, 0, now, now, "pending", now, now))

    cursor.execute("""
        INSERT INTO invoices
        (invoice_id, customer_id, total_amount, paid_amount, issued_date, due_date, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-2", "cust-inv", 500.0, 200.0, now, now, "partial", now, now))

    cursor.execute("""
        INSERT INTO invoices
        (invoice_id, customer_id, total_amount, paid_amount, issued_date, due_date, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, ("inv-3", "cust-inv", 300.0, 300.0, now, now, "paid", now, now))

    conn.commit()
    conn.close()

    # Test QueryAgent methods
    query_agent = QueryAgent(kafka_client=db_agent.kafka_client, db_path=db_agent._db_path)

    # Get single invoice
    invoice = query_agent.get_invoice("inv-1")
    assert invoice is not None
    assert invoice["total_amount"] == 1000.0

    # Get invoices by customer (non-paid only)
    invoices = query_agent.get_invoices_by_customer("cust-inv")
    assert len(invoices) >= 2, "Should return at least non-paid invoices"

    # Get all invoices for customer
    all_invoices = query_agent.get_all_invoices_by_customer("cust-inv")
    assert len(all_invoices) >= 3, "Should return all invoices"


@pytest.mark.unit
def test_get_customer_with_enrichment_fields(db_agent):
    """Test that get_customer returns enrichment fields (name, credit_limit)."""
    conn = sqlite3.connect(db_agent._db_path)
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    cursor.execute("""
        INSERT INTO customers (customer_id, name, credit_limit, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, ("cust-enriched", "Acme Corp", 100000, now, now))
    conn.commit()
    conn.close()

    query_agent = QueryAgent(kafka_client=db_agent.kafka_client, db_path=db_agent._db_path)
    customer = query_agent.get_customer("cust-enriched")

    assert customer is not None
    assert "name" in customer, "Query should include name field"
    assert "credit_limit" in customer, "Query should include credit_limit field"
    assert customer["name"] == "Acme Corp"
    assert customer["credit_limit"] == 100000

