"""Integration tests for the ACIS-X risk profile pipeline.

These tests exercise the full write path without a live Kafka broker:
  AggregatorAgent (event_type="risk.profile.updated")
      →  DBAgent._handle_customer_risk_profile()
      →  customer_risk_profile table

Tests run against a temp on-disk SQLite database that is created and
torn-down per test function, so they are safe to run in parallel.
"""

import sqlite3
import tempfile
import os
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from agents.storage.db_agent import DBAgent
from schemas.event_schema import Event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_kafka_client() -> MagicMock:
    """Return a minimal mock Kafka client that satisfies BaseAgent's interface."""
    client = MagicMock()
    client.subscribe.return_value = None
    client.publish.return_value = None
    client.poll.return_value = []
    client.commit.return_value = None
    client.close.return_value = None
    return client


def _make_risk_event(
    customer_id: str,
    company_name: str,
    financial_risk: float,
    litigation_risk: float,
    combined_risk: float,
    event_id: str | None = None,
) -> Event:
    """Build a synthetic risk.profile.updated Event as AggregatorAgent would emit."""
    return Event(
        event_id=event_id or f"evt_{uuid.uuid4()}",
        event_type="risk.profile.updated",  # must match AggregatorAgent exactly
        event_source="AggregatorAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        schema_version="1.1",
        correlation_id=f"corr_{uuid.uuid4()}",
        payload={
            "customer_id": customer_id,
            "company_name": company_name,
            "financial_risk": financial_risk,
            "litigation_risk": litigation_risk,
            "combined_risk": combined_risk,
            "severity": "medium",
            "confidence": 0.85,
            "financial_source": "NSE",
            "litigation_source": "NCLT",
            "generated_at": datetime.utcnow().isoformat(),
        },
        metadata={"environment": "test"},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_agent_with_temp_db():
    """
    Spin up a DBAgent backed by a fresh temp SQLite file.

    Yields (db_agent, db_path).  The temp file is deleted after the test.
    """
    kafka_client = _make_kafka_client()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    try:
        agent = DBAgent(kafka_client=kafka_client, db_path=db_path)
        yield agent, db_path
    finally:
        # Best-effort cleanup — Windows may keep the file locked briefly
        try:
            os.unlink(db_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Test 1: Schema initialisation
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_db_agent_schema_creates_customer_risk_profile_table(db_agent_with_temp_db):
    """DBAgent._init_database() must create the customer_risk_profile table."""
    _, db_path = db_agent_with_temp_db

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='customer_risk_profile'"
    )
    row = cursor.fetchone()
    conn.close()

    assert row is not None, "customer_risk_profile table must exist after schema init"


# ---------------------------------------------------------------------------
# Test 2: Full write-path integration
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_handle_customer_risk_profile_persists_correct_values(db_agent_with_temp_db):
    """
    Calling DBAgent._handle_customer_risk_profile() with a risk.profile.updated
    Event must upsert a row into customer_risk_profile with the correct values.
    """
    agent, db_path = db_agent_with_temp_db

    customer_id = "cust_00042"
    company_name = "Infosys Ltd"
    financial_risk = 0.65
    litigation_risk = 0.30
    combined_risk = 0.51

    event = _make_risk_event(
        customer_id=customer_id,
        company_name=company_name,
        financial_risk=financial_risk,
        litigation_risk=litigation_risk,
        combined_risk=combined_risk,
    )

    # Ensure the parent customer row exists (FK constraint)
    conn = sqlite3.connect(db_path)
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO customers (customer_id, name, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (customer_id, company_name, now, now),
    )
    conn.commit()
    conn.close()

    # Exercise the handler directly (no Kafka broker needed).
    # Patch QueryClient.query so the cache-invalidation tail-call is a no-op
    # (the live implementation would time-out waiting for a Kafka response).
    with patch("agents.storage.db_agent.QueryClient.query", return_value=None):
        agent._handle_customer_risk_profile(event)

    # --- Assert the row was persisted ---
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM customer_risk_profile WHERE customer_id = ?",
        (customer_id,),
    ).fetchone()
    conn.close()

    assert row is not None, f"Expected a customer_risk_profile row for {customer_id}"
    assert row["customer_id"] == customer_id
    assert row["company_name"] == company_name
    assert abs(row["financial_risk"] - financial_risk) < 1e-9, (
        f"financial_risk mismatch: stored={row['financial_risk']}, expected={financial_risk}"
    )
    assert abs(row["litigation_risk"] - litigation_risk) < 1e-9, (
        f"litigation_risk mismatch: stored={row['litigation_risk']}, expected={litigation_risk}"
    )
    assert abs(row["combined_risk"] - combined_risk) < 1e-9, (
        f"combined_risk mismatch: stored={row['combined_risk']}, expected={combined_risk}"
    )


# ---------------------------------------------------------------------------
# Test 3: Idempotency — duplicate event_id must not create a second row
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_handle_customer_risk_profile_is_idempotent(db_agent_with_temp_db):
    """Sending the same event twice must result in exactly one row."""
    agent, db_path = db_agent_with_temp_db

    customer_id = "cust_00099"
    fixed_event_id = f"evt_{uuid.uuid4()}"

    # Seed parent customer
    conn = sqlite3.connect(db_path)
    now = datetime.utcnow().isoformat()
    conn.execute(
        "INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at) VALUES (?, ?, ?)",
        (customer_id, now, now),
    )
    conn.commit()
    conn.close()

    event = _make_risk_event(
        customer_id=customer_id,
        company_name="Wipro Ltd",
        financial_risk=0.4,
        litigation_risk=0.2,
        combined_risk=0.35,
        event_id=fixed_event_id,
    )

    with patch("agents.storage.db_agent.QueryClient.query", return_value=None):
        agent._handle_customer_risk_profile(event)
        agent._handle_customer_risk_profile(event)  # duplicate

    conn = sqlite3.connect(db_path)
    count = conn.execute(
        "SELECT COUNT(*) FROM customer_risk_profile WHERE customer_id = ?",
        (customer_id,),
    ).fetchone()[0]
    conn.close()

    assert count == 1, f"Duplicate event must not create extra rows; got {count}"


# ---------------------------------------------------------------------------
# Test 4: event_type contract — AggregatorAgent ↔ DBAgent must match exactly
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_event_type_contract_aggregator_to_db_agent():
    """
    Verify that the event_type string published by AggregatorAgent
    ("risk.profile.updated") exactly matches the string DBAgent checks in
    process_event() for routing to _handle_customer_risk_profile().

    This test is a static contract check — it does NOT require a live agent.
    If either side changes the string without updating the other, this test
    fails immediately, surfacing the contract break before any runtime failure.
    """
    import inspect
    from agents.intelligence import aggregator_agent as agg_module
    from agents.storage import db_agent as db_module

    EXPECTED_EVENT_TYPE = "risk.profile.updated"

    # --- AggregatorAgent side: confirm it publishes the expected event_type ---
    agg_source = inspect.getsource(agg_module)
    assert f'event_type="{EXPECTED_EVENT_TYPE}"' in agg_source or \
           f"event_type='{EXPECTED_EVENT_TYPE}'" in agg_source, (
        f"AggregatorAgent must publish event_type='{EXPECTED_EVENT_TYPE}'"
    )

    # --- DBAgent side: confirm it routes on the expected event_type ---
    db_source = inspect.getsource(db_module)
    assert f'"{EXPECTED_EVENT_TYPE}"' in db_source or \
           f"'{EXPECTED_EVENT_TYPE}'" in db_source, (
        f"DBAgent must check event_type='{EXPECTED_EVENT_TYPE}' in process_event()"
    )


# ---------------------------------------------------------------------------
# Test 5: AgentSupervisor accepts and stores launch_fn
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_agent_supervisor_stores_launch_fn():
    """AgentSupervisor.__init__ must accept and persist launch_fn."""
    from runtime.agent_supervisor import AgentSupervisor

    dummy_fn = lambda agent_class, kwargs: None

    supervisor = AgentSupervisor(launch_fn=dummy_fn)
    assert supervisor._launch_fn is dummy_fn, (
        "AgentSupervisor must store launch_fn as self._launch_fn"
    )


@pytest.mark.integration
def test_agent_supervisor_restart_without_launch_fn_returns_false():
    """restart_agent() must return False and log an error when launch_fn is None."""
    from runtime.agent_supervisor import AgentSupervisor
    import multiprocessing

    supervisor = AgentSupervisor(launch_fn=None)

    # Register a dummy agent entry so the name is known
    dummy_process = MagicMock(spec=multiprocessing.Process)
    dummy_process.is_alive.return_value = False
    supervisor.register(
        agent_name="DummyAgent",
        process=dummy_process,
        agent_class=MagicMock,
        kwargs={},
    )

    result = supervisor.restart_agent("DummyAgent")
    assert result is False, (
        "restart_agent() must return False when no launch_fn is configured"
    )
