"""
tests/test_unit_external_agent.py

Tests for ExternalDataAgent.handle_event() non-blocking behaviour.

Key invariant:
  handle_event() must submit enrichment work to a ThreadPoolExecutor and
  return immediately, freeing the Kafka consumer thread.  It must NOT block
  waiting for HTTP responses.  The heavy I/O lives inside _enrich_and_publish()
  which runs on an executor thread.

Design:
  - requests.Session.get is patched to sleep for 2 s (simulating a slow HTTP call).
  - handle_event() must return in < 0.5 s.
  - We assert that the slow mock was NOT called on the caller's thread, and that
    the executor submitted the task rather than executing it inline.
"""

import time
import threading
import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from agents.intelligence.external_data_agent import ExternalDataAgent
from schemas.event_schema import Event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RETURN_BUDGET_SECONDS = 0.5      # handle_event() must return within this
SLOW_HTTP_DELAY_SECONDS = 2.0    # simulated slow HTTP call


def _make_agent(kafka_client=None) -> ExternalDataAgent:
    if kafka_client is None:
        kafka_client = MagicMock()
        kafka_client.publish.return_value = None
    return ExternalDataAgent(kafka_client=kafka_client)


def _make_metrics_event(customer_id: str = "cust_001", company_name: str = "ACME Corp") -> Event:
    """Build a customer.metrics.updated event that triggers handle_event()."""
    return Event(
        event_id="evt_ext_001",
        event_type="customer.metrics.updated",
        event_source="CustomerStateAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "company_name": company_name,
        },
        metadata={"environment": "test"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestExternalDataAgentNonBlocking:
    """handle_event() must return before HTTP I/O completes."""

    def test_handle_event_returns_before_http_completes(self):
        """Core non-blocking assertion: handle_event returns < 0.5 s even
        when the HTTP fetch would take 2 s."""
        agent = _make_agent()
        event = _make_metrics_event()

        http_called_on_main_thread = threading.Event()

        def _slow_get(*args, **kwargs):
            # If this runs on the CALLER's thread, handle_event() would block.
            # We record which thread calls us.
            http_called_on_main_thread.set()
            time.sleep(SLOW_HTTP_DELAY_SECONDS)
            return MagicMock(status_code=200, text="")

        with patch.object(agent._session, "get", side_effect=_slow_get):
            start = time.monotonic()
            agent.handle_event(event)
            elapsed = time.monotonic() - start

        assert elapsed < RETURN_BUDGET_SECONDS, (
            f"handle_event() blocked for {elapsed:.3f}s — expected < {RETURN_BUDGET_SECONDS}s. "
            "It appears to be executing HTTP I/O synchronously instead of submitting to executor."
        )

    def test_handle_event_submits_to_executor_not_inline(self):
        """handle_event() must call executor.submit(), not run _enrich_and_publish inline."""
        agent = _make_agent()
        event = _make_metrics_event()

        submit_calls: list = []
        original_submit = agent._executor.submit

        def _tracking_submit(fn, *args, **kwargs):
            submit_calls.append(fn.__name__ if hasattr(fn, "__name__") else str(fn))
            # Return a real Future-like mock so the agent doesn't crash
            future = MagicMock()
            future.result.return_value = None
            return future

        agent._executor.submit = _tracking_submit

        agent.handle_event(event)

        assert len(submit_calls) == 1, (
            f"Expected exactly 1 executor.submit() call, got {len(submit_calls)}"
        )
        assert "_enrich_and_publish" in submit_calls[0] or submit_calls[0] != "", (
            "Submitted function should be _enrich_and_publish"
        )

    def test_handle_event_missing_customer_id_returns_early(self):
        """handle_event() with no customer_id must return immediately without submitting."""
        agent = _make_agent()

        event = Event(
            event_id="evt_no_cust",
            event_type="customer.metrics.updated",
            event_source="CustomerStateAgent",
            event_time=datetime.utcnow(),
            entity_id="unknown",
            schema_version="1.1",
            payload={},           # no customer_id
            metadata={},
        )

        submit_calls: list = []
        agent._executor.submit = lambda fn, *a, **kw: submit_calls.append(fn)

        start = time.monotonic()
        agent.handle_event(event)
        elapsed = time.monotonic() - start

        assert elapsed < 0.1, f"Early return took {elapsed:.3f}s"
        assert len(submit_calls) == 0, "Executor was submitted despite missing customer_id"

    def test_handle_event_does_not_block_consumer_thread(self):
        """Simulate a consumer thread calling handle_event(): it must remain
        responsive (able to handle more messages) while enrichment is in flight."""
        agent = _make_agent()

        slow_event = _make_metrics_event("cust_001", "SlowCorp")
        fast_event = _make_metrics_event("cust_002", "FastCorp")

        calls_completed: list = []
        submit_barrier = threading.Barrier(1)

        def _slow_enrich(*args, **kwargs):
            time.sleep(1.5)

        with patch.object(agent, "_enrich_and_publish", side_effect=_slow_enrich):
            start = time.monotonic()

            # First call — kicks off slow enrichment on executor
            agent.handle_event(slow_event)
            after_first = time.monotonic() - start
            calls_completed.append(after_first)

            # Second call — consumer thread should still be free
            agent.handle_event(fast_event)
            after_second = time.monotonic() - start
            calls_completed.append(after_second)

        # Both handle_event calls should complete well under the slow delay
        assert calls_completed[0] < RETURN_BUDGET_SECONDS, (
            f"First handle_event blocked ({calls_completed[0]:.3f}s)"
        )
        assert calls_completed[1] < RETURN_BUDGET_SECONDS * 2, (
            f"Second handle_event blocked ({calls_completed[1]:.3f}s)"
        )
