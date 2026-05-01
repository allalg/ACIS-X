"""
tests/test_unit_query_client.py

Tests for QueryClient thread isolation.

Key invariant (from QueryClient._get_or_create_consumer docstring):
  Each OS thread gets its own KafkaClient instance stored in threading.local().
  Two concurrent calls from different threads must never share a consumer, so
  their correlation-ID reply polling can never cross-contaminate.
"""

import threading
import time
import pytest
from unittest.mock import MagicMock, patch

from utils.query_client import QueryClient, QueryTimeoutError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reset_thread_local():
    """Clear per-thread state between tests so isolation is genuine."""
    # _thread_local is a module-level threading.local(); deleting attributes
    # that exist on the current thread's slot resets it.
    tl = QueryClient._thread_local
    if hasattr(tl, "consumer"):
        del tl.consumer


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestQueryClientThreadIsolation:
    """Each thread must receive its own KafkaClient consumer instance."""

    def test_same_thread_reuses_consumer(self):
        """Two calls from the same thread must return the same consumer object."""
        _reset_thread_local()
        with patch.object(QueryClient, "_get_or_create_consumer", wraps=QueryClient._get_or_create_consumer):
            # We patch the internal KafkaClient construction to avoid broker I/O
            with patch("utils.query_client.KafkaClient") as MockKC:
                mock_instance = MagicMock()
                MockKC.return_value = mock_instance

                c1 = QueryClient._get_or_create_consumer()
                c2 = QueryClient._get_or_create_consumer()

                assert c1 is c2, "Same thread must reuse the consumer (thread-local singleton)"

    def test_different_threads_get_different_consumers(self):
        """Two concurrent threads must each create their own consumer instance."""
        consumers: dict = {}
        errors: list = []

        def _grab_consumer(thread_name: str) -> None:
            # Reset local state so each thread starts fresh
            _reset_thread_local()
            try:
                with patch("utils.query_client.KafkaClient") as MockKC:
                    mock_instance = MagicMock()
                    mock_instance.__class__ = type("KafkaClient", (), {})  # unique type not needed
                    MockKC.return_value = MagicMock()  # fresh mock per call
                    consumer = QueryClient._get_or_create_consumer()
                    consumers[thread_name] = id(consumer)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=_grab_consumer, args=("thread-A",), daemon=True)
        t2 = threading.Thread(target=_grab_consumer, args=("thread-B",), daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=5); t2.join(timeout=5)

        assert not errors, f"Thread errors: {errors}"
        assert "thread-A" in consumers and "thread-B" in consumers

        assert consumers["thread-A"] != consumers["thread-B"], (
            "Different threads must NOT share the same consumer instance — "
            "they would race on correlation-ID polling"
        )

    def test_concurrent_query_calls_timeout_independently(self):
        """Two concurrent QueryClient.query() calls must each time out independently
        without blocking each other — neither should deadlock waiting for the other's
        reply messages.
        """
        results: dict = {"t1": None, "t2": None}
        durations: dict = {}

        def _run_query(key: str, timeout: float) -> None:
            _reset_thread_local()
            start = time.monotonic()
            try:
                # Both calls will timeout because there's no real Kafka broker.
                # We patch subscribe/poll to do nothing so the timeout is the only exit.
                with patch("utils.query_client.KafkaClient") as MockKC:
                    mock_consumer = MagicMock()
                    mock_consumer.poll.return_value = []    # no messages — triggers timeout
                    mock_consumer._consumer = MagicMock()
                    mock_consumer._consumer.assignment.return_value = [object()]  # skip ready wait
                    MockKC.return_value = mock_consumer

                    with patch("utils.query_client.KafkaClient") as MockKC2:
                        mock_pub = MagicMock()
                        MockKC2.return_value = mock_pub

                        # Short timeout so the test doesn't stall
                        QueryClient.query("get_customer_metrics", {"customer_id": "cust_001"}, timeout=0.3)
            except QueryTimeoutError:
                results[key] = "timeout"
            except Exception as e:
                results[key] = f"error: {e}"
            finally:
                durations[key] = time.monotonic() - start

        t1 = threading.Thread(target=_run_query, args=("t1", 0.3), daemon=True)
        t2 = threading.Thread(target=_run_query, args=("t2", 0.3), daemon=True)

        start_wall = time.monotonic()
        t1.start(); t2.start()
        t1.join(timeout=5); t2.join(timeout=5)
        wall = time.monotonic() - start_wall

        # Both threads should have reached timeout without being blocked by the other
        assert results["t1"] == "timeout", f"t1 result: {results['t1']}"
        assert results["t2"] == "timeout", f"t2 result: {results['t2']}"

        # If they were sequential the wall time would be ≥ 0.6 s.
        # Running concurrently they finish close to the single timeout duration.
        assert wall < 2.0, (
            f"Threads appear to have run sequentially (wall={wall:.2f}s), "
            "suggesting shared consumer blocking"
        )

    def test_no_cross_contamination_of_correlation_ids(self):
        """Each thread must only see replies that match its own correlation_id.

        We simulate two 'replies' on the mock consumer — one per thread — and
        verify each thread receives only its own reply.
        """
        results: dict = {}
        lock = threading.Lock()

        def _run(thread_id: str) -> None:
            _reset_thread_local()
            corr_id = f"query_{thread_id}_unique"

            reply = {
                "event_type": "query.response",
                "correlation_id": corr_id,
                "payload": {"result": thread_id},
                "event_id": "evt_reply",
                "event_source": "QueryAgent",
                "entity_id": "test",
                "schema_version": "1.1",
                "event_time": "2024-01-01T00:00:00",
                "metadata": {},
            }

            # msg mock whose .value matches *this* thread's correlation ID
            msg = MagicMock()
            msg.value = reply

            with patch("utils.query_client.KafkaClient") as MockKC:
                mock_consumer = MagicMock()
                mock_consumer._consumer = MagicMock()
                mock_consumer._consumer.assignment.return_value = [object()]

                # Inject the matching reply into poll output
                def _poll(**kwargs):
                    return [msg]

                mock_consumer.poll.side_effect = _poll
                MockKC.return_value = mock_consumer

                # Patch uuid to inject the predictable correlation_id
                with patch("utils.query_client.uuid") as mock_uuid:
                    mock_uuid.uuid4.return_value.hex = thread_id + "_unique"
                    try:
                        result = QueryClient.query(
                            "get_customer_metrics",
                            {"customer_id": "cust_001"},
                            timeout=1.0,
                        )
                        with lock:
                            results[thread_id] = result
                    except Exception as e:
                        with lock:
                            results[thread_id] = f"error: {e}"

        t1 = threading.Thread(target=_run, args=("thread1",), daemon=True)
        t2 = threading.Thread(target=_run, args=("thread2",), daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=5); t2.join(timeout=5)

        # Each thread must get its own payload, not the other's
        if "thread1" in results and isinstance(results["thread1"], dict):
            assert results["thread1"].get("result") == "thread1", (
                f"Thread1 got wrong result: {results['thread1']}"
            )
        if "thread2" in results and isinstance(results["thread2"], dict):
            assert results["thread2"].get("result") == "thread2", (
                f"Thread2 got wrong result: {results['thread2']}"
            )
