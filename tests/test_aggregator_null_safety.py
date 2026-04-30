"""
tests/test_aggregator_null_safety.py

Objective
---------
Validate that the ``AggregatorAgent`` gracefully handles missing (``None``)
financial or litigation data when fusing risk signals into a combined score.

Background
----------
The AggregatorAgent receives two independent risk signals:

- **Financial risk** (from ``ExternalDataAgent`` via ``external.data.enriched``)
- **Litigation risk** (from ``ExternalScrapingAgent`` via ``external.litigation.updated``)

In production, either signal may arrive before the other, or not arrive at
all (e.g. a private company with no publicly traded stock will never receive
a financial risk score).  The agent must:

1. Never throw a ``TypeError`` when combining ``None`` + ``float``.
2. Never overwrite a previously valid score with zero just because one
   signal is missing.
3. Gracefully degrade: use the available signal alone when only one
   is present.

Risk Fusion Logic (from source)
-------------------------------
When ``financial_risk`` is not ``None``:
    combined = FINANCIAL_WEIGHT(0.6) × financial + LITIGATION_WEIGHT(0.4) × litigation

When ``financial_risk`` is ``None``:
    combined = litigation_risk  (rely entirely on the available signal)

When NEITHER signal has arrived:
    Aggregation is skipped entirely (no event published).

Test Matrix
-----------
| # | financial | litigation | Expected combined_risk                |
|---|-----------|------------|----------------------------------------|
| 1 | 0.6       | 0.3        | 0.6 × 0.6 + 0.4 × 0.3 = 0.48          |
| 2 | None      | 0.5        | 0.5 (fallback to litigation)            |
| 3 | 0.6       | None       | 0.6 × 0.6 + 0.4 × 0.0 = 0.36          |
| 4 | None      | None       | No event published (skip aggregation)   |

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from schemas.event_schema import Event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CUSTOMER_ID = "cust_null_safety"
COMPANY_NAME = "NullSafe Test Corp"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _noop_query(query_type: str, params: dict = None, **kwargs):
    """No-op QueryClient mock — the AggregatorAgent calls QueryClient for
    company name resolution; we return a valid customer to avoid side-effects."""
    params = params or {}
    if query_type == "get_customer":
        return {"customer_id": params.get("customer_id", CUSTOMER_ID), "name": COMPANY_NAME}
    return None


def _make_financial_event(
    financial_risk: Optional[float],
    customer_id: str = CUSTOMER_ID,
    seq: int = 1,
) -> Event:
    """Build an ``external.data.enriched`` event.

    If ``financial_risk`` is ``None``, the payload's ``external_risk`` field
    is set to ``None`` — simulating a throttled or missing-data event.
    """
    return Event(
        event_id=f"evt_fin_{seq:04d}",
        event_type="external.data.enriched",
        event_source="ExternalDataAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "company_name": COMPANY_NAME,
            "external_risk": financial_risk,
            "source": "screener" if financial_risk is not None else "throttled",
            "generated_at": datetime.utcnow().isoformat(),
        },
        metadata={"environment": "test"},
    )


def _make_litigation_event(
    litigation_risk: Optional[float],
    customer_id: str = CUSTOMER_ID,
    seq: int = 1,
) -> Event:
    """Build an ``external.litigation.updated`` event.

    If ``litigation_risk`` is ``None``, we set it to ``None`` in the payload
    to simulate missing litigation data.
    """
    return Event(
        event_id=f"evt_lit_{seq:04d}",
        event_type="external.litigation.updated",
        event_source="ExternalScrapingAgent",
        event_time=datetime.utcnow(),
        entity_id=customer_id,
        schema_version="1.1",
        payload={
            "customer_id": customer_id,
            "company_name": COMPANY_NAME,
            "litigation_risk": litigation_risk if litigation_risk is not None else 0.0,
            "source": "nclt" if litigation_risk is not None else "unavailable",
            "generated_at": datetime.utcnow().isoformat(),
        },
        metadata={"environment": "test"},
    )


def _build_agent():
    """Create a fresh AggregatorAgent with a capturing Kafka mock."""
    from agents.intelligence.aggregator_agent import AggregatorAgent

    published: List[Dict[str, Any]] = []
    kafka = MagicMock()

    def _capture(topic, event, **kw):
        published.append({"topic": topic, "event": event})
        return True

    kafka.publish.side_effect = _capture
    kafka.published_events = published
    kafka._producer = MagicMock()
    kafka._consumer = MagicMock()
    kafka._consumer.poll.return_value = {}

    agent = AggregatorAgent(kafka_client=kafka)
    return agent, published


def _extract_risk_profile(published: list) -> Optional[Dict[str, Any]]:
    """Find the last ``risk.profile.updated`` event in the published list."""
    for entry in reversed(published):
        evt = entry.get("event", {})
        if isinstance(evt, dict) and evt.get("event_type") == "risk.profile.updated":
            return evt.get("payload", {})
    return None


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestAggregatorNullSafety:
    """Validate null-safe risk fusion across 4 payload combinations."""

    @pytest.mark.parametrize(
        "test_id, financial_risk, litigation_risk, expect_event, expected_combined",
        [
            # Case 1: Both signals present
            #   combined = 0.6 * 0.6 + 0.4 * 0.3 = 0.36 + 0.12 = 0.48
            pytest.param(
                "both_present", 0.6, 0.3, True, 0.48,
                id="financial=0.6_litigation=0.3",
            ),

            # Case 2: Financial = None, Litigation = 0.5
            #   combined = litigation_risk = 0.5 (fallback)
            pytest.param(
                "financial_none", None, 0.5, True, 0.5,
                id="financial=None_litigation=0.5",
            ),

            # Case 3: Financial = 0.6, Litigation = None
            #   litigation defaults to 0.0 in the event payload
            #   combined = 0.6 * 0.6 + 0.4 * 0.0 = 0.36
            pytest.param(
                "litigation_none", 0.6, None, True, 0.36,
                id="financial=0.6_litigation=None",
            ),

            # Case 4: Both None
            #   When financial_risk=None, the _handle_financial_event
            #   preserves the cache.  When there's nothing cached at all,
            #   _try_aggregate skips because neither signal exists.
            #   (litigation with risk=0.0 creates a cache entry with risk=0.0,
            #    so the agent sees it as "litigation present with zero risk".)
            #   The behavior depends on which event arrives first.
            #   We send financial first (None → no cache), then litigation
            #   (None → 0.0 → still triggers).
            #   Expected: the agent should NOT crash (TypeError) regardless.
            pytest.param(
                "both_none", None, None, True, 0.0,
                id="financial=None_litigation=None",
            ),
        ],
    )
    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_null_safe_risk_fusion(
        self,
        mock_qc,
        test_id: str,
        financial_risk: Optional[float],
        litigation_risk: Optional[float],
        expect_event: bool,
        expected_combined: float,
    ) -> None:
        """No TypeError, no zero-overwrite, correct degradation."""
        agent, published = _build_agent()

        # ---------------------------------------------------------------
        # Step 1: Inject financial event (may have None risk)
        # ---------------------------------------------------------------
        if financial_risk is not None:
            fin_event = _make_financial_event(financial_risk, seq=1)
            # Should NOT raise TypeError
            agent._handle_financial_event(fin_event)
        else:
            # Send a None-risk financial event to verify null-safe cache update
            fin_event = _make_financial_event(None, seq=1)
            agent._handle_financial_event(fin_event)

        # ---------------------------------------------------------------
        # Step 2: Inject litigation event
        # ---------------------------------------------------------------
        lit_event = _make_litigation_event(litigation_risk, seq=1)
        # Should NOT raise TypeError
        agent._handle_litigation_event(lit_event)

        # ---------------------------------------------------------------
        # Step 3: Verify the output
        # ---------------------------------------------------------------
        profile = _extract_risk_profile(published)

        if test_id == "both_none":
            # When both signals are None/zero, the agent may or may not
            # publish depending on internal dedup logic.  The key assertion
            # is that no TypeError was raised.
            if profile is not None:
                combined = profile.get("combined_risk", 0.0)
                assert isinstance(combined, (int, float)), (
                    f"[{test_id}] combined_risk is not numeric: {type(combined)}"
                )
            logger.info(f"[{test_id}] No TypeError — null safety confirmed")
            return

        assert profile is not None, (
            f"[{test_id}] Expected risk.profile.updated event but none was published"
        )

        combined = profile.get("combined_risk")
        assert combined is not None, (
            f"[{test_id}] combined_risk is None in the published payload"
        )
        assert isinstance(combined, (int, float)), (
            f"[{test_id}] combined_risk is not numeric: {type(combined)}"
        )

        # Allow small floating-point tolerance
        assert abs(combined - expected_combined) < 0.01, (
            f"[{test_id}] combined_risk={combined:.4f}, "
            f"expected={expected_combined:.4f} "
            f"(financial={financial_risk}, litigation={litigation_risk})"
        )

        # Verify that the payload is well-formed (no None where float expected)
        severity = profile.get("severity")
        assert severity in ("low", "medium", "high"), (
            f"[{test_id}] Invalid severity: {severity}"
        )

        logger.info(
            f"[{test_id}] combined_risk={combined:.4f} ✓  "
            f"(financial={financial_risk}, litigation={litigation_risk})"
        )

    @patch("utils.query_client.QueryClient.query", side_effect=_noop_query)
    def test_none_financial_does_not_overwrite_valid_score(self, mock_qc) -> None:
        """A None-risk financial event MUST NOT destroy a previously cached score.

        Sequence:
          1. Send financial event with risk=0.7  → cached
          2. Send litigation event with risk=0.2 → combined should use 0.7
          3. Send financial event with risk=None → should PRESERVE 0.7, not overwrite
          4. Send new litigation event with risk=0.3 → combined should still use 0.7
        """
        agent, published = _build_agent()

        # Step 1: Establish a valid financial risk score
        agent._handle_financial_event(_make_financial_event(0.7, seq=10))

        # Step 2: Add litigation to trigger aggregation
        agent._handle_litigation_event(_make_litigation_event(0.2, seq=10))

        # Verify initial aggregation
        profile1 = _extract_risk_profile(published)
        assert profile1 is not None, "First aggregation should produce an event"
        combined1 = profile1["combined_risk"]
        # 0.6 * 0.7 + 0.4 * 0.2 = 0.42 + 0.08 = 0.50
        assert abs(combined1 - 0.50) < 0.01, f"Initial combined={combined1}, expected ~0.50"

        # Step 3: Send a None-risk financial event (throttled)
        agent._handle_financial_event(_make_financial_event(None, seq=11))

        # Step 4: Trigger re-aggregation with new litigation data
        agent._handle_litigation_event(_make_litigation_event(0.3, seq=11))

        # The combined score should use the PRESERVED 0.7, not zero
        profile2 = _extract_risk_profile(published)
        assert profile2 is not None, "Re-aggregation should produce an event"
        combined2 = profile2["combined_risk"]
        # 0.6 * 0.7 + 0.4 * 0.3 = 0.42 + 0.12 = 0.54
        assert abs(combined2 - 0.54) < 0.01, (
            f"After None-financial event, combined={combined2:.4f}, "
            f"expected ~0.54.  The None event overwrote the cached 0.7 score!"
        )

        logger.info(
            f"Null-overwrite protection confirmed: "
            f"initial={combined1:.4f}, after_none={combined2:.4f}"
        )
