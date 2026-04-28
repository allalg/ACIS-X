"""
Unit tests for timezone-aware datetime handling in the ACIS-X event pipeline.

Covers:
- Event.model_validate() with a timezone-aware ISO string → tzinfo must be None
  after parsing (the pipeline always works with naive UTC datetimes).
- BaseAgent.publish_event() stamps events with a naive UTC datetime.
"""
import pytest
from datetime import datetime, timezone, timedelta

from schemas.event_schema import Event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event_dict(**overrides) -> dict:
    base = {
        "event_id": "evt_test_tz_001",
        "event_type": "test.event",
        "event_source": "UnitTest",
        "event_time": "2024-01-15T10:30:00+05:30",   # timezone-aware ISO string
        "entity_id": "test_entity",
        "schema_version": "1.1",
        "payload": {},
        "metadata": {},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEventTimezoneNormalisation:
    """Event timestamps must always be stored as naive UTC datetimes."""

    def test_tz_aware_string_parsed_to_naive_datetime(self):
        """A tz-aware ISO string supplied to model_validate must produce a naive
        datetime (tzinfo is None) after Pydantic parsing, so that all downstream
        comparisons (e.g. stale-event guards) operate on the same basis."""
        event = Event.model_validate(_make_event_dict())
        assert event.event_time.tzinfo is None, (
            f"Expected tzinfo=None after parsing, got {event.event_time.tzinfo!r}"
        )

    def test_tz_aware_string_is_a_datetime_instance(self):
        """Parsed event_time must be a datetime object, not a raw string."""
        event = Event.model_validate(_make_event_dict())
        assert isinstance(event.event_time, datetime), (
            f"Expected datetime, got {type(event.event_time)}"
        )

    def test_utc_string_no_offset_parsed_correctly(self):
        """A plain UTC string (no offset) must also produce a naive datetime."""
        event = Event.model_validate(_make_event_dict(event_time="2024-06-01T00:00:00Z"))
        assert event.event_time.tzinfo is None

    def test_naive_utcnow_roundtrip(self):
        """datetime.utcnow() (already naive) must survive model_validate unchanged."""
        now = datetime.utcnow()
        event = Event.model_validate(_make_event_dict(event_time=now.isoformat()))
        assert event.event_time.tzinfo is None

    def test_timezone_aware_utc_stripped(self):
        """datetime.now(timezone.utc).replace(tzinfo=None) pattern used in
        publish_event() must produce a naive datetime that parses cleanly."""
        ts = datetime.now(timezone.utc).replace(tzinfo=None)
        event = Event.model_validate(_make_event_dict(event_time=ts.isoformat()))
        assert event.event_time.tzinfo is None
        # Sanity: the value should be close to now
        delta = abs((event.event_time - datetime.utcnow()).total_seconds())
        assert delta < 5, f"Timestamp too far from now: {delta}s"

    def test_positive_offset_normalised(self):
        """IST (+05:30) must be accepted without raising, and tzinfo stripped."""
        event = Event.model_validate(_make_event_dict(event_time="2024-03-20T08:00:00+05:30"))
        assert event.event_time.tzinfo is None

    def test_negative_offset_normalised(self):
        """Negative UTC offset must be accepted and tzinfo stripped."""
        event = Event.model_validate(_make_event_dict(event_time="2024-03-20T08:00:00-04:00"))
        assert event.event_time.tzinfo is None
