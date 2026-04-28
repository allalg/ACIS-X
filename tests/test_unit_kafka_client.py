"""
tests/test_unit_kafka_client.py

Tests for KafkaClient.commit() offset semantics.

Key invariant:
  commit(message) must commit offset+1 for that specific (topic, partition)
  so the next poll resumes at the *next* message, not at the current one.
  It must NOT commit any other partitions.
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, call, patch

from runtime.kafka_client import KafkaClient, KafkaConfig, KafkaMessage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(backend: str = "kafka-python") -> KafkaClient:
    config = KafkaConfig(
        bootstrap_servers=["localhost:9092"],
        client_id="test-kafka-commit",
    )
    client = KafkaClient(config=config, backend=backend)
    # Prevent real network connections
    client._producer = MagicMock()
    client._consumer = MagicMock()
    return client


def _make_message(topic: str = "acis.invoices", partition: int = 0, offset: int = 42) -> KafkaMessage:
    return KafkaMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=None,
        value={"event_id": "evt_test"},
        headers={},
        timestamp=datetime.utcnow(),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestKafkaClientCommitOffset:
    """commit(message) must commit exactly offset+1 for the given partition."""

    def test_commit_with_message_commits_offset_plus_one(self):
        """The committed offset must be message.offset + 1 (next-to-read)."""
        client = _make_client()
        msg = _make_message(topic="acis.invoices", partition=0, offset=99)

        client.commit(msg)

        from kafka import TopicPartition, OffsetAndMetadata

        tp = TopicPartition("acis.invoices", 0)
        expected_offsets = {tp: OffsetAndMetadata(100, None, -1)}   # offset + 1 = 100

        client._consumer.commit.assert_called_once_with(offsets=expected_offsets)

    def test_commit_with_message_does_not_call_bare_commit(self):
        """commit(message) must NOT fall through to the no-arg consumer.commit()."""
        client = _make_client()
        msg = _make_message(partition=1, offset=0)

        client.commit(msg)

        # The mock records all calls; filter for bare commit() with no offsets
        for c in client._consumer.commit.call_args_list:
            args, kwargs = c
            assert "offsets" in kwargs, (
                "Expected keyword-arg 'offsets' but got a bare commit() call"
            )

    def test_commit_without_message_uses_bare_commit(self):
        """commit(None) → consumer.commit() with no offset argument."""
        client = _make_client()

        client.commit(None)

        # For kafka-python a bare commit() is called when message is None
        client._consumer.commit.assert_called_once_with()

    def test_commit_offset_is_exactly_message_offset_plus_one(self):
        """Parametrise over multiple offset values to guard against off-by-one bugs."""
        for offset in (0, 1, 100, 9999):
            client = _make_client()
            msg = _make_message(partition=2, offset=offset)
            client.commit(msg)

            from kafka import TopicPartition, OffsetAndMetadata
            tp = TopicPartition(msg.topic, msg.partition)
            expected = {tp: OffsetAndMetadata(offset + 1, None, -1)}
            client._consumer.commit.assert_called_once_with(offsets=expected)

    def test_commit_only_touches_correct_partition(self):
        """A message on partition 3 must only commit partition 3, not 0 or 1."""
        client = _make_client()
        msg = _make_message(topic="acis.payments", partition=3, offset=7)

        client.commit(msg)

        from kafka import TopicPartition, OffsetAndMetadata
        _, kwargs = client._consumer.commit.call_args
        committed = kwargs["offsets"]

        # Only one partition in the commit
        assert len(committed) == 1
        assert TopicPartition("acis.payments", 3) in committed
        # Correct offset value (offset+1 = 8, with leader_epoch=-1)
        assert committed[TopicPartition("acis.payments", 3)].offset == 8

    def test_commit_when_no_consumer_is_a_no_op(self):
        """If _consumer is None (not yet subscribed), commit() must not raise."""
        client = _make_client()
        client._consumer = None
        msg = _make_message()

        # Should silently return, not raise AttributeError
        client.commit(msg)

    def test_commit_propagates_consumer_exceptions(self):
        """If the underlying consumer.commit() raises, KafkaClient re-raises it."""
        client = _make_client()
        client._consumer.commit.side_effect = RuntimeError("broker unavailable")
        msg = _make_message()

        with pytest.raises(RuntimeError, match="broker unavailable"):
            client.commit(msg)
