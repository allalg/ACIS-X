"""
tests/test_canonical_consumer_group_scaling.py

Objective
---------
Validate that canonical Kafka ``group_id``s enable true load-balancing
(not duplication) across multiple consumer replicas.

Background
----------
In Kafka, consumers sharing the **same** ``group_id`` form a consumer group.
Kafka partitions are assigned exclusively to one member of the group — this
is what enables horizontal scaling.  If consumers have **different**
``group_id``s, each receives ALL messages (broadcast / fan-out).

The ACIS-X architecture relies on canonical ``group_id``s (e.g.
``"customer-state-group"``) being shared across all replicas of the same
agent type.  This test validates:

1. **Same group_id** → load-balanced: 300 events split roughly equally among
   3 consumers (each gets ~100 ±20%).
2. **Unique group_ids** → broadcast: all 3 consumers each receive all 300
   events.

Since we don't have a live Kafka broker in unit tests, we simulate partition
assignment using a deterministic hash-based partitioner and an in-memory
topic model with 6 partitions.

Test Plan
---------
1. Instantiate an ``InMemoryTopicBroker`` with 6 partitions.
2. Publish 300 synthetic events with partition keys.
3. Test A: 3 consumers with the **same** ``group_id`` → each gets ~100
   events (±20% tolerance).
4. Test B: 3 consumers with **unique** ``group_id``s → each gets all 300.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import hashlib
import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import pytest

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_PARTITIONS = 6
NUM_EVENTS = 300
NUM_CONSUMERS = 3
VARIANCE_TOLERANCE = 0.20  # ±20% for partition hashing imbalance


# ---------------------------------------------------------------------------
# In-Memory Topic Broker (simulates Kafka partition assignment)
# ---------------------------------------------------------------------------

class InMemoryTopicBroker:
    """Simulates a Kafka topic with deterministic partition assignment.

    Key Features:
    - N partitions, each holding a list of events.
    - Partition assignment is based on ``hashlib.md5(key)`` mod N, mirroring
      Kafka's default ``Murmur2`` partitioner in spirit.
    - Consumer group assignment: partitions are round-robin assigned to
      consumers within the same group_id.
    - Unique group_ids each get their own independent offset tracker,
      meaning they each read ALL events.
    """

    def __init__(self, num_partitions: int = 6) -> None:
        self.num_partitions = num_partitions
        # partition_id → list of events
        self._partitions: Dict[int, List[Dict[str, Any]]] = {
            i: [] for i in range(num_partitions)
        }
        # group_id → {partition_id: consumer_index}
        self._group_assignments: Dict[str, Dict[int, int]] = {}
        # group_id → list of consumer_ids
        self._group_members: Dict[str, List[str]] = {}

    def publish(self, event: Dict[str, Any], key: Optional[str] = None) -> int:
        """Publish an event to a partition based on the key hash.

        Returns the partition the event was assigned to.
        """
        if key is None:
            key = str(uuid.uuid4())

        partition = int(hashlib.md5(key.encode()).hexdigest(), 16) % self.num_partitions
        self._partitions[partition].append(event)
        return partition

    def register_consumer(self, group_id: str, consumer_id: str) -> None:
        """Register a consumer in a group (triggers rebalance)."""
        if group_id not in self._group_members:
            self._group_members[group_id] = []
        if consumer_id not in self._group_members[group_id]:
            self._group_members[group_id].append(consumer_id)

        # Rebalance: round-robin assign partitions to members
        members = self._group_members[group_id]
        self._group_assignments[group_id] = {}
        for partition_id in range(self.num_partitions):
            consumer_idx = partition_id % len(members)
            self._group_assignments[group_id][partition_id] = consumer_idx

    def consume(self, group_id: str, consumer_id: str) -> List[Dict[str, Any]]:
        """Return all events assigned to this consumer within its group.

        Simulates Kafka's partition-exclusive assignment: each consumer
        only reads from partitions assigned to it by the rebalance.
        """
        if group_id not in self._group_members:
            return []

        members = self._group_members[group_id]
        try:
            consumer_idx = members.index(consumer_id)
        except ValueError:
            return []

        assignments = self._group_assignments.get(group_id, {})
        events = []
        for partition_id, assigned_idx in assignments.items():
            if assigned_idx == consumer_idx:
                events.extend(self._partitions[partition_id])

        return events


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_synthetic_event(seq: int) -> Dict[str, Any]:
    """Create a simple synthetic event with a deterministic partition key."""
    return {
        "event_id": f"evt_scale_{seq:06d}",
        "event_type": "customer.metrics.updated",
        "customer_id": f"cust_{seq:06d}",
        "timestamp": datetime.utcnow().isoformat(),
        "seq": seq,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.novel
class TestCanonicalConsumerGroupScaling:
    """Validate Kafka consumer group load-balancing vs. broadcast semantics."""

    def test_same_group_id_load_balances(self) -> None:
        """3 consumers with the SAME group_id split 300 events ~equally.

        Each consumer should receive approximately 100 events (±20%).
        The actual distribution depends on the partition hash function,
        but with 6 partitions and 3 consumers, each gets 2 partitions.
        """
        broker = InMemoryTopicBroker(num_partitions=NUM_PARTITIONS)
        canonical_group = "customer-state-group"

        # Register 3 consumers in the SAME group
        consumer_ids = [f"consumer_{i}" for i in range(NUM_CONSUMERS)]
        for cid in consumer_ids:
            broker.register_consumer(canonical_group, cid)

        # Publish 300 events with unique partition keys
        for seq in range(NUM_EVENTS):
            event = _make_synthetic_event(seq)
            broker.publish(event, key=event["customer_id"])

        # Each consumer reads from its assigned partitions
        consumer_counts: Dict[str, int] = {}
        total_received = 0
        for cid in consumer_ids:
            events = broker.consume(canonical_group, cid)
            consumer_counts[cid] = len(events)
            total_received += len(events)

        # Key assertion 1: total events received == total published
        # (no duplication, no loss)
        assert total_received == NUM_EVENTS, (
            f"Total events received ({total_received}) != published ({NUM_EVENTS}). "
            f"Per-consumer: {consumer_counts}"
        )

        # Key assertion 2: load is balanced within ±20% of ideal (100 each)
        expected_per_consumer = NUM_EVENTS / NUM_CONSUMERS
        min_allowed = expected_per_consumer * (1 - VARIANCE_TOLERANCE)
        max_allowed = expected_per_consumer * (1 + VARIANCE_TOLERANCE)

        for cid, count in consumer_counts.items():
            assert min_allowed <= count <= max_allowed, (
                f"Consumer {cid} received {count} events, expected "
                f"{expected_per_consumer:.0f} ±{VARIANCE_TOLERANCE:.0%} "
                f"[{min_allowed:.0f}, {max_allowed:.0f}]. "
                f"Distribution: {consumer_counts}"
            )

        logger.info(
            f"Same group_id test PASSED: "
            f"total={total_received}, per-consumer={consumer_counts}"
        )

    def test_unique_group_ids_broadcast(self) -> None:
        """3 consumers with UNIQUE group_ids each receive ALL 300 events.

        This is the broadcast / fan-out pattern — every consumer group
        independently reads the entire topic.
        """
        broker = InMemoryTopicBroker(num_partitions=NUM_PARTITIONS)

        # Register 3 consumers each in their OWN unique group
        consumer_configs = [
            (f"unique-group-{uuid.uuid4().hex[:8]}", f"consumer_{i}")
            for i in range(NUM_CONSUMERS)
        ]
        for group_id, cid in consumer_configs:
            broker.register_consumer(group_id, cid)

        # Publish 300 events
        for seq in range(NUM_EVENTS):
            event = _make_synthetic_event(seq)
            broker.publish(event, key=event["customer_id"])

        # Each consumer (in its own group) should receive ALL events
        for group_id, cid in consumer_configs:
            events = broker.consume(group_id, cid)
            assert len(events) == NUM_EVENTS, (
                f"Consumer {cid} (group={group_id}) received {len(events)} events, "
                f"expected {NUM_EVENTS} (broadcast mode)."
            )

        logger.info(
            f"Unique group_id test PASSED: "
            f"all {NUM_CONSUMERS} consumers received {NUM_EVENTS} events"
        )

    def test_acis_agents_use_canonical_group_ids(self) -> None:
        """Verify that ACIS-X agent classes use deterministic, canonical
        ``group_id``s — NOT UUID-suffixed unique IDs.

        If an agent uses a unique group_id per instance, Kafka would
        broadcast all events to all replicas (defeating horizontal scaling).
        This test catches that anti-pattern.
        """
        from unittest.mock import MagicMock
        from agents.intelligence.customer_state_agent import CustomerStateAgent
        from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        kafka = MagicMock()
        kafka.publish.return_value = True

        # Instantiate agents and check their group_ids
        agents_and_expected = [
            (CustomerStateAgent(kafka_client=kafka, db_path=":memory:"), "customer-state-group"),
            (PaymentPredictionAgent(kafka_client=kafka), "payment-prediction-group"),
            (RiskScoringAgent(kafka_client=kafka), "risk-scoring-group"),
        ]

        for agent, expected_group_id in agents_and_expected:
            assert agent.group_id == expected_group_id, (
                f"{agent.agent_name} uses group_id='{agent.group_id}', "
                f"expected canonical '{expected_group_id}'. "
                f"A unique group_id would disable load-balancing!"
            )

        logger.info(
            "Canonical group_id validation PASSED for all agents"
        )

    def test_multiple_replicas_share_canonical_group_id(self) -> None:
        """Verify that 3 replicas of the same agent type all share the same
        canonical ``group_id`` — a prerequisite for Kafka load-balancing."""
        from unittest.mock import MagicMock
        from agents.intelligence.customer_state_agent import CustomerStateAgent

        kafka = MagicMock()
        kafka.publish.return_value = True

        # Create 3 replicas
        replicas = [
            CustomerStateAgent(kafka_client=kafka, db_path=":memory:")
            for _ in range(3)
        ]

        # All replicas must share the same group_id
        group_ids = {r.group_id for r in replicas}
        assert len(group_ids) == 1, (
            f"Expected all replicas to share one group_id, "
            f"got {len(group_ids)} unique: {group_ids}"
        )
        assert "customer-state-group" in group_ids

        # But instance_ids must be UNIQUE (each replica is a separate consumer)
        instance_ids = {r.instance_id for r in replicas}
        assert len(instance_ids) == 3, (
            f"Expected 3 unique instance_ids, "
            f"got {len(instance_ids)}: {instance_ids}"
        )

        logger.info(
            f"Replica group_id test PASSED: "
            f"group_id={group_ids.pop()}, "
            f"unique instance_ids={len(instance_ids)}"
        )
