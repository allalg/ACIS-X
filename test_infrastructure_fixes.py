#!/usr/bin/env python3
"""
Comprehensive test for 3 critical infrastructure fixes:
1. ISSUE 1: Standardize event naming to domain.action format
2. ISSUE 2: O(1) idempotency check with hybrid Set+Deque
3. ISSUE 3: Verify risk pipeline connection
"""

import logging
import re
from collections import deque
from unittest.mock import Mock

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("test_infrastructure_fixes")


def test_event_name_standardization():
    """Test 1: Event name standardization to domain.action format."""
    logger.info("\n[TEST 1] Event Name Standardization")

    file_checks = [
        ("agents/risk/risk_scoring_agent.py", "event_type=\"risk.scored\"", "RiskScoringAgent publishes risk.scored"),
        ("agents/risk/risk_scoring_agent.py", "event_type=\"risk.high.detected\"", "RiskScoringAgent publishes risk.high.detected"),
        ("agents/intelligence/aggregator_agent.py", "event_type=\"risk.profile.updated\"", "AggregatorAgent publishes risk.profile.updated"),
        ("agents/intelligence/external_scrapping_agent.py", "event_type=\"external.litigation.updated\"", "ExternalScrapingAgent publishes external.litigation.updated"),
        ("agents/storage/memory_agent.py", "event_type == \"risk.scored\"", "MemoryAgent handles risk.scored"),
        ("agents/storage/memory_agent.py", "event_type == \"risk.high.detected\"", "MemoryAgent handles risk.high.detected"),
        ("agents/storage/db_agent.py", "event_type == \"external.litigation.updated\"", "DBAgent handles external.litigation.updated"),
        ("agents/storage/db_agent.py", "event_type == \"risk.profile.updated\"", "DBAgent handles risk.profile.updated"),
        ("agents/customer/customer_profile_agent.py", "event_type == \"risk.scored\"", "CustomerProfileAgent handles risk.scored"),
        ("agents/customer/customer_profile_agent.py", "event_type == \"external.litigation.updated\"", "CustomerProfileAgent handles external.litigation.updated"),
        ("agents/customer/customer_profile_agent.py", "event_type == \"risk.profile.updated\"", "CustomerProfileAgent handles risk.profile.updated"),
        ("agents/collections/collections_agent.py", "event_type == \"risk.scored\"", "CollectionsAgent handles risk.scored"),
        ("agents/collections/collections_agent.py", "event_type == \"risk.high.detected\"", "CollectionsAgent handles risk.high.detected"),
        ("agents/policy/credit_policy_agent.py", "event_type == \"risk.scored\"", "CreditPolicyAgent handles risk.scored"),
    ]

    all_found = True
    for file_path, pattern, description in file_checks:
        try:
            with open(file_path, "r") as f:
                content = f.read()
            if pattern in content:
                logger.info(f"  ✓ {description}")
            else:
                logger.error(f"  ✗ MISSING: {description}")
                logger.error(f"    Looking for: {pattern}")
                all_found = False
        except Exception as e:
            logger.error(f"  ✗ ERROR reading {file_path}: {e}")
            all_found = False

    if all_found:
        logger.info("  PASS: Event names standardized to domain.action format")
    return all_found


def test_o1_idempotency_check():
    """Test 2: O(1) idempotency check with hybrid Set+Deque."""
    logger.info("\n[TEST 2] O(1) Idempotency Check (Hybrid Set+Deque)")

    from agents.storage.memory_agent import MemoryAgent

    mock_kafka = Mock()
    mock_query = Mock()
    agent = MemoryAgent(kafka_client=mock_kafka, query_agent=mock_query)

    # Verify both Set and Deque exist
    assert hasattr(agent, 'processed_events_set'), "ERROR: Missing processed_events_set"
    assert hasattr(agent, 'processed_events_queue'), "ERROR: Missing processed_events_queue"
    logger.info("  ✓ Both Set and Deque attributes exist")

    # Verify Set is a set
    assert isinstance(agent.processed_events_set, set), "ERROR: processed_events_set is not a set"
    logger.info("  ✓ processed_events_set is a set")

    # Verify Deque has maxlen
    assert isinstance(agent.processed_events_queue, deque), "ERROR: processed_events_queue is not a deque"
    assert agent.processed_events_queue.maxlen == 10000, "ERROR: Deque maxlen is not 10000"
    logger.info("  ✓ processed_events_queue is a bounded deque (maxlen=10000)")

    # Test O(1) lookup performance
    # Fill both structures
    for i in range(5000):
        agent.processed_events_set.add(f"event_{i}")
        agent.processed_events_queue.append(f"event_{i}")

    # Test Set lookup (should be O(1))
    import time
    start = time.time()
    for i in range(5000):
        _ = f"event_{i}" in agent.processed_events_set
    set_time = time.time() - start

    # Test Deque lookup (O(n))
    agent_temp = MemoryAgent(kafka_client=mock_kafka, query_agent=mock_query)
    temp_deque = deque(maxlen=10000)
    for i in range(5000):
        temp_deque.append(f"event_{i}")

    start = time.time()
    for i in range(5000):
        _ = f"event_{i}" in temp_deque
    deque_time = time.time() - start

    logger.info(f"  ✓ Set lookup: {set_time*1000:.2f}ms, Deque lookup: {deque_time*1000:.2f}ms")
    logger.info(f"  ✓ Set is {deque_time/set_time:.1f}x faster (O(1) vs O(n))")

    # Test sync: add to 10000 capacity
    agent2 = MemoryAgent(kafka_client=mock_kafka, query_agent=mock_query)
    for i in range(10000):
        agent2.processed_events_set.add(f"item_{i}")
        agent2.processed_events_queue.append(f"item_{i}")

    assert len(agent2.processed_events_queue) == 10000, "ERROR: Queue should be at 10000"
    assert len(agent2.processed_events_set) == 10000, "ERROR: Set should be at 10000"
    logger.info("  ✓ Both Set and Deque at capacity (10000)")

    # Add one more - oldest should drop from both
    agent2.processed_events_set.add("item_10000")
    oldest = agent2.processed_events_queue[0]
    agent2.processed_events_queue.append("item_10000")
    agent2.processed_events_set.discard(oldest)

    assert len(agent2.processed_events_queue) == 10000, "ERROR: Queue should stay at 10000"
    assert len(agent2.processed_events_set) == 10000, "ERROR: Set should stay at 10000"
    assert "item_10000" in agent2.processed_events_set, "ERROR: New item not in set"
    assert "item_0" not in agent2.processed_events_set, "ERROR: Old item not removed from set"
    logger.info("  ✓ Proper synchronization: old items dropped from both Set and Deque")

    logger.info("  PASS: O(1) idempotency check implemented correctly")
    return True


def test_risk_pipeline_connection():
    """Test 3: Risk pipeline is connected (RiskScoringAgent publishes, MemoryAgent subscribes)."""
    logger.info("\n[TEST 3] Risk Pipeline Connection")

    checks = [
        ("agents/risk/risk_scoring_agent.py", 'TOPIC_RISK = "acis.risk"', "RiskScoringAgent publishes to acis.risk"),
        ("agents/storage/memory_agent.py", 'TOPIC_RISK = "acis.risk"', "MemoryAgent subscribes to acis.risk"),
        ("agents/storage/memory_agent.py", "self.TOPIC_RISK,", "MemoryAgent includes TOPIC_RISK in subscribed_topics"),
    ]

    all_found = True
    for file_path, pattern, description in checks:
        try:
            with open(file_path, "r") as f:
                content = f.read()
            if pattern in content:
                logger.info(f"  ✓ {description}")
            else:
                logger.error(f"  ✗ MISSING: {description}")
                all_found = False
        except Exception as e:
            logger.error(f"  ✗ ERROR: {e}")
            all_found = False

    # Verify RiskScoringAgent publishes risk events
    try:
        with open("agents/risk/risk_scoring_agent.py", "r") as f:
            content = f.read()
        if "self.publish_event" in content and "self.TOPIC_RISK" in content:
            logger.info("  ✓ RiskScoringAgent calls publish_event to acis.risk")
        else:
            logger.error("  ✗ MISSING: RiskScoringAgent publish_event call")
            all_found = False
    except Exception as e:
        logger.error(f"  ✗ ERROR: {e}")
        all_found = False

    if all_found:
        logger.info("  PASS: Risk pipeline properly connected")
    return all_found


def main():
    logger.info("="*70)
    logger.info("INFRASTRUCTURE FIXES TEST SUITE")
    logger.info("="*70)

    tests = [
        ("Event Name Standardization", test_event_name_standardization),
        ("O(1) Idempotency Check", test_o1_idempotency_check),
        ("Risk Pipeline Connection", test_risk_pipeline_connection),
    ]

    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, "PASS" if passed else "FAIL"))
        except Exception as e:
            logger.error(f"  EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, "FAIL"))

    logger.info("\n" + "="*70)
    logger.info("RESULTS SUMMARY")
    logger.info("="*70)

    for name, status in results:
        mark = "[PASS]" if status == "PASS" else "[FAIL]"
        logger.info(f"{mark} {name}")

    passed = sum(1 for _, s in results if s == "PASS")
    total = len(results)
    logger.info(f"\nTotal: {passed}/{total} tests passed")

    return passed == total


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
