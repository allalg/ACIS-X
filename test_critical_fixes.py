#!/usr/bin/env python3
"""
Test suite for critical fixes:
1. ISSUE 2: Single Kafka Client
2. IMPROVEMENT 3: last_payment_date tracking
3. MEMORY LEAK: Bounded deque
"""

import logging
from collections import deque

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("test_fixes")


def test_bounded_deque():
    """Test 1: Memory leak prevention with bounded deque."""
    logger.info("\n[TEST 1] Memory Leak Prevention: Bounded Deque")

    from agents.storage.memory_agent import MemoryAgent
    from unittest.mock import Mock

    mock_kafka = Mock()
    mock_query = Mock()
    agent = MemoryAgent(kafka_client=mock_kafka, query_agent=mock_query)

    # Verify processed_events is a bounded deque
    assert isinstance(agent.processed_events, deque), "ERROR: Not a deque"
    assert agent.processed_events.maxlen == 10000, f"ERROR: maxlen={agent.processed_events.maxlen}, want 10000"

    # Fill beyond capacity
    for i in range(15000):
        agent.processed_events.append(f"event_{i}")

    # Only maxlen should remain
    assert len(agent.processed_events) == 10000, "ERROR: Should have 10000 items"
    assert "event_0" not in agent.processed_events, "ERROR: Oldest should be dropped"
    assert "event_14999" in agent.processed_events, "ERROR: Newest should remain"

    logger.info("  PASS: Bounded deque (maxlen=10000) prevents memory leak")
    return True


def test_shared_kafka_client():
    """Test 2: Verify shared Kafka client in run_acis."""
    logger.info("\n[TEST 2] Single Shared Kafka Client")

    # Read run_acis.py and check for shared_kafka_client
    with open("run_acis.py", "r") as f:
        content = f.read()

    # Verify key patterns
    checks = [
        ("shared_kafka_client = _build_kafka_client()", "Shared client creation"),
        ("kafka_client=shared_kafka_client", "Passing shared client to agents"),
    ]

    all_found = True
    for pattern, desc in checks:
        if pattern in content:
            logger.info(f"  FOUND: {desc}")
        else:
            logger.error(f"  MISSING: {desc}")
            all_found = False

    if all_found:
        logger.info("  PASS: Shared Kafka client implementation verified")
    return all_found


def test_payment_date_tracking():
    """Test 3: last_payment_date only updated on payment events."""
    logger.info("\n[TEST 3] last_payment_date Tracking")

    # Read memory_agent.py and verify update_last_payment logic
    with open("agents/storage/memory_agent.py", "r") as f:
        content = f.read()

    checks = [
        ("update_last_payment: bool = False", "Parameter default"),
        ("update_last_payment=True", "Payment event passes True"),
        ("if update_last_payment:", "Conditional logic"),
        ("Don't update last_payment_date", "Comment about preservation"),
    ]

    all_found = True
    for pattern, desc in checks:
        if pattern in content:
            logger.info(f"  FOUND: {desc}")
        else:
            logger.error(f"  MISSING: {desc}")
            all_found = False

    if all_found:
        logger.info("  PASS: last_payment_date tracking verified")
    return all_found


def main():
    logger.info("="*60)
    logger.info("CRITICAL FIXES VERIFICATION")
    logger.info("="*60)

    tests = [
        ("Bounded Deque (Memory Leak Prevention)", test_bounded_deque),
        ("Shared Kafka Client", test_shared_kafka_client),
        ("last_payment_date Tracking", test_payment_date_tracking),
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

    logger.info("\n" + "="*60)
    logger.info("RESULTS")
    logger.info("="*60)
    for name, status in results:
        mark = "[PASS]" if status == "PASS" else "[FAIL]"
        logger.info(f"{mark} {name}")

    passed = sum(1 for _, s in results if s == "PASS")
    logger.info(f"\nSummary: {passed}/{len(results)} tests passed")
    return passed == len(results)


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
