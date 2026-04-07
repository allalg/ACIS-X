#!/usr/bin/env python3
"""
Diagnostic script to verify message flow through ACIS-X agents.

Run this to check:
1. Kafka connectivity
2. Agent subscriptions
3. Message flow through system
4. Event processing
"""

import time
import logging
import sys
from runtime.kafka_client import KafkaClient, KafkaConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("verify_message_flow")


def verify_kafka_connectivity():
    """Verify Kafka broker is accessible."""
    logger.info("=" * 60)
    logger.info("STEP 1: Verifying Kafka connectivity")
    logger.info("=" * 60)

    try:
        config = KafkaConfig(bootstrap_servers=["localhost:9092"])
        client = KafkaClient(config=config, backend="kafka-python")
        client.create_topic("test-connectivity", partitions=1, replication_factor=1)

        # Try to produce a test message
        client.produce(
            topic="test-connectivity",
            key="test",
            value={"test": "message", "timestamp": time.time()}
        )
        logger.info("✓ Kafka connectivity OK")
        client.close()
        return True
    except Exception as e:
        logger.error(f"✗ Kafka connectivity FAILED: {e}")
        return False


def verify_acis_topics_exist():
    """Verify ACIS topics are created."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: Verifying ACIS topics exist")
    logger.info("=" * 60)

    required_topics = [
        "acis.invoices",
        "acis.payments",
        "acis.customers",
        "acis.metrics",
        "acis.predictions",
        "acis.risk",
    ]

    try:
        from runtime.topic_manager import TopicAdmin
        admin = TopicAdmin(bootstrap_servers=["localhost:9092"], backend="kafka-python")

        for topic in required_topics:
            # Try to create (will succeed if exists, fail gracefully if already exists)
            try:
                admin.create_topic(topic, partitions=3, replication_factor=1)
                logger.info(f"✓ Topic {topic} OK")
            except Exception as e:
                if "already exists" in str(e):
                    logger.info(f"✓ Topic {topic} OK (already exists)")
                else:
                    logger.warning(f"? Topic {topic} status unclear: {e}")

        admin.close()
        return True
    except Exception as e:
        logger.error(f"✗ Topic verification FAILED: {e}")
        return False


def verify_message_flow():
    """Verify test message can flow through topics."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: Verifying message flow")
    logger.info("=" * 60)

    try:
        config = KafkaConfig(bootstrap_servers=["localhost:9092"])
        producer = KafkaClient(config=config, backend="kafka-python")

        # Produce test invoice
        test_event = {
            "event_id": "test-invoice-001",
            "event_type": "invoice.created",
            "entity_id": "cust_test",
            "topic": "acis.invoices",
            "payload": {
                "invoice_id": "inv_test",
                "customer_id": "cust_test",
                "amount": 100.0,
                "due_date": "2026-05-01T00:00:00",
                "status": "pending",
                "created_at": time.time()
            },
            "correlation_id": "corr-test",
            "event_time": time.time(),
            "schema_version": "1.1"
        }

        producer.produce(
            topic="acis.invoices",
            key="cust_test",
            value=test_event
        )
        logger.info("✓ Produced test invoice.created event")

        # Try to consume it back (with new consumer group)
        time.sleep(1)  # Wait for message to be written

        consumer = KafkaClient(config=config, backend="kafka-python")
        consumer.subscribe(["acis.invoices"], "test-consumer-verify")
        logger.info("✓ Subscribed to acis.invoices")

        # Poll with timeout
        messages = consumer.poll(timeout_ms=5000, max_messages=1)

        if messages:
            logger.info(f"✓ Message received: {messages[0].value['event_type']}")
            consumer.close()
            producer.close()
            return True
        else:
            logger.error("✗ No messages received (poll timeout)")
            consumer.close()
            producer.close()
            return False

    except Exception as e:
        logger.error(f"✗ Message flow test FAILED: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def verify_subscriber_connection():
    """Verify agents can connect and subscribe."""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: Verifying agent subscription works")
    logger.info("=" * 60)

    try:
        config = KafkaConfig(bootstrap_servers=["localhost:9092"])

        # Simulate RiskScoringAgent subscription
        client = KafkaClient(config=config, backend="kafka-python")
        client.subscribe(
            topics=["acis.metrics", "acis.customers", "acis.predictions"],
            group_id="test-risk-scoring-verify"
        )

        logger.info("✓ Successfully subscribed to multiple topics")
        logger.info("  Topics: acis.metrics, acis.customers, acis.predictions")

        # Try to poll (should timeout if no messages)
        messages = client.poll(timeout_ms=1000)
        logger.info(f"✓ Poll executed (returned {len(messages)} messages)")

        client.close()
        return True

    except Exception as e:
        logger.error(f"✗ Subscription test FAILED: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Run all verification steps."""
    logger.info("\n\n")
    logger.info("╔" + "=" * 58 + "╗")
    logger.info("║  ACIS-X MESSAGE FLOW VERIFICATION                         ║")
    logger.info("╚" + "=" * 58 + "╝\n")

    results = []

    results.append(("Kafka Connectivity", verify_kafka_connectivity()))
    results.append(("ACIS Topics", verify_acis_topics_exist()))
    results.append(("Agent Subscription", verify_subscriber_connection()))
    results.append(("Message Flow", verify_message_flow()))

    logger.info("\n" + "=" * 60)
    logger.info("RESULTS SUMMARY")
    logger.info("=" * 60)

    all_pass = True
    for check, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status:8} {check}")
        all_pass = all_pass and result

    logger.info("=" * 60)

    if all_pass:
        logger.info("\n✓ All checks PASSED - Message flow infrastructure is OK")
        logger.info("\nIf agents still not processing messages:")
        logger.info("1. Check acis.log for agent startup errors")
        logger.info("2. Verify agents are actually starting (grep 'Starting agent')")
        logger.info("3. Check for exceptions in process_event() handlers")
        return 0
    else:
        logger.error("\n✗ Some checks FAILED - Fix issues above before running ACIS-X")
        return 1


if __name__ == "__main__":
    sys.exit(main())
