"""
Diagnostic script to trace customer name persistence issue.

Uses the project's KafkaClient abstraction (runtime/kafka_client.py) with:
  - backend="kafka-python" for consistency with the production stack
  - group_id="acis-diagnostic-readonly" — a stable, dedicated consumer group
    that never interferes with the production db-agent-group offsets
  - auto_offset_reset="earliest" so all historical messages are visible

Events are validated against EventEnvelope before being inspected, mirroring
the validation that production agents perform.
"""

import sqlite3
import logging
import sys
import os
import json
from typing import Optional, List, Dict, Any

# Allow running from the project root without installing the package
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from runtime.kafka_client import KafkaClient, KafkaConfig

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

_BOOTSTRAP_SERVERS = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_BOOTSTRAP_LIST = [s.strip() for s in _BOOTSTRAP_SERVERS.split(",") if s.strip()]
_DB_PATH = os.getenv("ACIS_DB_PATH", "acis.db")

# Dedicated read-only consumer group.  Using a unique group name means this
# script NEVER moves production offsets and can be safely re-run at any time.
_DIAGNOSTIC_GROUP = "acis-diagnostic-readonly"


def _build_kafka_client() -> KafkaClient:
    """Create a diagnostic KafkaClient using the project's standard abstraction."""
    config = KafkaConfig(
        bootstrap_servers=_BOOTSTRAP_LIST,
        # Earliest so we see the full history of the topic.
        consumer_auto_offset_reset="earliest",
    )
    return KafkaClient(config=config, backend="kafka-python")


# ============================================================================
# 1. Check SQLite Database
# ============================================================================

def check_database() -> None:
    """Check what's in the customers table."""
    logger.info("\n=== DATABASE INSPECTION ===")
    try:
        conn = sqlite3.connect(_DB_PATH)
        cursor = conn.cursor()

        # Check table schema
        cursor.execute("PRAGMA table_info(customers)")
        columns = cursor.fetchall()
        logger.info("Customers table schema:")
        for col in columns:
            logger.info("  - %s (%s)", col[1], col[2])

        # Check record count
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        logger.info("Total customer records: %s", count)

        # Check for NULL names
        cursor.execute("SELECT COUNT(*) FROM customers WHERE name IS NULL")
        null_names = cursor.fetchone()[0]
        logger.info("Customer records with NULL name: %s", null_names)

        # Sample records
        cursor.execute("""
            SELECT customer_id, name, risk_score, credit_limit, status, created_at
            FROM customers
            LIMIT 5
        """)
        records = cursor.fetchall()
        logger.info("Sample records (first 5):")
        for rec in records:
            logger.info(
                "  - ID: %s  Name: %s  Risk: %s  Limit: %s  Status: %s  Created: %s",
                rec[0], rec[1], rec[2], rec[3], rec[4], rec[5],
            )

        conn.close()
    except Exception as e:
        logger.error("Database check failed: %s", e)


# ============================================================================
# 2. Check Kafka Topic — acis.customers
# ============================================================================

def check_kafka_topic() -> None:
    """Read acis.customers using the project's KafkaClient with EventEnvelope validation."""
    logger.info("\n=== KAFKA TOPIC: acis.customers ===")
    client = _build_kafka_client()
    try:
        client.subscribe(
            topics=["acis.customers"],
            group_id=_DIAGNOSTIC_GROUP,
        )

        # Poll until we have at least 5 messages or the topic is exhausted.
        records: List[Any] = []
        invalid_count = 0
        for _ in range(20):                    # up to 20 poll iterations
            messages = client.poll(timeout_ms=3000, max_messages=5)
            if not messages:
                break
            for msg in messages:
                # validate_event() parses and validates against the Event schema
                event = client.validate_event(msg.value)
                if event is None:
                    invalid_count += 1
                    logger.warning("  Invalid event on acis.customers (failed schema validation)")
                    continue
                records.append(event)
                if len(records) >= 5:
                    break
            if len(records) >= 5:
                break

        logger.info("Valid messages found in acis.customers: %d", len(records))
        if invalid_count:
            logger.warning("  %d messages failed EventEnvelope validation", invalid_count)

        for i, event in enumerate(records[:3]):
            logger.info("\n  Message %d:", i + 1)
            logger.info("    event_type: %s", event.event_type)
            logger.info("    entity_id:  %s", event.entity_id)
            payload = event.payload or {}
            logger.info("    payload.customer_id:   %s", payload.get("customer_id"))
            logger.info("    payload.customer_name: %s", payload.get("customer_name"))
            logger.info("    payload.name:          %s", payload.get("name"))
            logger.info("    payload keys: %s", list(payload.keys()))

    except Exception as e:
        logger.error("Kafka check failed: %s", e)
    finally:
        try:
            client.close()
        except Exception:
            pass


# ============================================================================
# 3. Check if DBAgent is processing messages
# ============================================================================

def check_agent_processing() -> None:
    """Check acis.log for evidence of customer profile processing."""
    logger.info("\n=== AGENT PROCESSING LOG ===")
    log_path = os.getenv("ACIS_LOG_PATH", "acis.log")
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()

        customer_profile_lines = [l for l in lines if "customer.profile" in l.lower()]
        db_agent_customer_lines = [
            l for l in lines if "dbagent" in l.lower() and "customer" in l.lower()
        ]
        validation_failed = [
            l for l in lines
            if "validation failed" in l.lower() and "customer" in l.lower()
        ]

        logger.info("'customer.profile' mentions:    %d", len(customer_profile_lines))
        if customer_profile_lines:
            logger.info("  Latest: %s", customer_profile_lines[-1].strip())

        logger.info("DBAgent customer-related lines: %d", len(db_agent_customer_lines))
        if db_agent_customer_lines:
            logger.info("  Latest: %s", db_agent_customer_lines[-1].strip())

        logger.info("Validation failures:            %d", len(validation_failed))
        for line in validation_failed[-3:]:
            logger.info("  %s", line.strip())

    except FileNotFoundError:
        logger.warning("Log file not found at '%s' — is ACIS-X running?", log_path)
    except Exception as e:
        logger.error("Log check failed: %s", e)


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """Run all diagnostics."""
    logger.info("=" * 60)
    logger.info("CUSTOMER NAME PERSISTENCE DIAGNOSTIC")
    logger.info("Bootstrap servers: %s", _BOOTSTRAP_LIST)
    logger.info("Consumer group:    %s (read-only, never moves production offsets)", _DIAGNOSTIC_GROUP)
    logger.info("=" * 60)

    check_database()
    check_kafka_topic()
    check_agent_processing()

    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
