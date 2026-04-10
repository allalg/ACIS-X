"""
Diagnostic script to trace customer name persistence issue.
"""

import sqlite3
import json
import logging
from typing import Optional, List, Dict, Any
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# 1. Check SQLite Database
# ============================================================================

def check_database() -> None:
    """Check what's in the customers table."""
    logger.info("\n=== DATABASE INSPECTION ===")
    try:
        conn = sqlite3.connect("acis.db")
        cursor = conn.cursor()

        # Check table schema
        cursor.execute("PRAGMA table_info(customers)")
        columns = cursor.fetchall()
        logger.info("Customers table schema:")
        for col in columns:
            logger.info(f"  - {col[1]} ({col[2]})")

        # Check record count
        cursor.execute("SELECT COUNT(*) FROM customers")
        count = cursor.fetchone()[0]
        logger.info(f"Total customer records: {count}")

        # Check for NULL names
        cursor.execute("SELECT COUNT(*) FROM customers WHERE name IS NULL")
        null_names = cursor.fetchone()[0]
        logger.info(f"Customer records with NULL name: {null_names}")

        # Sample records
        cursor.execute("""
            SELECT customer_id, name, risk_score, credit_limit, status, created_at
            FROM customers
            LIMIT 5
        """)
        records = cursor.fetchall()
        logger.info(f"Sample records (first 5):")
        for rec in records:
            logger.info(f"  - ID: {rec[0]}, Name: {rec[1]}, Risk: {rec[2]}, Limit: {rec[3]}, Status: {rec[4]}, Created: {rec[5]}")

        conn.close()
    except Exception as e:
        logger.error(f"Database check failed: {e}")

# ============================================================================
# 2. Check Kafka Topic - acis.customers
# ============================================================================

def check_kafka_topic() -> None:
    """Check acis.customers topic for messages."""
    logger.info("\n=== KAFKA TOPIC: acis.customers ===")
    try:
        # Admin client to check consumer groups and lag
        admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'], request_timeout_ms=5000)

        # Check consumer groups
        try:
            groups = admin.list_consumer_groups()[0]  # Returns (groups, errors)
            logger.info(f"Consumer groups: {len(groups)} found")

            # Find db-agent-group
            db_agent_groups = [g for g in groups if 'db-agent' in g[0]]
            for group_id, group_type in db_agent_groups:
                logger.info(f"  - Found: {group_id} (type: {group_type})")
        except Exception as e:
            logger.warning(f"Could not list consumer groups: {e}")

        # Create consumer to check messages
        consumer = KafkaConsumer(
            'acis.customers',
            bootstrap_servers=['localhost:9092'],
            group_id=f'diagnostic-group-{__import__("uuid").uuid4().hex[:8]}',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            timeout_ms=5000,
        )

        # Poll for messages
        records = []
        for message in consumer:
            records.append(message.value)
            if len(records) >= 5:
                break

        logger.info(f"Messages found in acis.customers: {len(records)}")

        # Inspect first few messages
        for i, msg in enumerate(records[:3]):
            if msg:
                logger.info(f"\n  Message {i+1}:")
                logger.info(f"    event_type: {msg.get('event_type')}")
                logger.info(f"    entity_id: {msg.get('entity_id')}")

                payload = msg.get('payload', {})
                logger.info(f"    payload.customer_id: {payload.get('customer_id')}")
                logger.info(f"    payload.customer_name: {payload.get('customer_name')}")
                logger.info(f"    payload.name: {payload.get('name')}")
                logger.info(f"    payload keys: {list(payload.keys())}")

        consumer.close()
        admin.close()

    except Exception as e:
        logger.error(f"Kafka check failed: {e}")

# ============================================================================
# 3. Check if DBAgent is processing messages
# ============================================================================

def check_agent_processing() -> None:
    """Check the acis.log for evidence of customer profile processing."""
    logger.info("\n=== AGENT PROCESSING LOG ===")
    try:
        with open("acis.log", "r") as f:
            lines = f.readlines()

        # Search for relevant log messages
        customer_profile_lines = [l for l in lines if 'customer.profile' in l.lower()]
        db_agent_customer_lines = [l for l in lines if 'dbagent' in l.lower() and 'customer' in l.lower()]
        validation_failed = [l for l in lines if 'validation failed' in l.lower() and 'customer' in l.lower()]

        logger.info(f"'customer.profile' mentions: {len(customer_profile_lines)}")
        if customer_profile_lines:
            logger.info(f"  Latest: {customer_profile_lines[-1].strip()}")

        logger.info(f"DBAgent customer-related lines: {len(db_agent_customer_lines)}")
        if db_agent_customer_lines:
            logger.info(f"  Latest: {db_agent_customer_lines[-1].strip()}")

        logger.info(f"Validation failures: {len(validation_failed)}")
        if validation_failed:
            for line in validation_failed[-3:]:
                logger.info(f"  {line.strip()}")

    except Exception as e:
        logger.error(f"Log check failed: {e}")

# ============================================================================
# Main
# ============================================================================

def main() -> None:
    """Run all diagnostics."""
    logger.info("=" * 60)
    logger.info("CUSTOMER NAME PERSISTENCE DIAGNOSTIC")
    logger.info("=" * 60)

    check_database()
    check_kafka_topic()
    check_agent_processing()

    logger.info("\n" + "=" * 60)
    logger.info("DIAGNOSTIC COMPLETE")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
