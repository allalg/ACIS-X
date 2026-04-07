#!/usr/bin/env python3
"""
Event Schema Validation Diagnostic

Shows exactly what Event schema validation requires and helps identify
why events might be silently dropped.
"""

import json
import logging
from datetime import datetime
from schemas.event_schema import Event, validate_event, ValidationError

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def test_valid_event():
    """Test a properly formed event."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST 1: Valid Event (Should PASS)")
    logger.info("=" * 70)

    valid_event = {
        "event_id": "evt_test_001",
        "event_type": "invoice.created",
        "event_source": "ScenarioGeneratorAgent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "inv_00001",
        "schema_version": "1.1",
        "payload": {
            "invoice_id": "inv_00001",
            "customer_id": "cust_00001",
            "amount": 50000.0,
            "due_date": "2026-05-01T00:00:00",
            "status": "pending"
        },
        "correlation_id": "corr_test_001",
        "metadata": {"environment": "production"}
    }

    logger.info("\nEvent structure:")
    logger.info(json.dumps(valid_event, indent=2, default=str))

    try:
        event = Event.model_validate(valid_event)
        logger.info("\n✓ VALIDATION PASSED")
        logger.info(f"  Event ID: {event.event_id}")
        logger.info(f"  Event Type: {event.event_type}")
        logger.info(f"  Entity ID: {event.entity_id}")
        return True
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED: {e}")
        return False


def test_missing_fields():
    """Test event with missing required fields."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST 2: Missing Required Fields (Should FAIL)")
    logger.info("=" * 70)

    # Simulating what ScenarioGenerator might publish if publish_event() isn't called
    incomplete_event = {
        "event_type": "invoice.created",
        "payload": {
            "invoice_id": "inv_00001",
            "customer_id": "cust_00001",
            "amount": 50000.0
        }
    }

    logger.info("\nEvent structure (INCOMPLETE):")
    logger.info(json.dumps(incomplete_event, indent=2, default=str))

    logger.info("\nMissing fields:")
    required = {'event_id', 'event_type', 'event_source', 'event_time', 'entity_id', 'payload'}
    provided = set(incomplete_event.keys())
    missing = required - provided
    for field in missing:
        logger.info(f"  ❌ {field}")

    try:
        event = Event.model_validate(incomplete_event)
        logger.info("\n✓ VALIDATION PASSED (Unexpected!)")
        return False
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED (Expected)")
        logger.error(f"  Errors: {e.error_count()} validation error(s)")
        for error in e.errors():
            logger.error(f"    - {error['loc']}: {error['msg']}")
        return True


def test_invalid_datetime_format():
    """Test event with invalid datetime format."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST 3: Invalid DateTime Format (Should FAIL)")
    logger.info("=" * 70)

    invalid_datetime_event = {
        "event_id": "evt_test_002",
        "event_type": "invoice.created",
        "event_source": "ScenarioGeneratorAgent",
        "event_time": "2026-05-01",  # ❌ Missing time component!
        "entity_id": "inv_00002",
        "schema_version": "1.1",
        "payload": {
            "invoice_id": "inv_00002",
            "customer_id": "cust_00002",
            "amount": 60000.0
        }
    }

    logger.info("\nEvent structure:")
    logger.info(json.dumps(invalid_datetime_event, indent=2, default=str))

    logger.info("\n❌ Problem: event_time is date only, not datetime!")
    logger.info("   Given: '2026-05-01'")
    logger.info("   Expected: '2026-05-01T15:30:45.123456' or similar")

    try:
        event = Event.model_validate(invalid_datetime_event)
        logger.info("\n✓ VALIDATION PASSED (Unexpected!)")
        return False
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED (Expected)")
        for error in e.errors():
            logger.error(f"  - {error['loc']}: {error['msg']}")
        return True


def test_wrong_type():
    """Test event with wrong payload type."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST 4: Wrong Payload Type (Should FAIL)")
    logger.info("=" * 70)

    wrong_type_event = {
        "event_id": "evt_test_003",
        "event_type": "invoice.created",
        "event_source": "ScenarioGeneratorAgent",
        "event_time": datetime.utcnow().isoformat(),
        "entity_id": "inv_00003",
        "schema_version": "1.1",
        "payload": "not_a_dict"  # ❌ Should be Dict[str, Any]!
    }

    logger.info("\nEvent structure:")
    logger.info(json.dumps(wrong_type_event, indent=2, default=str))

    logger.info("\n❌ Problem: payload is a string, not a dictionary!")

    try:
        event = Event.model_validate(wrong_type_event)
        logger.info("\n✓ VALIDATION PASSED (Unexpected!)")
        return False
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED (Expected)")
        for error in e.errors():
            logger.error(f"  - {error['loc']}: {error['msg']}")
        return True


def test_missing_payload_datetime():
    """Test ScenarioGenerator event with string datetime in payload."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST 5: ScenarioGenerator Typical Event")
    logger.info("=" * 70)

    # This is what ScenarioGenerator creates (using publish_event)
    scenario_gen_event = {
        "event_id": "evt_sg_001",
        "event_type": "invoice.created",
        "event_source": "ScenarioGeneratorAgent",
        "event_time": datetime.utcnow().isoformat(),  # ✓ Proper datetime
        "entity_id": "inv_00004",
        "schema_version": "1.1",
        "payload": {
            "invoice_id": "inv_00004",
            "customer_id": "cust_00004",
            "amount": 75000.0,
            "due_date": "2026-05-15T00:00:00",  # ✓ String datetime in payload is OK
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": None,
            "line_items": [],
            "notes": None
        },
        "correlation_id": "corr_sg_001"
    }

    logger.info("\nEvent structure:")
    logger.info(json.dumps(scenario_gen_event, indent=2, default=str))

    logger.info("\n✓ Event has all required fields")

    try:
        event = Event.model_validate(scenario_gen_event)
        logger.info("\n✓ VALIDATION PASSED")
        logger.info(f"  Payload keys: {list(scenario_gen_event['payload'].keys())}")
        return True
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED (Unexpected!)")
        for error in e.errors():
            logger.error(f"  - {error}")
        return False


def main():
    """Run all diagnostic tests."""
    logger.info("\n")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║  EVENT SCHEMA VALIDATION DIAGNOSTIC                              ║")
    logger.info("╚" + "=" * 68 + "╝")

    logger.info("\n" + "=" * 70)
    logger.info("REQUIRED EVENT FIELDS (all mandatory unless Optional)")
    logger.info("=" * 70)
    logger.info("  ✓ event_id: str")
    logger.info("  ✓ event_type: str")
    logger.info("  ✓ event_source: str")
    logger.info("  ✓ event_time: datetime (ISO format)")
    logger.info("  ✓ entity_id: str")
    logger.info("  ✓ schema_version: str (default: '1.1')")
    logger.info("  ✓ payload: Dict[str, Any]")
    logger.info("  ○ correlation_id: Optional[str]")
    logger.info("  ○ metadata: Optional[Dict[str, Any]]")

    results = []
    results.append(("Valid Event", test_valid_event()))
    results.append(("Missing Fields Detection", test_missing_fields()))
    results.append(("Invalid DateTime Detection", test_invalid_datetime_format()))
    results.append(("Wrong Type Detection", test_wrong_type()))
    results.append(("ScenarioGen Event", test_missing_payload_datetime()))

    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)

    for check, result in results:
        status = "✓" if result else "✗"
        logger.info(f"  {status} {check}")

    logger.info("\n" + "=" * 70)
    logger.info("HOW TO DEBUG IN PRODUCTION")
    logger.info("=" * 70)
    logger.info("\n1. Check acis.log for validation failures:")
    logger.info("   tail -100 acis.log | grep 'EVENT VALIDATION FAILED'")
    logger.info("\n2. Look for patterns in failure messages")
    logger.info("\n3. Most common issues:")
    logger.info("   ❌ Missing 'event_time' (must be datetime, not date-only)")
    logger.info("   ❌ Missing 'event_id' or 'event_source'")
    logger.info("   ❌ payload is not a Dict")
    logger.info("   ❌ Agents not calling publish_event() properly")
    logger.info("\n4. Solution: Check that:")
    logger.info("   - All agents use BaseAgent.publish_event()")
    logger.info("   - Never bypass publish_event with direct Kafka.produce()")
    logger.info("\n" + "=" * 70)


if __name__ == "__main__":
    main()
