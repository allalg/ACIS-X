import sys
import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer

def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("pause", "resume"):
        print("Usage: python control_scenario.py [pause|resume]")
        sys.exit(1)

    action = sys.argv[1]
    event_type = f"scenario.{action}"

    producer_config = {
        "bootstrap.servers": "localhost:9092",
    }
    producer = Producer(producer_config)

    event = {
        "event_id": f"evt_{uuid.uuid4().hex}",
        "event_type": event_type,
        "event_source": "control_scenario_script",
        "event_time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z",
        "correlation_id": f"ctrl_{uuid.uuid4().hex}",
        "entity_id": "scenario_generator",
        "schema_version": "1.1",
        "payload": {},
        "metadata": {}
    }

    try:
        producer.produce(
            topic="acis.control",
            key="scenario_generator",
            value=json.dumps(event).encode("utf-8")
        )
        producer.flush()
        print(f"Successfully published '{event_type}' control event to Kafka topic 'acis.control'")
    except Exception as e:
        print(f"Failed to publish control event: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
