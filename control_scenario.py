import sys
import json
import os
import uuid
from datetime import datetime, timezone

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOOTSTRAP_SERVERS = os.environ.get("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
BACKEND = os.environ.get("ACIS_KAFKA_BACKEND", "kafka-python")


def _make_producer():
    if BACKEND == "confluent":
        from confluent_kafka import Producer as ConfluentProducer
        return "confluent", ConfluentProducer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    else:
        from kafka import KafkaProducer
        return "kafka-python", KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
            value_serializer=lambda v: v,
        )


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("pause", "resume"):
        print("Usage: python control_scenario.py [pause|resume]")
        sys.exit(1)

    action = sys.argv[1]
    event_type = f"scenario.{action}"

    backend, producer = _make_producer()

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

    payload = json.dumps(event).encode("utf-8")

    try:
        if backend == "confluent":
            producer.produce(
                topic="acis.control",
                key="scenario_generator",
                value=payload,
            )
            producer.flush()
        else:
            producer.send("acis.control", key=b"scenario_generator", value=payload)
            producer.flush()
        print(f"[{backend}] Successfully published '{event_type}' control event to Kafka topic 'acis.control'")
    except Exception as e:
        print(f"Failed to publish control event: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
