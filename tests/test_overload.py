import time
from datetime import datetime

from runtime.kafka_client import KafkaClient, KafkaConfig
from schemas.event_schema import Event


BOOTSTRAP = ["localhost:9092"]

TARGET_AGENT_NAME = "ScenarioGeneratorAgent"


def find_agent_id(kafka: KafkaClient):

    print("Looking for agent in registry...")

    kafka.subscribe(
        topics=["acis.registry"],
        group_id="test-overload"
    )

    start = time.time()

    while time.time() - start < 10:

        messages = kafka.poll()

        if not messages:
            continue

        for msg in messages:

            event = msg.value

            if event["event_type"] == "registry.agent.registered":

                payload = event["payload"]

                if payload.get("agent_name") == TARGET_AGENT_NAME:

                    agent_id = payload["agent_id"]

                    print("Found agent:", agent_id)

                    return agent_id

    raise RuntimeError("Agent not found")


def create_overload_event(agent_id):

    return Event(
        event_id=f"overload_{int(time.time())}",
        event_type="agent.overloaded",
        event_source="tests.overload",
        event_time=datetime.utcnow(),
        entity_id=agent_id,
        schema_version="1.1",
        payload={
            "agent_id": agent_id,
            "cpu_percent": 99,
            "memory_percent": 95,
            "queue_depth": 1000,
            "consumer_lag": 500,
            "error_count": 10,
            "latency_ms": 1500,
            "status": "overloaded",
        },
        metadata={},
    )


def main():

    kafka = KafkaClient(
        config=KafkaConfig(
            bootstrap_servers=BOOTSTRAP
        )
    )

    agent_id = find_agent_id(kafka)

    event = create_overload_event(agent_id)

    print("Sending overload to:", agent_id)

    kafka.publish(
        topic="acis.agent.health",
        event=event.model_dump(),
        key=agent_id,
    )

    print("Done")


if __name__ == "__main__":
    main()