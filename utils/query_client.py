import logging
import threading
import time
import uuid
import os
from datetime import datetime
from typing import Any, Dict, Optional

from runtime.kafka_client import KafkaClient, KafkaConfig

logger = logging.getLogger(__name__)

class QueryTimeoutError(Exception):
    pass

class QueryClient:
    """
    Helper class to send synchronous queries via Kafka.
    """
    _request_topic = "acis.query.request"
    _response_topic = "acis.query.response"

    # Publisher is process-wide (producers are thread-safe in kafka-python /
    # confluent-kafka).
    _publisher_client = None

    # Per-thread consumer store: each thread keeps its own KafkaClient consumer
    # and a boolean flag indicating whether it has received partition assignments.
    # Using threading.local() prevents threads from sharing or corrupting each
    # other's consumer state.
    _thread_local: threading.local = threading.local()

    @classmethod
    def _get_publisher(cls) -> KafkaClient:
        if cls._publisher_client is None:
            bootstrap_servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]
            config = KafkaConfig(
                bootstrap_servers=servers,
            )
            backend = os.getenv("ACIS_KAFKA_BACKEND", "kafka-python")
            cls._publisher_client = KafkaClient(config=config, backend=backend)
        return cls._publisher_client

    @classmethod
    def _get_or_create_consumer(cls) -> KafkaClient:
        """
        Return the Kafka consumer for the calling thread, creating one on first use.

        Each thread receives its own KafkaClient subscribed to the response topic
        under a group_id that is unique to the thread
        (``f"query-client-{threading.get_ident()}"``) so that:

        * Response messages are never stolen by a consumer on another thread.
        * Consumer group state (committed offsets, heartbeats) is isolated per
          thread, eliminating race conditions on the ready flag.

        The consumer is stored in ``cls._thread_local`` and reused across calls
        within the same thread.
        """
        tl = cls._thread_local

        if not getattr(tl, "consumer", None):
            bootstrap_servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
            backend = os.getenv("ACIS_KAFKA_BACKEND", "kafka-python")

            group_id = f"query-client-{threading.get_ident()}"
            config = KafkaConfig(
                bootstrap_servers=servers,
                consumer_auto_offset_reset="latest",
                client_id=f"query_client_{uuid.uuid4().hex[:8]}",
            )

            consumer = KafkaClient(config=config, backend=backend)
            consumer.subscribe([cls._response_topic], group_id=group_id)

            tl.consumer = consumer
            tl.consumer_ready = False
            logger.debug(
                "[QueryClient] Created per-thread consumer (thread=%s, group=%s)",
                threading.get_ident(),
                group_id,
            )

        return tl.consumer

    @classmethod
    def query(cls, query_type: str, payload: Dict[str, Any], timeout: float = 5.0) -> Optional[Any]:
        publisher = cls._get_publisher()
        correlation_id = f"query_{uuid.uuid4().hex}"
        
        request_event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_type": "query.request",
            "event_source": "QueryClient",
            "event_time": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id,
            "entity_id": payload.get("customer_id", "unknown"),
            "schema_version": "1.1",
            "payload": {
                "query_type": query_type,
                "data": payload
            },
            "metadata": {}
        }
        
        # Obtain (or lazily create) the per-thread consumer.
        consumer = cls._get_or_create_consumer()
        tl = cls._thread_local

        if not tl.consumer_ready:
            # Force the consumer to join the group and get partition assignments.
            assigned = False
            for _ in range(50):  # Wait up to 5 seconds for initial assignment
                consumer.poll(timeout_ms=100)
                if hasattr(consumer, "_consumer") and consumer._consumer:
                    if consumer._consumer.assignment():
                        assigned = True
                        break
                elif hasattr(consumer, "_confluent_consumer") and consumer._confluent_consumer:
                    if consumer._confluent_consumer.assignment():
                        assigned = True
                        break
                        
            if assigned:
                tl.consumer_ready = True
            else:
                logger.warning(
                    "[QueryClient] Consumer did not get assignments within 5s "
                    "(thread=%s, correlation_id=%s)",
                    threading.get_ident(),
                    correlation_id,
                )
            
        publisher.publish(cls._request_topic, request_event)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                messages = consumer.poll(timeout_ms=100)
                for message in messages:
                    if hasattr(message, "value") and isinstance(message.value, dict):
                        val = message.value
                        if val.get("correlation_id") == correlation_id:
                            return val.get("payload", {}).get("data")
            except Exception as e:
                logger.error(f"Error polling query response: {e}")
                time.sleep(0.1)
                
        raise QueryTimeoutError(f"Query {query_type} timed out after {timeout} seconds")
