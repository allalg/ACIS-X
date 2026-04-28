import logging
import time
import uuid
import os
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
    _publisher_client = None
    _response_consumer = None
    _consumer_ready = False

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
    def query(cls, query_type: str, payload: Dict[str, Any], timeout: float = 5.0) -> Optional[Any]:
        publisher = cls._get_publisher()
        correlation_id = f"query_{uuid.uuid4().hex}"
        
        request_event = {
            "event_id": f"evt_{uuid.uuid4().hex}",
            "event_type": "query.request",
            "event_source": "QueryClient",
            "event_time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            "correlation_id": correlation_id,
            "entity_id": payload.get("customer_id", "unknown"),
            "schema_version": "1.1",
            "payload": {
                "query_type": query_type,
                "data": payload
            },
            "metadata": {}
        }
        
        # Initialize consumer once per process
        if cls._response_consumer is None:
            bootstrap_servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]
            backend = os.getenv("ACIS_KAFKA_BACKEND", "kafka-python")
            
            temp_group_id = f"query-client-temp-{uuid.uuid4().hex[:8]}"
            config = KafkaConfig(
                bootstrap_servers=servers,
                consumer_auto_offset_reset="latest",
                client_id=f"query_client_{uuid.uuid4().hex[:8]}"
            )
            
            cls._response_consumer = KafkaClient(config=config, backend=backend)
            cls._response_consumer.subscribe([cls._response_topic], group_id=temp_group_id)
            cls._consumer_ready = False

        if not cls._consumer_ready:
            # Force the consumer to join the group and get partition assignments
            assigned = False
            for _ in range(50):  # Wait up to 5 seconds for initial assignment
                cls._response_consumer.poll(timeout_ms=100)
                if hasattr(cls._response_consumer, "_consumer") and cls._response_consumer._consumer:
                    if cls._response_consumer._consumer.assignment():
                        assigned = True
                        break
                elif hasattr(cls._response_consumer, "_confluent_consumer") and cls._response_consumer._confluent_consumer:
                    if cls._response_consumer._confluent_consumer.assignment():
                        assigned = True
                        break
                        
            if assigned:
                cls._consumer_ready = True
            else:
                logger.warning(f"QueryClient consumer did not get assignments within 5s for {correlation_id}")
            
        publisher.publish(cls._request_topic, request_event)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                messages = cls._response_consumer.poll(timeout_ms=100)
                for message in messages:
                    if hasattr(message, "value") and isinstance(message.value, dict):
                        val = message.value
                        if val.get("correlation_id") == correlation_id:
                            return val.get("payload", {}).get("data")
            except Exception as e:
                logger.error(f"Error polling query response: {e}")
                time.sleep(0.1)
                
        raise QueryTimeoutError(f"Query {query_type} timed out after {timeout} seconds")
