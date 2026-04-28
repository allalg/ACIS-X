import logging
import threading
import time
from datetime import datetime
from typing import Any, List, Dict
from collections import defaultdict

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class DLQMonitorAgent(BaseAgent):
    """
    Monitors acis.dlq for failed events and publishes statistics.

    BaseAgent._handle_message() validates the raw Kafka message, parses it
    into an Event object, and then calls process_event().  DLQMonitorAgent
    uses process_event() to access the already-parsed Event rather than
    overriding _handle_message() to touch message.value directly.  This
    keeps the agent resilient to any future changes in BaseAgent's internal
    message-handling pipeline.

    DLQ event structure (via Event.payload and Event.metadata):
      metadata.dlq_reason        — why the event was dead-lettered
      metadata.original_topic    — the topic it failed on
      event_id                   — original event identifier
    """

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="DLQMonitorAgent",
            agent_version="1.0.0",
            group_id="dlq-monitor-group",
            subscribed_topics=["acis.dlq"],
            capabilities=["dlq_monitoring"],
            kafka_client=kafka_client,
        )
        self.total_dlq_count = 0
        self.last_60s_count = 0
        self.top_reasons: Dict[str, int] = defaultdict(int)

        # Separate lock for stats: accessed by consumer thread and stats thread.
        self.stats_lock = threading.Lock()
        self.stats_thread = None

    def subscribe(self) -> List[str]:
        return ["acis.dlq"]

    def start(self) -> None:
        super().start()
        self.stats_thread = threading.Thread(
            target=self._stats_loop,
            daemon=True,
            name="dlq-stats-publisher",
        )
        self.stats_thread.start()

    def _stats_loop(self) -> None:
        # Brief wait so the agent fully initialises before the first publish.
        self._shutdown_event.wait(timeout=5)
        while self._running:
            try:
                self._publish_stats()
            except Exception as e:
                logger.error("Failed to publish dlq stats: %s", e)
            self._shutdown_event.wait(timeout=60)

    def _publish_stats(self) -> None:
        with self.stats_lock:
            payload = {
                "total_dlq_count": self.total_dlq_count,
                "last_60s_count": self.last_60s_count,
                "top_reasons": dict(self.top_reasons),
            }
            # Reset rolling-window counters for the next interval.
            self.last_60s_count = 0
            self.top_reasons.clear()

        self.publish_event(
            topic="acis.monitoring",
            event_type="dlq.stats",
            entity_id="system.dlq",
            payload=payload,
        )

    def process_event(self, event: Event) -> None:
        """Process a DLQ event that BaseAgent has already validated and parsed.

        BaseAgent._handle_message() is responsible for deserialising the raw
        Kafka message and calling this method with a fully validated Event
        object.  We extract DLQ metadata from event.metadata (where
        dead-letter context is stored) and payload fields as needed.

        DLQ convention:
          event.metadata["dlq_reason"]     — why the event was rejected
          event.metadata["original_topic"] — the topic it originally failed on
          event.event_id                   — the original event's ID
        """
        metadata = event.metadata or {}
        payload = event.payload or {}

        dlq_reason = metadata.get("dlq_reason", "unknown")
        original_topic = metadata.get("original_topic", "unknown")
        # Prefer the event_id embedded in the payload (the *original* event's
        # ID) if available; fall back to the DLQ wrapper event's own ID.
        event_id = payload.get("event_id", event.event_id)

        logger.warning(
            "DLQ Event: reason=%s  original_topic=%s  event_id=%s",
            dlq_reason, original_topic, event_id,
        )

        with self.stats_lock:
            self.total_dlq_count += 1
            self.last_60s_count += 1
            self.top_reasons[dlq_reason] += 1
