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
        self.top_reasons = defaultdict(int)
        
        # We need a separate lock for stats as they are accessed by consumer thread and stats thread
        self.stats_lock = threading.Lock()
        self.stats_thread = None

    def subscribe(self) -> List[str]:
        return ["acis.dlq"]

    def start(self):
        super().start()
        self.stats_thread = threading.Thread(
            target=self._stats_loop,
            daemon=True,
            name="dlq-stats-publisher"
        )
        self.stats_thread.start()

    def _stats_loop(self):
        # We wait to allow agent to fully initialize
        self._shutdown_event.wait(timeout=5)
        while self._running:
            try:
                self._publish_stats()
            except Exception as e:
                logger.error(f"Failed to publish dlq stats: {e}")
            
            self._shutdown_event.wait(timeout=60)

    def _publish_stats(self):
        with self.stats_lock:
            payload = {
                "total_dlq_count": self.total_dlq_count,
                "last_60s_count": self.last_60s_count,
                "top_reasons": dict(self.top_reasons)
            }
            # Reset rolling window stats
            self.last_60s_count = 0
            self.top_reasons.clear()
            
        self.publish_event(
            topic="acis.monitoring",
            event_type="dlq.stats",
            entity_id="system.dlq",
            payload=payload
        )

    def _handle_message(self, message: Any) -> None:
        raw = message.value
        if not isinstance(raw, dict):
            return

        dlq_reason = "unknown"
        original_topic = "unknown"
        event_id = raw.get("event_id", "unknown")

        metadata = raw.get("metadata", {})
        if isinstance(metadata, dict):
            dlq_reason = metadata.get("dlq_reason", "unknown")
            original_topic = metadata.get("original_topic", "unknown")

        logger.warning(f"DLQ Event Received: reason={dlq_reason}, original_topic={original_topic}, event_id={event_id}")

        with self.stats_lock:
            self.total_dlq_count += 1
            self.last_60s_count += 1
            self.top_reasons[dlq_reason] += 1

    def process_event(self, event: Event) -> None:
        pass
