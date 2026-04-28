"""
Time Tick Agent for ACIS-X.

Publishes time tick events every 5 seconds.
Required for overdue detection and time-based logic to function.

Produces:
- acis.time (time.tick events with current timestamp)
"""

import logging
import threading
import time
from datetime import datetime
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class TimeTickAgent(BaseAgent):
    """
    Time Tick Agent for ACIS-X.

    Publishes time tick events every 5 seconds to enable time-based logic:
    - Overdue detection (compares invoice due_date with current time)
    - Late payment detection
    - SLA tracking
    - Temporal risk analysis

    Produces:
    - acis.time (time.tick events)
    """

    TOPIC_TIME = "acis.time"
    TICK_INTERVAL_SECONDS = 5

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="TimeTickAgent",
            agent_version="1.0.0",
            group_id="time-tick-group",
            subscribed_topics=[],  # No input topics - only produces
            capabilities=[
                "time_generation",
                "tick_publishing",
            ],
            kafka_client=kafka_client,
            agent_type="TimeTickAgent",
        )
        self._running = False
        self._tick_thread = None
        # Event used to (a) interrupt the startup delay and (b) wake the
        # inter-tick sleep early when stop() is called.
        self._shutdown_event = threading.Event()

    def subscribe(self) -> List[str]:
        """TimeTickAgent produces only, does not consume."""
        return []

    def process_event(self, event: Event) -> None:
        """TimeTickAgent does not process incoming events."""
        pass

    def start(self) -> None:
        """Start the time tick agent with full lifecycle."""
        # Register, publish card, start heartbeat (from BaseAgent)
        super().start()

        # Then start the tick generator loop
        logger.info("[TimeTickAgent] Starting time tick generator loop")
        self._tick_thread = threading.Thread(
            target=self._tick_loop,
            daemon=True,
            name="TimeTickAgent-tick"
        )
        self._tick_thread.start()

    def stop(self) -> None:
        """Stop the time tick agent gracefully."""
        logger.info("[TimeTickAgent] Stopping time tick agent")
        self._running = False
        # Wake the tick loop immediately (startup wait or inter-tick sleep).
        self._shutdown_event.set()
        if self._tick_thread:
            self._tick_thread.join(timeout=2)
        # Deregister, stop heartbeat (from BaseAgent)
        super().stop()

    def _tick_loop(self) -> None:
        """Main loop: publish time ticks every 5 seconds.

        A 1-second startup delay is applied before the first tick so that the
        Kafka producer's lazy initialisation (connection setup, metadata fetch)
        can complete before ``publish_event`` is called for the first time.
        If ``stop()`` is called during this wait the loop exits immediately.
        """
        # --- startup delay ---------------------------------------------------
        self._shutdown_event.wait(timeout=1.0)
        if not self._running:
            logger.info("[TimeTickAgent] Shutdown requested during startup delay, exiting tick loop")
            return
        # ---------------------------------------------------------------------

        tick_count = 0
        while self._running:
            try:
                current_time = datetime.utcnow()
                tick_count += 1

                # Publish time tick event
                self.publish_event(
                    topic=self.TOPIC_TIME,
                    event_type="time.tick",
                    entity_id="system",
                    payload={
                        "current_time": current_time.isoformat(),
                        "timestamp": current_time.timestamp(),
                        "tick_count": tick_count,
                    },
                )

                logger.debug(
                    f"[TimeTickAgent] Published tick #{tick_count}: {current_time.isoformat()}"
                )

                # Sleep for tick interval, but wake immediately on shutdown.
                self._shutdown_event.wait(timeout=self.TICK_INTERVAL_SECONDS)

            except Exception as e:
                logger.error(f"[TimeTickAgent] Error in tick loop: {e}")
                self._shutdown_event.wait(timeout=1)  # Back off on error
