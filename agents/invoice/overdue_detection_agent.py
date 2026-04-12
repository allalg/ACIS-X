"""
OverdueDetectionAgent for ACIS-X.

Detects overdue invoices based on time ticks and emits invoice.overdue events.
Stateless agent that queries DB for unpaid invoices.
"""

import logging
from datetime import datetime, timezone
from typing import List, Any, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class OverdueDetectionAgent(BaseAgent):
    """
    Overdue Detection Agent for ACIS-X.

    Stateless agent that queries the DB to detect overdue invoices.
    Does NOT maintain its own state - reads from QueryAgent.

    Subscribes to:
    - time.tick: Trigger overdue checks

    Produces:
    - invoice.overdue: Emitted when an invoice becomes overdue
    """

    # Topic constants (aligned with existing system conventions)
    TOPIC_INVOICES = "acis.invoices"
    TOPIC_TIME = "acis.time"

    def __init__(
        self,
        kafka_client: Any,
        query_agent: Any,
    ):
        super().__init__(
            agent_name="OverdueDetectionAgent",
            agent_version="1.0.0",
            group_id="overdue-detection-group",
            subscribed_topics=[
                self.TOPIC_TIME,
            ],
            capabilities=[
                "overdue_detection",
                "invoice_tracking",
            ],
            kafka_client=kafka_client,
            agent_type="OverdueDetectionAgent",
        )

        self._query_agent = query_agent

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_TIME]

    def process_event(self, event: Event) -> None:
        """Process incoming events based on event type."""
        event_type = event.event_type

        if event_type == "time.tick":
            self.handle_time_tick(event)
        else:
            logger.debug(f"Ignoring unhandled event type: {event_type}")


    def _to_naive_utc(self, dt: datetime) -> datetime:
        """
        Convert datetime to naive UTC for safe comparison.

        Handles both timezone-aware and naive datetimes to avoid
        TypeError when comparing mixed types.
        """
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def handle_time_tick(self, event: Event) -> None:
        """
        Handle time.tick event.

        Queries QueryAgent for unpaid invoices and emits invoice.overdue events
        for those that are overdue.

        Expected payload:
        - current_time: str (ISO format)
        """
        payload = event.payload or {}
        current_time_str = payload.get("current_time")

        if not current_time_str:
            logger.warning("Ignoring time.tick event: missing current_time")
            return

        current_time = self._parse_datetime(current_time_str)
        if current_time is None:
            logger.warning("Ignoring time.tick event: invalid current_time format")
            return

        # Normalize to naive UTC for safe comparison
        current_time = self._to_naive_utc(current_time)

        try:
            logger.debug(f"Time tick at {current_time_str}: checking for overdue invoices")

            invoices = self._query_agent.get_unpaid_invoices()

            for invoice in invoices:
                invoice_id = invoice.get("invoice_id")
                customer_id = invoice.get("customer_id")
                due_date_str = invoice.get("due_date")

                if not invoice_id or not customer_id or not due_date_str:
                    continue

                due_date = self._parse_datetime(due_date_str)
                if due_date is None:
                    continue

                due_date = self._to_naive_utc(due_date)

                if current_time > due_date:
                    self._emit_overdue(
                        invoice_id=invoice_id,
                        customer_id=customer_id,
                        total_amount=invoice.get("total_amount"),
                        remaining_amount=invoice.get("remaining_amount"),
                        due_date=due_date,
                        current_time=current_time,
                        correlation_id=event.correlation_id,
                    )

        except Exception as e:
            logger.error(f"Error during overdue detection: {e}")

    def _emit_overdue(
        self,
        invoice_id: str,
        customer_id: str,
        total_amount: Optional[Any],
        remaining_amount: Optional[Any],
        due_date: datetime,
        current_time: datetime,
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Emit invoice.overdue event.

        DBAgent listens to this event and updates invoice status to 'overdue' in DB.
        """
        # Calculate overdue days
        delta = current_time - due_date
        overdue_days = max(1, int(delta.total_seconds() // 86400) + 1)

        # Build payload
        overdue_payload = {
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "total_amount": max(float(total_amount or 0.0), 0.0),
            "remaining_amount": max(float(remaining_amount or total_amount or 0.0), 0.0),
            "due_date": due_date.isoformat(),
            "overdue_days": overdue_days,
            "timestamp": current_time.isoformat(),
            "status": "overdue",
        }

        # Publish the event
        self.publish_event(
            topic=self.TOPIC_INVOICES,
            event_type="invoice.overdue",
            entity_id=invoice_id,
            payload=overdue_payload,
            correlation_id=correlation_id,
        )

        logger.info(
            f"Emitted invoice.overdue for invoice {invoice_id}, "
            f"customer {customer_id}, overdue_days={overdue_days}"
        )

    def _parse_datetime(self, dt_string: str) -> Optional[datetime]:
        """
        Parse an ISO format datetime string.

        Supports:
        - Full ISO format: 2024-01-15T10:30:00
        - With microseconds: 2024-01-15T10:30:00.123456
        - With timezone: 2024-01-15T10:30:00Z or 2024-01-15T10:30:00+00:00
        - Date only: 2024-01-15

        Returns None if parsing fails.
        """
        if not dt_string:
            return None

        # Normalize Z suffix to +00:00
        if dt_string.endswith("Z"):
            dt_string = dt_string[:-1] + "+00:00"

        # Try various formats
        formats = [
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]

        for fmt in formats:
            try:
                return datetime.strptime(dt_string, fmt)
            except ValueError:
                continue

        logger.warning(f"Failed to parse datetime: {dt_string}")
        return None

