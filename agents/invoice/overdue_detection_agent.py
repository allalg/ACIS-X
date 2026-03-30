"""
OverdueDetectionAgent for ACIS-X.

Detects overdue invoices based on time ticks and emits invoice.overdue events.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class OverdueDetectionAgent(BaseAgent):
    """
    Overdue Detection Agent for ACIS-X.

    Tracks invoice creation and payment events to detect overdue invoices.

    Subscribes to:
    - invoice.created: Track new invoices with due dates
    - payment.received: Mark invoices as paid
    - invoice.paid: Mark invoices as paid (alternative event)
    - time.tick: Check for overdue invoices

    Produces:
    - invoice.overdue: Emitted when an invoice becomes overdue
    """

    # Topic constants (aligned with existing system conventions)
    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_TIME = "acis.time"

    # Invoice status constants
    STATUS_PENDING = "PENDING"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="OverdueDetectionAgent",
            agent_version="1.0.0",
            group_id="overdue-detection-group",
            subscribed_topics=[
                self.TOPIC_INVOICES,
                self.TOPIC_PAYMENTS,
                self.TOPIC_TIME,
            ],
            capabilities=[
                "overdue_detection",
                "invoice_tracking",
            ],
            kafka_client=kafka_client,
            agent_type="OverdueDetectionAgent",
        )

        # In-memory state for tracking invoices
        # Format: {invoice_id: {"customer_id": str, "due_date": datetime, "status": str}}
        self._tracked_invoices: Dict[str, Dict[str, Any]] = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [
            self.TOPIC_INVOICES,
            self.TOPIC_PAYMENTS,
            self.TOPIC_TIME,
        ]

    def process_event(self, event: Event) -> None:
        """Process incoming events based on event type."""
        event_type = event.event_type

        if event_type == "invoice.created":
            self.handle_invoice_created(event)
        elif self._is_payment_event(event_type):
            self.handle_payment(event)
        elif event_type == "time.tick":
            self.handle_time_tick(event)
        else:
            logger.debug(f"Ignoring unhandled event type: {event_type}")

    def _is_payment_event(self, event_type: str) -> bool:
        """
        Check if event type indicates a payment.

        Handles various payment event naming conventions:
        - payment.received
        - payment.created
        - invoice.paid
        """
        return (
            event_type.startswith("payment.") or
            event_type == "invoice.paid"
        )

    def _to_naive_utc(self, dt: datetime) -> datetime:
        """
        Convert datetime to naive UTC for safe comparison.

        Handles both timezone-aware and naive datetimes to avoid
        TypeError when comparing mixed types.
        """
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def handle_event(self, event: Event) -> None:
        """Alias for process_event to maintain consistency with other agents."""
        self.process_event(event)

    def handle_invoice_created(self, event: Event) -> None:
        """
        Handle invoice.created event.

        Adds the invoice to tracking with PENDING status.

        Expected payload:
        - invoice_id: str
        - customer_id: str
        - due_date: str (ISO format)
        - amount: float (optional, not used for overdue detection)
        """
        payload = event.payload or {}

        invoice_id = payload.get("invoice_id")
        customer_id = payload.get("customer_id")
        due_date_str = payload.get("due_date")

        # Validate required fields
        if not invoice_id:
            logger.warning("Ignoring invoice.created event: missing invoice_id")
            return

        if not customer_id:
            logger.warning(f"Ignoring invoice.created event for {invoice_id}: missing customer_id")
            return

        if not due_date_str:
            logger.warning(f"Ignoring invoice.created event for {invoice_id}: missing due_date")
            return

        # Parse due_date
        due_date = self._parse_datetime(due_date_str)
        if due_date is None:
            logger.warning(f"Ignoring invoice.created event for {invoice_id}: invalid due_date format")
            return

        # Warn on duplicate invoice
        if invoice_id in self._tracked_invoices:
            logger.warning(f"Duplicate invoice.created for {invoice_id}, overwriting")

        # Track the invoice
        self._tracked_invoices[invoice_id] = {
            "customer_id": customer_id,
            "due_date": due_date,
            "status": self.STATUS_PENDING,
        }

        logger.info(
            f"Tracking invoice {invoice_id} for customer {customer_id}, "
            f"due_date={due_date.isoformat()}"
        )

    def handle_payment(self, event: Event) -> None:
        """
        Handle payment events (payment.received, payment.created, invoice.paid).

        Removes the invoice from tracking since it's paid.

        Expected payload:
        - invoice_id: str
        """
        payload = event.payload or {}

        invoice_id = payload.get("invoice_id")

        if not invoice_id:
            logger.warning("Ignoring payment event: missing invoice_id")
            return

        if invoice_id not in self._tracked_invoices:
            logger.debug(f"Payment received for untracked invoice {invoice_id}, ignoring")
            return

        # Remove from tracking (paid invoices don't need to be tracked)
        del self._tracked_invoices[invoice_id]

        logger.info(f"Invoice {invoice_id} marked as PAID and removed from tracking")

    def handle_time_tick(self, event: Event) -> None:
        """
        Handle time.tick event.

        Checks all PENDING invoices against the current time and emits
        invoice.overdue events for those that are overdue.

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

        # Log tracking stats for observability
        stats = self.get_tracked_invoices_count()
        logger.debug(f"Time tick at {current_time_str}, tracking stats: {stats}")

        # Collect invoices to process and remove (avoid mutating dict during iteration)
        invoices_to_remove = []

        # Use list() to safely iterate over a copy
        for invoice_id, invoice_data in list(self._tracked_invoices.items()):
            status = invoice_data["status"]

            # Only process PENDING invoices
            if status != self.STATUS_PENDING:
                continue

            due_date = self._to_naive_utc(invoice_data["due_date"])

            # Check if overdue: current_time > due_date
            if current_time > due_date:
                self._emit_overdue(
                    invoice_id=invoice_id,
                    customer_id=invoice_data["customer_id"],
                    due_date=due_date,
                    current_time=current_time,
                    correlation_id=event.correlation_id,
                )
                # Mark for removal after emitting
                invoices_to_remove.append(invoice_id)

        # Remove overdue invoices from tracking to prevent memory growth
        for invoice_id in invoices_to_remove:
            del self._tracked_invoices[invoice_id]
            logger.debug(f"Removed overdue invoice {invoice_id} from tracking")

    def _emit_overdue(
        self,
        invoice_id: str,
        customer_id: str,
        due_date: datetime,
        current_time: datetime,
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Emit invoice.overdue event.

        The caller is responsible for removing the invoice from tracking
        after this method returns.
        """
        # Calculate overdue days
        delta = current_time - due_date
        overdue_days = max(1, int(delta.total_seconds() // 86400) + 1)

        # Build payload
        overdue_payload = {
            "invoice_id": invoice_id,
            "customer_id": customer_id,
            "due_date": due_date.isoformat(),
            "overdue_days": overdue_days,
            "timestamp": current_time.isoformat(),
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

    def get_tracked_invoices_count(self) -> Dict[str, int]:
        """
        Get count of tracked invoices.

        Returns a dictionary with:
        - total: Total number of invoices being tracked
        - pending: Number still pending (all tracked are pending)
        """
        total = len(self._tracked_invoices)
        return {
            "total": total,
            "pending": total,  # All tracked invoices are PENDING
        }
