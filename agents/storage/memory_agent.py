import logging
import threading
from datetime import datetime
from typing import List, Any, Dict, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class MemoryAgent(BaseAgent):
    """
    Memory Agent for ACIS-X.

    Maintains in-memory customer state derived from invoice, payment, risk, and metrics events.
    Provides fast access to aggregated customer metrics without database lookups.

    Subscribes to:
    - acis.invoices (invoice.created)
    - acis.payments (payment.received)
    - acis.risk (risk.scored)
    - acis.metrics (customer.metrics.updated)

    Publishes to:
    - acis.memory (customer.state.updated)
    """

    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_RISK = "acis.risk"
    TOPIC_METRICS = "acis.metrics"
    TOPIC_MEMORY = "acis.memory"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="MemoryAgent",
            agent_version="1.0.0",
            group_id="memory-agent-group",
            subscribed_topics=[
                self.TOPIC_INVOICES,
                self.TOPIC_PAYMENTS,
                self.TOPIC_RISK,
                self.TOPIC_METRICS,
            ],
            capabilities=[
                "state_management",
                "customer_memory",
            ],
            kafka_client=kafka_client,
            agent_type="MemoryAgent",
        )

        # In-memory customer state
        self.customer_state: Dict[str, Dict[str, Any]] = {}
        self._state_lock = threading.Lock()

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [
            self.TOPIC_INVOICES,
            self.TOPIC_PAYMENTS,
            self.TOPIC_RISK,
            self.TOPIC_METRICS,
        ]

    def process_event(self, event: Event) -> None:
        """Process incoming events and update customer state."""
        event_type = event.event_type

        if event_type == "invoice.created":
            self._handle_invoice_created(event)
        elif event_type == "payment.received":
            self._handle_payment_received(event)
        elif event_type in ["PaymentRiskPredicted", "risk.scored", "RiskScoreUpdated"]:
            self._handle_risk_scored(event)
        elif event_type == "customer.metrics.updated":
            self._handle_metrics_updated(event)

    def _get_or_create_customer_state(self, customer_id: str) -> Dict[str, Any]:
        """Get existing customer state or create new one."""
        if customer_id not in self.customer_state:
            self.customer_state[customer_id] = {
                "total_outstanding": 0.0,
                "avg_delay": 0.0,
                "on_time_ratio": 0.5,
                "risk_score": 0.0,
                "last_updated": datetime.utcnow().isoformat(),
            }
            logger.info(f"Created new customer state for customer: {customer_id}")
        return self.customer_state[customer_id]

    def _handle_invoice_created(self, event: Event) -> None:
        """Handle invoice.created event - increase total_outstanding."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        amount = data.get("amount", 0.0)

        if not customer_id:
            logger.warning("invoice.created event missing customer_id, skipping")
            return

        try:
            amount = float(amount) if amount else 0.0
        except (ValueError, TypeError):
            amount = 0.0

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)
            state["total_outstanding"] += amount
            state["last_updated"] = datetime.utcnow().isoformat()

            logger.info(
                f"Updated customer {customer_id} state: total_outstanding={state['total_outstanding']:.2f}"
            )

            self._publish_state_update(customer_id, state, event.correlation_id)

    def _handle_payment_received(self, event: Event) -> None:
        """Handle payment.received event - decrease total_outstanding."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        amount = data.get("amount", 0.0)

        if not customer_id:
            logger.warning("payment.received event missing customer_id, skipping")
            return

        try:
            amount = float(amount) if amount else 0.0
        except (ValueError, TypeError):
            amount = 0.0

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)
            state["total_outstanding"] = max(0.0, state["total_outstanding"] - amount)
            state["last_updated"] = datetime.utcnow().isoformat()

            logger.info(
                f"Updated customer {customer_id} state: total_outstanding={state['total_outstanding']:.2f}"
            )

            self._publish_state_update(customer_id, state, event.correlation_id)

    def _handle_risk_scored(self, event: Event) -> None:
        """Handle risk.scored event - update risk_score."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        risk_score = data.get("risk_score", 0.0)

        if not customer_id:
            logger.warning("risk.scored event missing customer_id, skipping")
            return

        try:
            risk_score = float(risk_score) if risk_score else 0.0
        except (ValueError, TypeError):
            risk_score = 0.0

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)
            state["risk_score"] = risk_score
            state["last_updated"] = datetime.utcnow().isoformat()

            logger.info(
                f"Updated customer {customer_id} state: risk_score={state['risk_score']:.4f}"
            )

            self._publish_state_update(customer_id, state, event.correlation_id)

    def _handle_metrics_updated(self, event: Event) -> None:
        """Handle customer.metrics.updated event - update customer metrics."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Skipping metrics update due to missing customer_id")
            return

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)

            state["avg_delay"] = data.get("avg_delay", state.get("avg_delay", 0.0))
            state["on_time_ratio"] = data.get("on_time_ratio", 0.5)
            state["total_outstanding"] = data.get("total_outstanding", state.get("total_outstanding", 0.0))
            state["last_updated"] = datetime.utcnow().isoformat()

        logger.info(f"Updated memory state from metrics for customer {customer_id}")

    def _publish_state_update(
        self,
        customer_id: str,
        state: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        """Publish customer state update event."""
        payload = {
            "customer_id": customer_id,
            "total_outstanding": round(state["total_outstanding"], 2),
            "risk_score": round(state["risk_score"], 4),
        }

        self.publish_event(
            topic=self.TOPIC_MEMORY,
            event_type="customer.state.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=correlation_id,
        )

        logger.debug(f"Published customer.state.updated for {customer_id}")

    def get_customer_state(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get current state for a customer (for internal use)."""
        with self._state_lock:
            return self.customer_state.get(customer_id)
