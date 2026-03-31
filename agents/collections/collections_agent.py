"""
Collections Agent for ACIS-X.

Converts risk intelligence into actionable credit control decisions.
Supports both invoice-level and customer-level collection actions.

Subscribes to: acis.risk
Publishes to: acis.collections

Event Types:
- Handles: RiskScoreUpdated, HighRiskDetected
- Emits: collection.reminder, collection.escalation, collection.action
"""

import logging
from typing import List, Any, Dict, Set, Tuple

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class CollectionsAgent(BaseAgent):
    """
    Collections Agent for ACIS-X.

    Consumes:
    - RiskScoreUpdated (invoice-level risk)
    - HighRiskDetected (customer-level high risk)

    Produces:
    - collection.reminder (medium risk, outstanding balance)
    - collection.escalation (invoice or customer escalation)
    - collection.action (credit hold)

    Responsibility:
    Transform risk signals into collection workflows and credit control actions.
    """

    TOPIC_RISK = "acis.risk"
    TOPIC_COLLECTIONS = "acis.collections"

    def __init__(
        self,
        kafka_client: Any,
        query_agent: Any,
    ):
        super().__init__(
            agent_name="CollectionsAgent",
            agent_version="1.0.0",
            group_id="collections-group",
            subscribed_topics=[self.TOPIC_RISK],
            capabilities=[
                "collection_management",
                "credit_control",
                "invoice_escalation",
                "customer_escalation",
            ],
            kafka_client=kafka_client,
            agent_type="CollectionsAgent",
        )
        self._query_agent = query_agent

        # Duplicate prevention: track (customer_id, invoice_id) or (customer_id, "customer")
        self._processed_actions: Set[Tuple[str, str]] = set()

    def subscribe(self) -> List[str]:
        """Return topics to subscribe to."""
        return [self.TOPIC_RISK]

    def process_event(self, event: Event) -> None:
        """Process incoming risk events."""
        if event.event_type == "RiskScoreUpdated":
            self._handle_risk_score_updated(event)
        elif event.event_type == "HighRiskDetected":
            self._handle_high_risk_detected(event)

    # ----------------------------
    # INVOICE-LEVEL LOGIC
    # ----------------------------

    def _handle_risk_score_updated(self, event: Event) -> None:
        """Handle RiskScoreUpdated event (invoice-level)."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        risk_level = data.get("risk_level", "low")

        if not customer_id or not invoice_id:
            logger.warning(
                f"[Collections] RiskScoreUpdated missing customer_id or invoice_id"
            )
            return

        logger.info(
            f"[Collections] Processing RiskScoreUpdated: "
            f"customer={customer_id}, invoice={invoice_id}, risk={risk_level}"
        )

        # Check for duplicates
        action_key = (customer_id, invoice_id)
        if action_key in self._processed_actions:
            logger.debug(
                f"[Collections] Duplicate action ignored: {action_key}"
            )
            return

        # Step 1: Query customer metrics
        invoices = self._query_agent.get_invoices_by_customer(customer_id)
        # Use remaining_amount for accurate outstanding (accounts for partial payments)
        total_outstanding = sum(
            float(inv.get("remaining_amount", inv.get("amount", 0)))
            for inv in invoices
        )

        logger.info(
            f"[Collections] Customer metrics: outstanding={total_outstanding:.2f}, "
            f"pending_invoices={len(invoices)}"
        )

        # Step 2: Decision rules
        if risk_level == "low":
            logger.debug(
                f"[Collections] customer={customer_id}, invoice={invoice_id}: "
                f"Low risk - no action"
            )
            return

        if risk_level == "medium" and total_outstanding > 0:
            # Emit collection.reminder
            self._processed_actions.add(action_key)
            self._emit_reminder(
                customer_id=customer_id,
                invoice_id=invoice_id,
                event=event,
            )
            return

        if risk_level == "high":
            # Emit collection.escalation
            self._processed_actions.add(action_key)
            self._emit_invoice_escalation(
                customer_id=customer_id,
                invoice_id=invoice_id,
                event=event,
            )
            return

    def _emit_reminder(
        self,
        customer_id: str,
        invoice_id: str,
        event: Event,
    ) -> None:
        """Emit collection.reminder event."""
        payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "priority": "medium",
            "reason": "Moderate risk with outstanding balance",
            "action_type": "send_reminder",
        }

        self.publish_event(
            topic=self.TOPIC_COLLECTIONS,
            event_type="collection.reminder",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[Collections] customer={customer_id}, invoice={invoice_id} → reminder"
        )

    def _emit_invoice_escalation(
        self,
        customer_id: str,
        invoice_id: str,
        event: Event,
    ) -> None:
        """Emit collection.escalation event for invoice."""
        payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "priority": "high",
            "reason": "High invoice risk",
            "action_type": "escalate_invoice",
            "escalation_level": 1,
        }

        self.publish_event(
            topic=self.TOPIC_COLLECTIONS,
            event_type="collection.escalation",
            entity_id=invoice_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[Collections] customer={customer_id}, invoice={invoice_id} → escalation"
        )

    # ----------------------------
    # CUSTOMER-LEVEL LOGIC
    # ----------------------------

    def _handle_high_risk_detected(self, event: Event) -> None:
        """Handle HighRiskDetected event (customer-level)."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning(
                f"[Collections] HighRiskDetected missing customer_id"
            )
            return

        logger.info(
            f"[Collections] Processing HighRiskDetected: customer={customer_id}"
        )

        # Check for duplicates
        action_key = (customer_id, "customer")
        if action_key in self._processed_actions:
            logger.debug(
                f"[Collections] Duplicate customer action ignored: {action_key}"
            )
            return

        # Step 1: Query customer metrics using actual overdue invoices
        overdue_invoices = self._query_agent.get_overdue_invoices(customer_id)
        overdue_count = len(overdue_invoices)

        if not overdue_invoices:
            logger.debug(
                f"[Collections] customer={customer_id}: No overdue invoices, skipping"
            )
            return

        # Calculate average delay (simplified: use count as proxy)
        avg_delay_days = 30 if overdue_count > 0 else 0

        logger.info(
            f"[Collections] Customer metrics: overdue_count={overdue_count}, "
            f"avg_delay_days={avg_delay_days}"
        )

        # Step 2: Decision rules
        if overdue_count >= 3 or avg_delay_days > 30:
            # Emit collection.action (hold_credit)
            self._processed_actions.add(action_key)
            self._emit_credit_hold(
                customer_id=customer_id,
                reason="Severe delinquency",
                event=event,
            )
            return

        if overdue_count > 0:
            # Emit collection.escalation
            self._processed_actions.add(action_key)
            self._emit_customer_escalation(
                customer_id=customer_id,
                reason="Customer has overdue invoices",
                event=event,
            )
            return

    def _emit_credit_hold(
        self,
        customer_id: str,
        reason: str,
        event: Event,
    ) -> None:
        """Emit collection.action event with hold_credit action."""
        payload = {
            "customer_id": customer_id,
            "action": "hold_credit",
            "reason": reason,
            "action_type": "apply_hold",
            "priority": "critical",
        }

        self.publish_event(
            topic=self.TOPIC_COLLECTIONS,
            event_type="collection.action",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[Collections] customer={customer_id} → hold_credit (severe delinquency)"
        )

    def _emit_customer_escalation(
        self,
        customer_id: str,
        reason: str,
        event: Event,
    ) -> None:
        """Emit collection.escalation event for customer."""
        payload = {
            "customer_id": customer_id,
            "action": "escalate_customer",
            "reason": reason,
            "action_type": "escalate_customer",
            "escalation_level": 1,
            "priority": "high",
        }

        self.publish_event(
            topic=self.TOPIC_COLLECTIONS,
            event_type="collection.escalation",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[Collections] customer={customer_id} → customer escalation"
        )
