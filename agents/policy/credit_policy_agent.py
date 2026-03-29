import logging
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class CreditPolicyAgent(BaseAgent):
    """
    Credit Policy Agent for ACIS-X.

    Subscribes to RiskScoreUpdated events and triggers policy actions.

    Produces:
    - PolicyActionTriggered events
    """

    TOPIC_RISK = "acis.risk"
    TOPIC_POLICY = "acis.policy"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="CreditPolicyAgent",
            agent_version="1.0.0",
            group_id="credit-policy-group",
            subscribed_topics=[self.TOPIC_RISK],
            capabilities=[
                "credit_policy",
                "policy_enforcement",
            ],
            kafka_client=kafka_client,
            agent_type="CreditPolicyAgent",
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_RISK]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "RiskScoreUpdated":
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle RiskScoreUpdated event and trigger policy action."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        if not customer_id and not invoice_id:
            logger.warning("Skipping event due to missing identifiers")
            return
        risk_score = float(data.get("risk_score", 0) or 0)
        risk_score = max(0, min(1, risk_score))
        risk_level = data.get("risk_level", "low")

        # Step 2: Determine policy action
        if risk_score > 0.75:
            action = "credit_hold"
            priority = "high"
        elif risk_score > 0.4:
            action = "manual_review"
            priority = "medium"
        else:
            action = "no_action"
            priority = "low"

        logger.info(
            f"[PolicyAgent] invoice={invoice_id}, risk_score={risk_score:.2f}, "
            f"action={action}, priority={priority}"
        )

        # Step 3: Create PolicyActionTriggered payload
        policy_payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "risk_score": round(risk_score, 4),
            "risk_level": risk_level,
            "action": action,
            "priority": priority,
        }

        # Step 4: Publish event
        self.publish_event(
            topic=self.TOPIC_POLICY,
            event_type="PolicyActionTriggered",
            entity_id=customer_id or invoice_id,
            payload=policy_payload,
            correlation_id=event.correlation_id,
        )

        # Step 5: Log published event
        logger.info(
            f"Published PolicyActionTriggered event for invoice {invoice_id}: "
            f"action={action}, priority={priority}"
        )
