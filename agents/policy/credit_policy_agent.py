import logging
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class CreditPolicyAgent(BaseAgent):
    """
    Credit Policy Agent for ACIS-X.

    ⚠️ DEPRECATED: This agent duplicates CollectionsAgent logic.
    Subscribes to risk.scored events and triggers policy actions.

    NOTE: CollectionsAgent is the primary decision engine.
    This agent will be replaced by CollectionsAgent.

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
        if event.event_type == "risk.scored":  # FIX: standardized name
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle risk.scored event and trigger policy action."""
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

        # Read explanation from risk score agent
        reasons = data.get("reasons", [])

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

        # Add policy explanation
        policy_reasons = []

        if action == "credit_hold":
            policy_reasons.append("high risk threshold exceeded")

        if action == "manual_review":
            policy_reasons.append("moderate risk requires review")

        # Add decision confidence
        if risk_score > 0.75:
            decision_confidence = 0.9
        elif risk_score > 0.4:
            decision_confidence = 0.7
        else:
            decision_confidence = 0.95

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
            "prediction_reasons": reasons,
            "policy_reasons": policy_reasons,
            "decision_confidence": round(decision_confidence, 2),
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
