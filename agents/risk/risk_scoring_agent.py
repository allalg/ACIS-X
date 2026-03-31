import logging
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class RiskScoringAgent(BaseAgent):
    """
    Risk Scoring Agent for ACIS-X.

    Subscribes to PaymentRiskPredicted events and refines risk scores.

    Produces:
    - RiskScoreUpdated events
    - HighRiskDetected events (when risk_level is high)
    """

    TOPIC_PREDICTIONS = "acis.predictions"
    TOPIC_RISK = "acis.risk"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="RiskScoringAgent",
            agent_version="1.0.0",
            group_id="risk-scoring-group",
            subscribed_topics=[self.TOPIC_PREDICTIONS],
            capabilities=[
                "risk_scoring",
                "risk_classification",
            ],
            kafka_client=kafka_client,
            agent_type="RiskScoringAgent",
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_PREDICTIONS]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "PaymentRiskPredicted":
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle PaymentRiskPredicted event and refine risk score."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        risk_score = float(data.get("risk_score", 0) or 0)
        confidence = float(data.get("confidence", 0.5) or 0.5)
        risk_score = max(0, min(1, risk_score))
        confidence = max(0, min(1, confidence))

        # Propagate explanation from prediction
        reasons = data.get("reasons", [])

        # Step 2: Refine risk
        adjusted_risk = risk_score

        if confidence < 0.5:
            adjusted_risk += 0.1
            reasons.append("low confidence adjustment applied")

        # Clamp adjusted_risk between 0 and 1
        adjusted_risk = max(0, min(1, adjusted_risk))

        logger.info(
            f"[RiskAgent] invoice={invoice_id}, base={risk_score:.2f}, "
            f"confidence={confidence:.2f}, adjusted={adjusted_risk:.2f}"
        )

        # Step 3: Classify risk level
        if adjusted_risk > 0.75:
            risk_level = "high"
        elif adjusted_risk > 0.4:
            risk_level = "medium"
        else:
            risk_level = "low"

        # Add severity level
        if adjusted_risk > 0.85:
            severity = "critical"
        elif adjusted_risk > 0.6:
            severity = "elevated"
        else:
            severity = "normal"

        logger.debug(f"Risk reasons: {reasons}")
        logger.debug(f"Severity: {severity}")

        # Step 4: Create RiskScoreUpdated payload
        risk_payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "risk_score": round(adjusted_risk, 4),
            "risk_level": risk_level,
            "severity": severity,
            "confidence": confidence,
            "reasons": reasons,
        }

        # Step 5: Publish RiskScoreUpdated event
        self.publish_event(
            topic=self.TOPIC_RISK,
            event_type="RiskScoreUpdated",
            entity_id=customer_id or invoice_id,
            payload=risk_payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"Published RiskScoreUpdated event for invoice {invoice_id}: "
            f"risk_score={adjusted_risk:.4f}, risk_level={risk_level}"
        )

        # Step 6: Publish HighRiskDetected if risk_level is high
        if risk_level == "high":
            high_risk_payload = {
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "risk_score": adjusted_risk,
            }

            self.publish_event(
                topic=self.TOPIC_RISK,
                event_type="HighRiskDetected",
                entity_id=customer_id or invoice_id,
                payload=high_risk_payload,
                correlation_id=event.correlation_id,
            )

            logger.info(
                f"Published HighRiskDetected event for invoice {invoice_id}: "
                f"risk_score={adjusted_risk:.4f}"
            )
