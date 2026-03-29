import logging
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class PaymentPredictionAgent(BaseAgent):
    """
    Payment Prediction Agent for ACIS-X.

    Subscribes to invoice.created events and predicts payment risk
    based on customer credit metrics and payment history.

    Produces:
    - PaymentRiskPredicted events
    """

    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PREDICTIONS = "acis.predictions"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="PaymentPredictionAgent",
            agent_version="1.0.0",
            group_id="payment-prediction-group",
            subscribed_topics=[self.TOPIC_INVOICES],
            capabilities=[
                "payment_risk_prediction",
                "credit_analysis",
            ],
            kafka_client=kafka_client,
            agent_type="PaymentPredictionAgent",
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INVOICES]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "invoice.created":
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle invoice.created event and predict payment risk."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data from event payload
        data = event.payload
        invoice_id = data.get("invoice_id")
        customer_id = data.get("customer_id")
        invoice_amount = data.get("amount", 0)
        credit_limit = data.get("credit_limit", 1)
        current_outstanding = data.get("current_outstanding", 0)
        credit_rating = data.get("rating", "B")
        avg_delay = data.get("avg_delay")
        on_time_ratio = data.get("on_time_ratio", 0.5)

        # Step 2: Compute features
        if credit_limit > 0:
            utilization = current_outstanding / credit_limit
            invoice_ratio = invoice_amount / credit_limit
        else:
            utilization = 0
            invoice_ratio = 0
        delay_score = (avg_delay / 30) if avg_delay is not None else 0

        # Step 3: Compute risk_score
        risk_score = (
            0.35 * delay_score +
            0.25 * utilization +
            0.20 * invoice_ratio +
            0.10 * (1 - on_time_ratio)
        )

        # Adjust for rating
        if credit_rating == "C":
            risk_score += 0.2
        elif credit_rating == "B":
            risk_score += 0.1

        # Clamp risk_score between 0 and 1
        risk_score = max(0, min(1, risk_score))

        logger.info(f"Computed risk_score: {risk_score} for invoice {invoice_id}")

        # Step 4: Compute confidence
        confidence = 0.7

        if avg_delay is None:
            confidence -= 0.3

        # Clamp confidence between 0 and 1
        confidence = max(0, min(1, confidence))

        # Step 5: Assign risk_category
        if risk_score > 0.7:
            risk_category = "high"
        elif risk_score > 0.4:
            risk_category = "medium"
        else:
            risk_category = "low"

        # Step 6: Create output event payload
        prediction_payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "risk_score": round(risk_score, 4),
            "confidence": round(confidence, 2),
            "risk_category": risk_category,
        }

        # Step 7: Publish event using BaseAgent publish method
        self.publish_event(
            topic=self.TOPIC_PREDICTIONS,
            event_type="PaymentRiskPredicted",
            entity_id=customer_id,
            payload=prediction_payload,
            correlation_id=event.correlation_id,
        )

        # Step 8: Log published event
        logger.info(
            f"Published PaymentRiskPredicted event for invoice {invoice_id}: "
            f"risk_score={risk_score:.4f}, risk_category={risk_category}"
        )
