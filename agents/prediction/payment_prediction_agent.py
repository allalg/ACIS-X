import logging
import time
from typing import List, Any, Dict

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class PaymentPredictionAgent(BaseAgent):
    """
    Payment Prediction Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Predicts payment risk for all open invoices based on customer
    credit metrics and payment history.

    Produces:
    - acis.predictions (PaymentRiskPredicted)
    """

    TOPIC_METRICS = "acis.metrics"
    TOPIC_PREDICTIONS = "acis.predictions"

    def __init__(
        self,
        kafka_client: Any,
        query_agent: Any,
    ):
        super().__init__(
            agent_name="PaymentPredictionAgent",
            agent_version="1.0.0",
            group_id="payment-prediction-group",
            subscribed_topics=[self.TOPIC_METRICS],
            capabilities=[
                "payment_risk_prediction",
                "credit_analysis",
            ],
            kafka_client=kafka_client,
            agent_type="PaymentPredictionAgent",
        )
        self._query_agent = query_agent
        self._last_prediction_time: Dict[str, float] = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_METRICS]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and predict payment risk for all open invoices."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data from event payload
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        logger.debug(f"Prediction triggered by metrics update for customer {customer_id}")

        # Step 2: Get all pending invoices for this customer
        invoices = self._query_agent.get_invoices_by_customer(customer_id)

        if not invoices:
            logger.warning(f"No pending invoices found for customer {customer_id}")
            return

        # Step 3: Get customer data for credit_limit
        customer_data = self._query_agent.get_customer(customer_id)
        credit_limit = customer_data.get("credit_limit", 1) if customer_data else 1
        if credit_limit <= 0:
            credit_limit = 1

        # Extract metrics once (shared across all invoices)
        current_outstanding = data.get("total_outstanding", 0)
        credit_rating = data.get("rating", "B")
        avg_delay = data.get("avg_delay")
        on_time_ratio = data.get("on_time_ratio", 0.5)

        # Step 4: Predict risk for each pending invoice
        now = time.time()
        for invoice in invoices:
            invoice_id = invoice.get("invoice_id")

            # Safety check: skip if invoice_id is missing
            if not invoice_id:
                logger.warning("Skipping invoice with missing invoice_id")
                continue

            invoice_amount = invoice.get("amount", 0)

            # Deduplication: skip if prediction was recent for this invoice
            last_time = self._last_prediction_time.get(invoice_id, 0)
            if now - last_time < 5:
                logger.debug(f"Skipping duplicate prediction for invoice {invoice_id}")
                continue

            self._last_prediction_time[invoice_id] = now

            # Compute features
            if credit_limit > 0:
                utilization = current_outstanding / credit_limit
                invoice_ratio = invoice_amount / credit_limit
            else:
                utilization = 0
                invoice_ratio = 0
            # Cap delay_score to prevent extreme spikes
            delay_score = min((avg_delay / 30), 1) if avg_delay is not None else 0

            # Compute risk_score
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

            # Compute confidence
            confidence = 0.7

            if avg_delay is None:
                confidence -= 0.3

            # Clamp confidence between 0 and 1
            confidence = max(0, min(1, confidence))

            # Assign risk_category
            if risk_score > 0.7:
                risk_category = "high"
            elif risk_score > 0.4:
                risk_category = "medium"
            else:
                risk_category = "low"

            # Create output event payload
            prediction_payload = {
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "risk_score": round(risk_score, 4),
                "confidence": round(confidence, 2),
                "risk_category": risk_category,
            }

            # Publish event using BaseAgent publish method
            self.publish_event(
                topic=self.TOPIC_PREDICTIONS,
                event_type="PaymentRiskPredicted",
                entity_id=customer_id,
                payload=prediction_payload,
                correlation_id=event.correlation_id,
            )

            # Log published event
            logger.info(
                f"Published PaymentRiskPredicted event for invoice {invoice_id}: "
                f"risk_score={risk_score:.4f}, risk_category={risk_category}"
            )
