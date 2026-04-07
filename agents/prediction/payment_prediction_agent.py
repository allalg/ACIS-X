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
        self.external_cache = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_METRICS]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)
        elif event.event_type == "ExternalDataEnriched":
            self.handle_external_event(event)

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
        on_time_ratio = data.get("on_time_ratio", 0.0)

        # Check external data availability
        if customer_id not in self.external_cache:
            logger.debug(f"No external data yet for {customer_id}, using defaults")

        # Get external data if available
        external_data = self.external_cache.get(customer_id, {})
        financial_score = external_data.get("financial_score", 0.6)
        external_risk = external_data.get("external_risk", 0.5)
        litigation_flag = external_data.get("litigation_flag", False)

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

            # Normalize features to [0, 1]
            utilization = min(utilization, 1)
            invoice_ratio = min(invoice_ratio, 1)

            # Cap delay_score to prevent extreme spikes
            delay_score = min((avg_delay / 30), 1) if avg_delay is not None else 0

            logger.debug(
                f"Features for {invoice_id}: "
                f"delay={delay_score:.2f}, util={utilization:.2f}, "
                f"invoice_ratio={invoice_ratio:.2f}, behavior={(1 - on_time_ratio):.2f}"
            )

            # Compute features for risk scoring
            features = [
                delay_score,
                utilization,
                invoice_ratio,
                (1 - on_time_ratio),
                external_risk,
                (1 - financial_score)
            ]

            weights = [0.25, 0.20, 0.15, 0.10, 0.15, 0.15]

            risk_score = sum(f * w for f, w in zip(features, weights))

            # Restore credit rating impact
            if credit_rating == "C":
                risk_score += 0.2
            elif credit_rating == "B":
                risk_score += 0.1

            # Apply litigation impact
            if litigation_flag:
                risk_score += 0.15

            # Clamp risk_score between 0 and 1
            risk_score = max(0, min(1, risk_score))

            logger.info(f"Computed risk_score: {risk_score} for invoice {invoice_id}")

            # Improve confidence calculation
            confidence = 0.8

            missing_fields = 0

            if avg_delay is None:
                missing_fields += 1

            if current_outstanding == 0:
                missing_fields += 1

            confidence -= 0.1 * missing_fields
            confidence = max(0, min(1, confidence))

            # Assign risk_category
            if risk_score > 0.7:
                risk_category = "high"
            elif risk_score > 0.4:
                risk_category = "medium"
            else:
                risk_category = "low"

            # Build explanation: reasons for the prediction
            reasons = []

            if delay_score > 0.5:
                reasons.append("high payment delay")

            if utilization > 0.7:
                reasons.append("high credit utilization")

            if invoice_ratio > 0.5:
                reasons.append("large invoice relative to credit limit")

            if on_time_ratio < 0.5:
                reasons.append("low on-time payment ratio")

            if credit_rating == "C":
                reasons.append("poor credit rating")

            if external_risk > 0.6:
                reasons.append("high external risk")

            if financial_score < 0.5:
                reasons.append("weak financial health")

            if litigation_flag:
                reasons.append("litigation risk detected")

            # Create output event payload
            prediction_payload = {
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "risk_score": round(risk_score, 4),
                "confidence": round(confidence, 2),
                "risk_category": risk_category,
                "reasons": reasons
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

    def handle_external_event(self, event: Event) -> None:
        """Handle ExternalDataEnriched event and cache external data."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        self.external_cache[customer_id] = data
