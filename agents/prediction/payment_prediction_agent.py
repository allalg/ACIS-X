import logging
import time
from typing import List, Any, Dict
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import shap

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class PaymentPredictionAgent(BaseAgent):
    """
    Payment Prediction Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (customer.metrics.updated)

    Predicts payment risk for all open invoices based on customer
    credit metrics and payment history.

    Produces:
    - acis.predictions (payment.risk.predicted)

    Each prediction payload includes a `shap_values` dict providing
    SHAP-style per-feature attribution (closed-form for linear models:
    phi_i = w_i * x_i relative to zero baseline).
    """

    TOPIC_METRICS = "acis.metrics"
    TOPIC_PREDICTIONS = "acis.predictions"
    TOPIC_RISK = "acis.risk"

    # Feature names aligned with the weight vector
    FEATURE_NAMES = [
        "payment_delay",       # normalised avg delay (0=none, 1=30d+)
        "credit_utilization",  # outstanding / credit_limit
        "invoice_size_ratio",  # invoice_amount / credit_limit
        "late_payment_rate",   # 1 - on_time_ratio
        "external_risk",       # from ExternalDataAgent [0,1]
        "financial_weakness",  # 1 - financial_score [0,1]
        "litigation_risk",     # continuous litigation risk
        "credit_rating_penalty" # 0 for A, 0.5 for B, 1.0 for C
    ]
    WEIGHTS = [0.20, 0.15, 0.10, 0.15, 0.10, 0.10, 0.10, 0.10]

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="PaymentPredictionAgent",
            agent_version="1.1.1",  # bumped: use risk.profile.updated
            group_id="payment-prediction-group",
            subscribed_topics=[self.TOPIC_METRICS, self.TOPIC_RISK],
            capabilities=[
                "payment_risk_prediction",
                "credit_analysis",
                "shap_explainability",
            ],
            kafka_client=kafka_client,
            agent_type="PaymentPredictionAgent",
        )
        self._last_prediction_time: Dict[str, float] = {}
        self.external_cache = {}
        self.EXTERNAL_CACHE_TTL_SECONDS = 24 * 3600
        self.MAX_EXTERNAL_CACHE_SIZE = 5000
        self._warmup_ml_model()

    def _warmup_ml_model(self):
        """Initialize and warm up the ML model with synthetic data."""
        self.model = RandomForestClassifier(n_estimators=50, random_state=42)
        np.random.seed(42)
        
        # Generate 1000 synthetic samples to bootstrap the AI model
        X_train = np.random.rand(1000, 8)
        
        # Create sensible labels based on heuristics so the model starts with logical predictions
        weights = np.array(self.WEIGHTS)
        y_prob = np.dot(X_train, weights)
        # Add some noise and label as high risk if > 0.4
        y_train = (y_prob + np.random.normal(0, 0.05, 1000) > 0.4).astype(int)
        
        self.model.fit(X_train, y_train)
        
        # Initialize SHAP explainer
        self.explainer = shap.TreeExplainer(self.model)
        if isinstance(self.explainer.expected_value, (list, np.ndarray)):
            self.expected_value = float(self.explainer.expected_value[1])
        else:
            self.expected_value = float(self.explainer.expected_value)

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_METRICS, self.TOPIC_RISK]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "customer.metrics.updated":
            self.handle_event(event)
        elif event.event_type == "risk.profile.updated":
            self.handle_risk_profile_event(event)

    def handle_event(self, event: Event) -> None:
        """Handle customer.metrics.updated event and predict payment risk for all open invoices."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("Missing customer_id in metrics event")
            return

        logger.debug(f"Prediction triggered by metrics update for customer {customer_id}")

        # Get all pending invoices for this customer
        invoices_response = QueryClient.query("get_invoices_by_customer", {"customer_id": customer_id})
        invoices = invoices_response.get("invoices", []) if isinstance(invoices_response, dict) else (invoices_response or [])
        invoices = [inv for inv in invoices if inv.get("status") != "paid"]

        if not invoices:
            logger.warning(f"No pending invoices found for customer {customer_id}")
            return

        # Get customer data for credit_limit
        customer_data = QueryClient.query("get_customer", {"customer_id": customer_id})
        credit_limit = customer_data.get("credit_limit", 1) if customer_data else 1
        if credit_limit <= 0:
            credit_limit = 1

        # Shared metrics across all invoices
        current_outstanding = data.get("total_outstanding", 0)
        credit_rating = data.get("rating", "B")
        avg_delay = data.get("avg_delay")
        on_time_ratio = data.get("on_time_ratio", 0.0)
        now = time.time()

        # External data
        if customer_id not in self.external_cache:
            logger.debug(f"No external data yet for {customer_id}, using defaults")

        external_entry = self.external_cache.get(customer_id, {})
        if external_entry and now - external_entry.get("cached_at", 0) > self.EXTERNAL_CACHE_TTL_SECONDS:
            self.external_cache.pop(customer_id, None)
            external_entry = {}
        external_data = external_entry.get("data", external_entry)
        
        fin_risk = external_data.get("financial_risk")
        fin_risk = float(fin_risk) if fin_risk is not None else 0.4
        financial_score = 1.0 - fin_risk
        
        external_risk = external_data.get("combined_risk")
        external_risk = float(external_risk) if external_risk is not None else 0.5
        
        litigation_risk = external_data.get("litigation_risk", 0.0)
        litigation_risk = float(litigation_risk) if litigation_risk is not None else 0.0
        litigation_flag = litigation_risk > 0.1

        credit_rating_penalty = 0.0
        if credit_rating == "C":
            credit_rating_penalty = 1.0
        elif credit_rating == "B":
            credit_rating_penalty = 0.5

        for invoice in invoices:
            invoice_id = invoice.get("invoice_id")
            if not invoice_id:
                logger.warning("Skipping invoice with missing invoice_id")
                continue

            invoice_amount = invoice.get("amount", 0)

            # Deduplication: skip if prediction was recent
            last_time = self._last_prediction_time.get(invoice_id, 0)
            if now - last_time < 5:
                logger.debug(f"Skipping duplicate prediction for invoice {invoice_id}")
                continue
            self._last_prediction_time[invoice_id] = now

            # ── Compute normalised feature vector ─────────────────────────────
            utilization = min(current_outstanding / credit_limit, 1) if credit_limit > 0 else 0
            invoice_ratio = min(invoice_amount / credit_limit, 1) if credit_limit > 0 else 0
            delay_score = min((avg_delay / 30), 1) if avg_delay is not None else 0

            features = [
                delay_score,           # payment_delay
                utilization,           # credit_utilization
                invoice_ratio,         # invoice_size_ratio
                (1 - on_time_ratio),   # late_payment_rate
                external_risk,         # external_risk
                (1 - financial_score), # financial_weakness
                litigation_risk,       # litigation_risk
                credit_rating_penalty, # credit_rating_penalty
            ]

            # ── AI Model Prediction ─────────────────────────────
            X_input = np.array(features).reshape(1, -1)
            
            # Predict risk probability (class 1)
            risk_score = float(self.model.predict_proba(X_input)[0][1])

            # ── True SHAP values (TreeExplainer) ───────────────
            explanation = self.explainer(X_input)
            
            if len(explanation.values.shape) == 3:
                shap_vals = explanation.values[0, :, 1]
            else:
                shap_vals = explanation.values[0]

            shap_values_dict = {
                name: round(float(val), 4)
                for name, val in zip(self.FEATURE_NAMES, shap_vals)
            }
            top_driver = max(shap_values_dict, key=lambda k: abs(shap_values_dict[k]))

            # ── 2-Stage Policy Adjustments (Non-Negotiable Risk Events) ────────
            r_ml = risk_score
            
            rating_adjustment = 0.0
            if credit_rating == "C":
                rating_adjustment = 0.20
            elif credit_rating == "B":
                rating_adjustment = 0.10

            # Tiered sophistication for litigation severity
            litigation_adjustment = 0.0
            if litigation_risk > 0.5:
                litigation_adjustment = 0.25
            elif litigation_risk > 0.1:
                litigation_adjustment = 0.15

            r_policy = rating_adjustment + litigation_adjustment
            risk_score = max(0.0, min(1.0, r_ml + r_policy))

            logger.info(f"Computed risk_score: {risk_score} for invoice {invoice_id}")

            # ── Confidence calculation ─────────────────────────────────────────
            confidence = 0.8
            missing_fields = sum([avg_delay is None, current_outstanding == 0])
            confidence -= 0.1 * missing_fields
            confidence = max(0, min(1, confidence))

            # ── Risk category ──────────────────────────────────────────────────
            if risk_score > 0.7:
                risk_category = "high"
            elif risk_score > 0.4:
                risk_category = "medium"
            else:
                risk_category = "low"

            # ── Human-readable reasons ─────────────────────────────────────────
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

            # ── Prediction payload with SHAP explainability ────────────────────
            prediction_payload = {
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "risk_score": round(risk_score, 4),
                "confidence": round(confidence, 2),
                "risk_category": risk_category,
                "reasons": reasons,
                # SHAP explainability block
                "shap_values": shap_values_dict,
                "shap_top_driver": top_driver,
                "shap_baseline": round(self.expected_value, 4),
                "shap_sum": round(sum(shap_values_dict.values()), 4),
                # Auxiliary adjustments (outside linear Shapley decomposition)
                "shap_rating_adjustment": round(rating_adjustment, 4),
                "shap_litigation_adjustment": round(litigation_adjustment, 4),
            }

            self.publish_event(
                topic=self.TOPIC_PREDICTIONS,
                event_type="payment.risk.predicted",
                entity_id=customer_id,
                payload=prediction_payload,
                correlation_id=event.correlation_id,
            )

            logger.info(
                f"Published payment.risk.predicted for {invoice_id}: "
                f"risk={risk_score:.4f} ({risk_category}), "
                f"top_driver={top_driver} ({shap_values_dict[top_driver]:+.4f})"
            )

    def handle_risk_profile_event(self, event: Event) -> None:
        """Handle risk.profile.updated event and cache aggregated risk data."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        if len(self.external_cache) >= self.MAX_EXTERNAL_CACHE_SIZE:
            oldest_customer_id = min(
                self.external_cache,
                key=lambda cid: self.external_cache[cid].get("cached_at", 0),
            )
            self.external_cache.pop(oldest_customer_id, None)
        self.external_cache[customer_id] = {
            "data": data,
            "cached_at": time.time(),
        }
