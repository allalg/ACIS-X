"""
Customer Profile Agent for ACIS-X.

Consumes risk signals and metrics to build final customer business profile.
Emits customer.profile.updated events for credit decisioning.
"""

import logging
from typing import List, Any, Dict

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class CustomerProfileAgent(BaseAgent):
    """
    Customer Profile Agent for ACIS-X.

    Consumes:
    - customer.metrics.updated
    - RiskScoreUpdated

    Produces:
    - customer.profile.updated

    Responsibility:
    Aggregate signals into FINAL customer-level business decision.
    """

    TOPIC_METRICS = "acis.metrics"
    TOPIC_RISK = "acis.risk"
    TOPIC_PROFILE = "acis.customers"

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="CustomerProfileAgent",
            agent_version="2.0.0",
            group_id="customer-profile-group",
            subscribed_topics=[self.TOPIC_METRICS, self.TOPIC_RISK],
            capabilities=[
                "customer_risk_aggregation",
                "credit_decisioning",
            ],
            kafka_client=kafka_client,
            agent_type="CustomerProfileAgent",
        )

        # In-memory aggregation state (NOT source of truth)
        self._state: Dict[str, Dict[str, Any]] = {}

    def subscribe(self) -> List[str]:
        return [self.TOPIC_METRICS, self.TOPIC_RISK]

    def process_event(self, event: Event) -> None:
        if event.event_type == "customer.metrics.updated":
            self._handle_metrics(event)
        elif event.event_type == "RiskScoreUpdated":
            self._handle_risk(event)

    # ----------------------------
    # STATE MANAGEMENT
    # ----------------------------

    def _get_state(self, customer_id: str) -> Dict[str, Any]:
        if customer_id not in self._state:
            self._state[customer_id] = {
                "risks": [],  # list of invoice-level risks
                "total_outstanding": 0.0,
                "avg_delay": 0.0,
                "on_time_ratio": 0.5,
            }
        return self._state[customer_id]

    # ----------------------------
    # EVENT HANDLERS
    # ----------------------------

    def _handle_metrics(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        state["total_outstanding"] = data.get("total_outstanding", 0.0)
        state["avg_delay"] = data.get("avg_delay", 0.0)
        state["on_time_ratio"] = data.get("on_time_ratio", 0.5)

        self._emit_profile(customer_id, event)

    def _handle_risk(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        risk_score = float(data.get("risk_score", 0.0))

        # Store multiple risks (DO NOT overwrite)
        state["risks"].append(risk_score)

        # Optional: keep last N risks only
        if len(state["risks"]) > 20:
            state["risks"] = state["risks"][-20:]

        self._emit_profile(customer_id, event)

    # ----------------------------
    # CORE LOGIC
    # ----------------------------

    def _compute_customer_risk(self, state: Dict[str, Any]) -> float:
        risks = state.get("risks", [])

        if not risks:
            return 0.0

        # Conservative approach: take max risk
        base_risk = max(risks)

        # Behavior adjustment
        delay = float(state.get("avg_delay", 0.0))
        on_time = float(state.get("on_time_ratio", 0.5))

        delay_factor = min(1.0, delay / 30.0)

        adjusted_risk = (
            0.6 * base_risk +
            0.25 * delay_factor +
            0.15 * (1 - on_time)
        )

        return max(0.0, min(1.0, adjusted_risk))

    def _emit_profile(self, customer_id: str, event: Event):
        state = self._state.get(customer_id)
        if not state or not state.get("risks"):
            return

        risk_score = self._compute_customer_risk(state)
        outstanding = float(state.get("total_outstanding", 0.0))
        delay = float(state.get("avg_delay", 0.0))

        # ----------------------------
        # BUSINESS DECISION LOGIC
        # ----------------------------

        if risk_score > 0.8:
            status = "blocked"
            credit_limit = 0
        elif risk_score > 0.6:
            status = "restricted"
            credit_limit = max(0, 50000 - outstanding)
        else:
            status = "active"
            credit_limit = 100000

        # Hard override based on delay
        if delay > 45:
            status = "high_risk"
            credit_limit = 0

        payload = {
            "customer_id": customer_id,
            "risk_score": round(risk_score, 4),
            "credit_limit": round(credit_limit, 2),
            "status": status,
        }

        self.publish_event(
            topic=self.TOPIC_PROFILE,
            event_type="customer.profile.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[CustomerProfile] customer={customer_id} "
            f"risk={risk_score:.2f} status={status} limit={credit_limit}"
        )
