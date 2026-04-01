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
    - ExternalDataEnriched

    Produces:
    - customer.profile.updated

    Responsibility:
    Aggregate signals into FINAL customer-level business decision.
    """

    TOPIC_METRICS = "acis.metrics"
    TOPIC_RISK = "acis.risk"
    TOPIC_EXTERNAL = "acis.external"
    TOPIC_PROFILE = "acis.customers"

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="CustomerProfileAgent",
            agent_version="2.0.0",
            group_id="customer-profile-group",
            subscribed_topics=[
                self.TOPIC_METRICS,
                self.TOPIC_RISK,
                self.TOPIC_EXTERNAL
            ],
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
        return [
            self.TOPIC_METRICS,
            self.TOPIC_RISK,
            self.TOPIC_EXTERNAL
        ]

    def process_event(self, event: Event) -> None:
        if event.event_type == "customer.metrics.updated":
            self._handle_metrics(event)
        elif event.event_type == "RiskScoreUpdated":
            self._handle_risk(event)
        elif event.event_type == "ExternalDataEnriched":
            self._handle_external(event)
        elif event.event_type == "LitigationRiskUpdated":
            self._handle_scraped(event)

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
                "external_risk": 0.0,
                "external_scraped_risk": 0.0,
                "litigation_flag": False,
                "news_flag": False,
                "reputation_flag": False,
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

    def _handle_external(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        external_risk = float(data.get("external_risk", 0.0))
        if external_risk > 0:
            state["external_risk"] = external_risk

        self._emit_profile(customer_id, event)

    def _handle_scraped(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        state["external_scraped_risk"] = float(data.get("external_scraped_risk", 0.0))
        state["litigation_flag"] = bool(data.get("litigation_flag", False))
        state["news_flag"] = bool(data.get("news_flag", False))
        state["reputation_flag"] = bool(data.get("reputation_flag", False))

        self._emit_profile(customer_id, event)

    # ----------------------------
    # CORE LOGIC
    # ----------------------------

    def _compute_customer_risk(self, state: Dict[str, Any]) -> float:
        risks = state.get("risks", [])

        # Behavior adjustment
        delay = float(state.get("avg_delay", 0.0))
        on_time = float(state.get("on_time_ratio", 0.5))
        try:
            external = float(state.get("external_risk", 0.0))
        except Exception:
            external = 0.0
        try:
            scraped = float(state.get("external_scraped_risk", 0.0))
        except Exception:
            scraped = 0.0

        litigation_flag = state.get("litigation_flag", False)
        news_flag = state.get("news_flag", False)
        reputation_flag = state.get("reputation_flag", False)

        if not risks and external == 0 and scraped == 0:
            return 0.0

        # Conservative approach: take max risk
        base_risk = max(risks) if risks else 0.0

        delay_factor = min(1.0, delay / 30.0)

        # Flag penalty calculation
        flag_penalty = 0.0

        if litigation_flag:
            flag_penalty += 0.15

        if news_flag:
            flag_penalty += 0.1

        if reputation_flag:
            flag_penalty += 0.05

        logger.debug(
            f"[RiskComponents] base={base_risk:.2f}, "
            f"delay_factor={delay_factor:.2f}, "
            f"on_time={on_time:.2f}, "
            f"external={external:.2f}, "
            f"scraped={scraped:.2f}"
        )

        logger.debug(
            f"[Flags] litigation={litigation_flag}, "
            f"news={news_flag}, reputation={reputation_flag}"
        )

        adjusted_risk = (
            0.45 * base_risk +
            0.2 * delay_factor +
            0.1 * (1 - on_time) +
            0.15 * external +
            0.1 * scraped +
            flag_penalty
        )

        if external > 0.85:
            adjusted_risk = max(adjusted_risk, 0.9)

        if litigation_flag and state.get("external_scraped_risk", 0) > 0.6:
            adjusted_risk = max(adjusted_risk, 0.9)

        return max(0.0, min(1.0, adjusted_risk))

    def _emit_profile(self, customer_id: str, event: Event):
        state = self._state.get(customer_id)
        if not state:
            return

        logger.debug(
            f"[CustomerProfile] external_risk={state.get('external_risk', 0.0):.2f}"
        )

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

        # Override based on litigation flag
        if state.get("litigation_flag", False):
            status = "restricted"
            credit_limit = min(credit_limit, 20000)

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
