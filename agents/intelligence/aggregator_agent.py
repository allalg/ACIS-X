"""
Aggregator Agent for ACIS-X.

Combines financial risk (from ExternalDataAgent) and litigation risk
(from ExternalScrapingAgent) into a unified customer risk profile.

Subscribes to:
- acis.metrics (ExternalDataEnriched, LitigationRiskUpdated)

Produces:
- acis.risk (CustomerRiskProfileUpdated)
"""

import logging
from typing import List, Any, Dict, Optional
from datetime import datetime

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class AggregatorAgent(BaseAgent):
    """
    Aggregator Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (ExternalDataEnriched, LitigationRiskUpdated)

    Produces:
    - acis.risk (CustomerRiskProfileUpdated)

    Responsibility:
    Combine financial and litigation risk signals into a unified risk profile.
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.risk"

    # Risk aggregation weights
    FINANCIAL_WEIGHT = 0.6
    LITIGATION_WEIGHT = 0.4

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="AggregatorAgent",
            agent_version="1.0.0",
            group_id="aggregator-agent-group",
            subscribed_topics=[self.TOPIC_INPUT],
            capabilities=[
                "risk_aggregation",
                "financial_litigation_fusion",
                "unified_risk_profile",
            ],
            kafka_client=kafka_client,
            agent_type="AggregatorAgent",
        )
        self._cache: Dict[str, Dict[str, Any]] = {}

        logger.info("[AggregatorAgent] Initialized - aggregating financial + litigation risk")

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "ExternalDataEnriched":
            self._handle_financial_event(event)
        elif event.event_type == "LitigationRiskUpdated":
            self._handle_litigation_event(event)

    # ─────────────────────────────────────────────────────────────────────────
    # EVENT HANDLERS
    # ─────────────────────────────────────────────────────────────────────────

    def _handle_financial_event(self, event: Event) -> None:
        """Handle ExternalDataEnriched event - update financial data in cache."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("[AggregatorAgent] ExternalDataEnriched missing customer_id, skipping")
            return

        company_name = data.get("company_name") or customer_id
        financial_risk = data.get("external_risk", 0.0)
        source = data.get("source", "unknown")

        # Initialize cache entry if needed
        if customer_id not in self._cache:
            self._cache[customer_id] = {
                "financial": None,
                "litigation": None,
                "timestamp": datetime.utcnow(),
            }

        # Update financial data
        self._cache[customer_id]["financial"] = {
            "risk": financial_risk,
            "source": source,
            "company_name": company_name,
            "payload": data,
            "updated_at": datetime.utcnow(),
        }
        self._cache[customer_id]["timestamp"] = datetime.utcnow()

        logger.info(
            f"[AggregatorAgent] Updated financial data: customer={customer_id}, "
            f"risk={financial_risk:.4f}, source={source}"
        )

        # Try to aggregate
        self._try_aggregate(customer_id, event.correlation_id)

    def _handle_litigation_event(self, event: Event) -> None:
        """Handle LitigationRiskUpdated event - update litigation data in cache."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("[AggregatorAgent] LitigationRiskUpdated missing customer_id, skipping")
            return

        company_name = data.get("company_name") or customer_id
        litigation_risk = data.get("litigation_risk", 0.0)
        source = data.get("source", "unknown")

        # Initialize cache entry if needed
        if customer_id not in self._cache:
            self._cache[customer_id] = {
                "financial": None,
                "litigation": None,
                "timestamp": datetime.utcnow(),
            }

        # Update litigation data
        self._cache[customer_id]["litigation"] = {
            "risk": litigation_risk,
            "source": source,
            "company_name": company_name,
            "payload": data,
            "updated_at": datetime.utcnow(),
        }
        self._cache[customer_id]["timestamp"] = datetime.utcnow()

        logger.info(
            f"[AggregatorAgent] Updated litigation data: customer={customer_id}, "
            f"risk={litigation_risk:.4f}, source={source}"
        )

        # Try to aggregate
        self._try_aggregate(customer_id, event.correlation_id)

    # ─────────────────────────────────────────────────────────────────────────
    # AGGREGATION LOGIC
    # ─────────────────────────────────────────────────────────────────────────

    def _try_aggregate(self, customer_id: str, correlation_id: Optional[str] = None) -> None:
        """
        Attempt to aggregate risk if both financial and litigation data are available.
        Only publishes when BOTH signals exist.
        """
        cache_entry = self._cache.get(customer_id)
        if not cache_entry:
            return

        financial_data = cache_entry.get("financial")
        litigation_data = cache_entry.get("litigation")

        # Only aggregate when BOTH are available
        if not financial_data or not litigation_data:
            missing = []
            if not financial_data:
                missing.append("financial")
            if not litigation_data:
                missing.append("litigation")
            logger.debug(
                f"[AggregatorAgent] Cannot aggregate for {customer_id} - missing: {', '.join(missing)}"
            )
            return

        logger.info(f"[AggregatorAgent] Aggregating risk for customer={customer_id}")

        # Extract risk values
        financial_risk = financial_data.get("risk", 0.0)
        litigation_risk = litigation_data.get("risk", 0.0)

        # Compute combined risk
        combined_risk = (self.FINANCIAL_WEIGHT * financial_risk) + (self.LITIGATION_WEIGHT * litigation_risk)
        combined_risk = max(0.0, min(1.0, combined_risk))  # Clamp to [0, 1]

        # Determine severity
        if combined_risk >= 0.7:
            severity = "high"
        elif combined_risk >= 0.4:
            severity = "medium"
        else:
            severity = "low"

        # Get company name (prefer financial source)
        company_name = financial_data.get("company_name") or litigation_data.get("company_name") or customer_id

        # Compute confidence (average of available confidences)
        fin_confidence = financial_data.get("payload", {}).get("confidence", 0.8)
        lit_confidence = litigation_data.get("payload", {}).get("confidence", 0.7)
        confidence = (fin_confidence + lit_confidence) / 2

        # Build payload
        payload = {
            "customer_id": customer_id,
            "company_name": company_name,

            "financial_risk": round(financial_risk, 4),
            "litigation_risk": round(litigation_risk, 4),
            "combined_risk": round(combined_risk, 4),
            "severity": severity,

            "financial_source": financial_data.get("source", "unknown"),
            "litigation_source": litigation_data.get("source", "unknown"),

            "confidence": round(confidence, 4),
            "generated_at": datetime.utcnow().isoformat(),
        }

        # Publish aggregated risk event
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="CustomerRiskProfileUpdated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=correlation_id,
        )

        logger.info(
            f"[AggregatorAgent] Published risk profile: customer={customer_id}, "
            f"combined_risk={combined_risk:.4f}, severity={severity}, "
            f"financial={financial_risk:.4f}, litigation={litigation_risk:.4f}"
        )
