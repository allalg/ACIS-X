"""
Aggregator Agent for ACIS-X.

Combines financial risk (from ExternalDataAgent) and litigation risk
(from ExternalScrapingAgent) into a unified customer risk profile.

Subscribes to:
- acis.metrics (ExternalDataEnriched, external.litigation.updated)

Produces:
- acis.risk (risk.profile.updated)
"""

import logging
from typing import List, Any, Dict, Optional
from datetime import datetime, timedelta
import time

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class AggregatorAgent(BaseAgent):
    """
    Aggregator Agent for ACIS-X.

    Subscribes to:
    - acis.metrics (ExternalDataEnriched, external.litigation.updated)

    Produces:
    - acis.risk (risk.profile.updated)

    Responsibility:
    Combine financial and litigation risk signals into a unified risk profile.
    """

    TOPIC_INPUT = "acis.metrics"
    TOPIC_OUTPUT = "acis.risk"

    # Risk aggregation weights
    FINANCIAL_WEIGHT = 0.6
    LITIGATION_WEIGHT = 0.4
    # FIX 7: Cache TTL and cleanup settings
    CACHE_TTL_SECONDS = 24 * 3600  # 24 hours
    MAX_CACHE_SIZE = 50000  # Maximum customers to cache

    def __init__(self, kafka_client: Any):
        super().__init__(
            agent_name="AggregatorAgent",
            agent_version="1.0.1",  # Bumped: FIX 7 added cache cleanup
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
        # FIX 7: Track last cleanup time
        self._last_cleanup = time.time()
        # FIX 9: Track published risk values to prevent duplicate events
        self._last_published: Dict[str, Dict[str, float]] = {}
        # FIX 12: QueryAgent reference for company name resolution
        self._query_agent = None

        logger.info("[AggregatorAgent] Initialized - aggregating financial + litigation risk")

    def set_query_agent(self, query_agent: Any) -> None:
        """Set reference to QueryAgent for company name resolution."""
        self._query_agent = query_agent
        logger.info("QueryAgent reference set for company name resolution")

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INPUT]

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "ExternalDataEnriched":
            self._handle_financial_event(event)
        elif event.event_type == "external.litigation.updated":  # FIX: standardized name
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
            f"risk={financial_risk if financial_risk is not None else 'None'}, source={source}"
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

    def _cleanup_expired_cache(self) -> None:
        """
        FIX 7: Cleanup expired cache entries.

        Removes entries not updated in the last CACHE_TTL_SECONDS (24 hours).
        Also enforces MAX_CACHE_SIZE - removes oldest entries if exceeded.

        This prevents unbounded memory growth from accumulating customer data.
        """
        current_time = datetime.utcnow()
        expired_customers = []

        # Find expired entries
        for customer_id, entry in self._cache.items():
            entry_time = entry.get("timestamp", current_time)
            age = (current_time - entry_time).total_seconds()

            if age > self.CACHE_TTL_SECONDS:
                expired_customers.append(customer_id)

        # Remove expired
        for customer_id in expired_customers:
            del self._cache[customer_id]

        # Enforce size limit - remove oldest 10% if exceeded
        if len(self._cache) > self.MAX_CACHE_SIZE:
            sorted_customers = sorted(
                self._cache.items(),
                key=lambda x: x[1].get("timestamp", datetime.utcnow())
            )
            remove_count = len(self._cache) - self.MAX_CACHE_SIZE
            for customer_id, _ in sorted_customers[:remove_count]:
                del self._cache[customer_id]

        if expired_customers or len(self._cache) > self.MAX_CACHE_SIZE:
            logger.info(
                f"[AggregatorAgent] Cache cleanup: removed {len(expired_customers)} expired, "
                f"cache size now: {len(self._cache)}"
            )

        self._last_cleanup = time.time()

    def _try_aggregate(self, customer_id: str, correlation_id: Optional[str] = None) -> None:
        """
        Attempt to aggregate risk from available signals.

        Publishes risk.profile.updated as soon as EITHER financial OR litigation
        data is available, using 0.0 for the missing signal.
        Only skips entirely when NEITHER signal exists.
        """
        # FIX 7: Periodic cleanup (every 1000 aggregations or every 60 seconds)
        if len(self._cache) % 1000 == 0 and time.time() - self._last_cleanup > 60:
            self._cleanup_expired_cache()

        cache_entry = self._cache.get(customer_id)
        if not cache_entry:
            return

        financial_data = cache_entry.get("financial")
        litigation_data = cache_entry.get("litigation")

        # Skip only if NEITHER signal exists yet
        if not financial_data and not litigation_data:
            logger.debug(
                f"[AggregatorAgent] No signals yet for {customer_id}, skipping"
            )
            return

        if not financial_data:
            logger.debug(f"[AggregatorAgent] No financial data for {customer_id}, using 0.0")
        if not litigation_data:
            logger.debug(f"[AggregatorAgent] No litigation data for {customer_id}, using 0.0")

        logger.info(f"[AggregatorAgent] Aggregating risk for customer={customer_id}")

        # Extract risk values — financial_risk can be None if absent/private company
        financial_risk = financial_data.get("risk") if financial_data else None
        if financial_risk is not None: financial_risk = float(financial_risk)
        litigation_risk = litigation_data.get("risk") if litigation_data else None
        litigation_risk = float(litigation_risk) if litigation_risk is not None else 0.0

        # FIX 9: DEDUPLICATION - Only publish if risk values actually changed
        # Compare against last published values to prevent duplicate events
        last_published = self._last_published.get(customer_id, {})
        last_fin_risk = last_published.get("financial_risk", None)
        last_lit_risk = last_published.get("litigation_risk", None)

        # If both financial and litigation risks haven't changed, skip publishing
        if last_fin_risk == financial_risk and last_lit_risk == litigation_risk:
            fin_str = f"{financial_risk:.4f}" if financial_risk is not None else "None"
            lit_str = f"{litigation_risk:.4f}" if litigation_risk is not None else "None"
            logger.debug(
                f"[AggregatorAgent] Skipping publish for {customer_id}: "
                f"financial={fin_str}, litigation={lit_str} (unchanged)"
            )
            return

        # Compute combined risk
        if financial_risk is not None:
            combined_risk = (self.FINANCIAL_WEIGHT * financial_risk) + (self.LITIGATION_WEIGHT * litigation_risk)
        else:
            # Calculate risk based on external litigation agent only if financial data is missing
            combined_risk = litigation_risk
        combined_risk = max(0.0, min(1.0, combined_risk))  # Clamp to [0, 1]

        # Determine severity
        if combined_risk >= 0.7:
            severity = "high"
        elif combined_risk >= 0.4:
            severity = "medium"
        else:
            severity = "low"

        # Get company name — safely handle if either signal is absent
        company_name = (
            (financial_data.get("company_name") if financial_data else None)
            or (litigation_data.get("company_name") if litigation_data else None)
        )

        # If still missing or a customer_id fallback, try DB
        import re as _re
        _is_id_fallback = lambda n: not n or bool(_re.match(r'^cust_\d+$', n))
        if _is_id_fallback(company_name):
            try:
                if self._query_agent:
                    customer = self._query_agent.get_customer(customer_id)
                    if customer and customer.get("name"):
                        company_name = customer.get("name")
                        logger.debug(f"[AggregatorAgent] Resolved company_name from DB for {customer_id}: {company_name}")
            except Exception as e:
                logger.debug(f"[AggregatorAgent] Failed to fetch company_name from DB: {e}")

        # If still unresolved, leave as None — DBAgent will handle the fallback
        # Do NOT use customer_id as a company name (it's meaningless in the output)
        if _is_id_fallback(company_name):
            company_name = None

        # Compute confidence (average of available confidences)
        # Only include financial confidence if we actually have a financial risk score
        if financial_data and financial_risk is not None:
            fin_confidence = financial_data.get("payload", {}).get("confidence", 0.8)
        else:
            fin_confidence = 0.0
            
        lit_confidence = litigation_data.get("payload", {}).get("confidence", 0.7) if litigation_data else 0.0
        
        valid_confidences = [c for c in [fin_confidence, lit_confidence] if c > 0]
        confidence = sum(valid_confidences) / len(valid_confidences) if valid_confidences else 0.5

        # Build payload
        payload = {
            "customer_id": customer_id,
            "company_name": company_name,

            "financial_risk": round(financial_risk, 4) if financial_risk is not None else None,
            "litigation_risk": round(litigation_risk, 4),
            "combined_risk": round(combined_risk, 4),
            "severity": severity,

            "financial_source": financial_data.get("source", "unknown") if financial_data else "none",
            "litigation_source": litigation_data.get("source", "unknown") if litigation_data else "none",

            "confidence": round(confidence, 4),
            "generated_at": datetime.utcnow().isoformat(),
        }

        # Publish aggregated risk event (standardized naming)
        self.publish_event(
            topic=self.TOPIC_OUTPUT,
            event_type="risk.profile.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=correlation_id,
        )

        # FIX 9: Track published values to prevent duplicate events on next cycle
        self._last_published[customer_id] = {
            "financial_risk": financial_risk,
            "litigation_risk": litigation_risk,
        }

        logger.info(
            f"[AggregatorAgent] Published risk.profile.updated: customer={customer_id}, "
            f"combined_risk={combined_risk:.4f}, severity={severity}, "
            f"financial={financial_risk if financial_risk is not None else 'None'}, litigation={litigation_risk if litigation_risk is not None else 'None'}"
        )
