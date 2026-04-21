"""
Customer Profile Agent for ACIS-X - Context/Profile Layer (NOT Decision Authority).

Responsibility: Aggregate customer signals into a rich profile/context object.
This is CONTEXT ONLY - NOT final risk authority - NOT decision maker.

Consumes signals and builds customer profile for use by decision-making agents.
RiskScoringAgent has FINAL authority for risk. CollectionsAgent has FINAL authority for actions.
"""

import logging
import time
from typing import List, Any, Dict

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class CustomerProfileAgent(BaseAgent):
    """
    Customer Profile Agent for ACIS-X - Pure Context/Profile Layer.

    CRITICAL RULE: This agent builds CONTEXT, not decisions.

    Consumes: behavioral metrics, invoice-level risks, external data, litigation signals
    Produces: customer.profile.updated (context object for other agents' consumption)

    WHAT THIS AGENT DOES:
    - Aggregates invoice-level risks into customer insights
    - Tracks behavioral patterns (payment history, delays)
    - Stores external financial signals
    - Maintains risk trends and volatility

    WHAT THIS AGENT DOES NOT DO:
    - Make final risk decisions (RiskScoringAgent does)
    - Make collection decisions (CollectionsAgent does)
    - Publish final business decisions

    Output: Rich profile for RiskScoringAgent to refine invoice risks, CollectionsAgent to make decisions.
    """

    TOPIC_METRICS = "acis.metrics"
    TOPIC_RISK = "acis.risk"
    TOPIC_PROFILE = "acis.customers"

    def __init__(self, kafka_client: Any, query_agent: Any = None):
        super().__init__(
            agent_name="CustomerProfileAgent",
            agent_version="2.2.0",
            group_id="customer-profile-group",
            subscribed_topics=[
                self.TOPIC_METRICS,
                self.TOPIC_RISK,
            ],
            capabilities=[
                "customer_profile_aggregation",  # Renamed from "credit_decisioning"
                "context_enrichment",
            ],
            kafka_client=kafka_client,
            agent_type="CustomerProfileAgent",
        )

        # Optional QueryAgent for DB name resolution
        self._query_agent = query_agent

        # In-memory profile state (NOT source of truth)
        self._state: Dict[str, Dict[str, Any]] = {}

        # Memory cleanup with TTL
        self.MAX_CUSTOMERS = 10000  # Maximum customers to track
        self.TTL_SECONDS = 24 * 3600  # 24 hours
        self._last_cleanup = time.time()

    def set_query_agent(self, query_agent: Any) -> None:
        """Set QueryAgent reference for DB name resolution."""
        self._query_agent = query_agent
        logger.info("[CustomerProfileAgent] QueryAgent reference set")

    def subscribe(self) -> List[str]:
        return [
            self.TOPIC_METRICS,
            self.TOPIC_RISK,
        ]

    def process_event(self, event: Event) -> None:
        if event.event_type == "customer.metrics.updated":
            self._handle_metrics(event)
        elif event.event_type == "risk.scored":  # FIX: standardized name
            self._handle_risk(event)
        elif event.event_type == "ExternalDataEnriched":
            self._handle_external(event)
        elif event.event_type == "external.litigation.updated":  # FIX: standardized name
            self._handle_scraped(event)
        elif event.event_type == "risk.profile.updated":  # FIX: standardized name
            self._handle_aggregated_risk(event)

    # ----------------------------
    # STATE MANAGEMENT
    # ----------------------------

    def _get_state(self, customer_id: str) -> Dict[str, Any]:
        if customer_id not in self._state:
            self._state[customer_id] = {
                "risks": [],  # list of invoice-level risks
                "invoice_details": [],  # CRITICAL FIX: Initialize structured invoice data
                "total_outstanding": 0.0,
                "avg_delay": 0.0,
                "on_time_ratio": 0.5,
                "external_risk": 0.0,
                "external_scraped_risk": 0.0,
                "litigation_flag": False,
                "aggregated_risk": None,
                "financial_risk": 0.0,
                "litigation_risk": 0.0,
                "severity": None,
                "credit_limit": 100000.0,  # FIX 1: Add credit_limit to state
                "company_name": None,  # FIX 7: Forward customer name through pipeline
                "previous_risk_score": None,  # FIX 5: Track for event spam check
                # FIX 6: Track time for TTL-based cleanup
                "_last_updated_time": time.time(),
            }
        else:
            # FIX 6: Update last accessed time on every access
            self._state[customer_id]["_last_updated_time"] = time.time()

        # FIX 6: Periodic cleanup check (only every 1000 accesses = low overhead)
        if len(self._state) % 1000 == 0 and time.time() - self._last_cleanup > 60:
            self._cleanup_expired_customers()

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

        # FIX 2: Update credit_limit from metrics (dynamic, not static)
        if "credit_limit" in data:
            state["credit_limit"] = float(data.get("credit_limit") or 100000.0)

        # FIX 7: Capture company_name from metrics to forward to profile
        if "company_name" in data:
            state["company_name"] = data.get("company_name")

        self._emit_profile(customer_id, event)

    def _handle_risk(self, event: Event):
        """
        FIX 2: Handle risk.scored events and populate invoice_details.

        When RiskScoringAgent publishes risk.scored with invoice metadata (amount, days_overdue, timestamp),
        this handler extracts and stores that data for sophisticated risk aggregation.
        """
        import time

        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        risk_score = float(data.get("risk_score") or 0.0)

        # Store multiple risks (DO NOT overwrite)
        state["risks"].append(risk_score)

        # Optional: keep last N risks only
        if len(state["risks"]) > 20:
            state["risks"] = state["risks"][-20:]

        # FIX 2: CRITICAL - Extract invoice metadata and populate invoice_details
        # This enables sophisticated aggregation using invoice_details array
        if "amount" in data or "days_overdue" in data:
            invoice_detail = {
                "score": risk_score,
                "amount": float(data.get("amount") or 0.0),
                "days_overdue": float(data.get("days_overdue") or 0.0),
                "timestamp": float(data.get("timestamp") or time.time()),
            }
            state["invoice_details"].append(invoice_detail)

            # Limit to last 50 invoices for memory efficiency
            if len(state["invoice_details"]) > 50:
                state["invoice_details"] = state["invoice_details"][-50:]

            logger.debug(
                f"[CustomerProfile] Added invoice_detail for customer={customer_id}: "
                f"amount={invoice_detail['amount']:.2f}, days_overdue={invoice_detail['days_overdue']:.0f}, score={risk_score:.4f}"
            )

        self._emit_profile(customer_id, event)

    def _handle_external(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        external_risk = float(data.get("external_risk") or 0.0)
        if external_risk > 0:
            state["external_risk"] = external_risk

        self._emit_profile(customer_id, event)

    def _handle_scraped(self, event: Event):
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        state["external_scraped_risk"] = float(data.get("litigation_risk") or 0.0)
        state["litigation_flag"] = bool(data.get("litigation_flag", False))

        self._emit_profile(customer_id, event)

    def _handle_aggregated_risk(self, event: Event):
        """Handle CustomerRiskProfileUpdated - PRIMARY signal from AggregatorAgent."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        state = self._get_state(customer_id)

        # Store aggregated risk as primary signal
        state["aggregated_risk"] = float(data.get("combined_risk") or 0.0)
        state["financial_risk"] = float(data.get("financial_risk") or 0.0)
        state["litigation_risk"] = float(data.get("litigation_risk") or 0.0)
        state["severity"] = data.get("severity")

        # Also update legacy fields for backward compatibility
        state["external_risk"] = state["financial_risk"]
        state["external_scraped_risk"] = state["litigation_risk"]
        state["litigation_flag"] = state["litigation_risk"] > 0

        logger.info(
            f"[CustomerProfileAgent] Received aggregated risk: customer={customer_id}, "
            f"combined={state['aggregated_risk']:.4f}, severity={state['severity']}"
        )

        self._emit_profile(customer_id, event)

    # ----------------------------
    # CORE LOGIC
    # ----------------------------

    def _cleanup_expired_customers(self) -> None:
        """
        FIX 6: Cleanup expired customer entries from in-memory cache.

        Removes customers not accessed in the last TTL_SECONDS (24 hours).
        Also limits total customers to MAX_CUSTOMERS - removes least recently accessed.

        This prevents unbounded memory growth while preserving active customer data.
        """
        current_time = time.time()
        expired_customers = []

        # Find expired entries (not accessed in >24 hours)
        for customer_id, state in self._state.items():
            last_updated = state.get("_last_updated_time", current_time)
            age_seconds = current_time - last_updated

            if age_seconds > self.TTL_SECONDS:
                expired_customers.append(customer_id)

        # Remove expired customers
        for customer_id in expired_customers:
            del self._state[customer_id]

        # If still over limit, remove least recently accessed
        if len(self._state) > self.MAX_CUSTOMERS:
            # Sort by last_updated_time and remove oldest 10%
            sorted_customers = sorted(
                self._state.items(),
                key=lambda x: x[1].get("_last_updated_time", 0),
            )
            remove_count = len(self._state) - self.MAX_CUSTOMERS
            for customer_id, _ in sorted_customers[:remove_count]:
                del self._state[customer_id]

        if expired_customers or len(self._state) > self.MAX_CUSTOMERS:
            logger.info(
                f"[CustomerProfile] Cleanup: removed {len(expired_customers)} expired "
                f"customers, state size now: {len(self._state)}"
            )

        self._last_cleanup = time.time()

    def _compute_customer_risk(self, state: Dict[str, Any]) -> float:
        """
        Compute customer risk using invoice-level details with weighted aggregation.

        NEW: Uses invoice_details array (amount, days_overdue, timestamp) for sophisticated analysis.
        FALLBACK: If invoice_details unavailable, uses legacy risks array.

        FIX 3: Blends aggregated risk (70%) with computed risk (30%) for robustness.

        Weighting factors:
        1. Recency: Recent invoices weighted higher (30-day window)
        2. Overdue days: Older overdue invoices are riskier (normalized to 60 days)
        3. Exposure: Larger amounts increase concentration risk

        Final model: 50% max_risk + 30% weighted_avg + 20% concentration
        Then blend with behavioral signals: delay, on_time_ratio, external, litigation
        """
        import time

        # FIX 3: BLEND aggregated risk with computed risk (not pure override)
        # Aggregated signals (70%) + invoice computation (30%)
        aggregated = state.get("aggregated_risk")

        # ========== INVOICE-LEVEL AGGREGATION (NEW) ==========
        invoice_details = state.get("invoice_details", [])
        base_risk = None

        if invoice_details and len(invoice_details) > 0:
            try:
                base_risk = self._aggregate_invoice_risks(invoice_details, state)
            except Exception as e:
                logger.warning(f"[InvoiceAggregation] Error: {e}, falling back to legacy risks")
                base_risk = None

        # ========== FALLBACK: Legacy risks array ==========
        if base_risk is None:
            base_risk = self._compute_risk_legacy(state)
        else:
            # ========== BLEND: Invoice metrics + behavioral signals ==========
            base_risk = self._blend_invoice_and_behavioral_risk(base_risk, state)

        # FIX 3: Blend with aggregated risk if available
        if aggregated is not None:
            blended_risk = (0.7 * aggregated) + (0.3 * base_risk)
            logger.debug(
                f"[RiskBlending] aggregated={aggregated:.3f} + computed={base_risk:.3f} "
                f"-> blended={blended_risk:.3f}"
            )
            return max(0.0, min(1.0, blended_risk))

        return base_risk

    def _aggregate_invoice_risks(self, invoice_details: List[Dict], state: Dict[str, Any]) -> float:
        """
        Aggregate invoice-level risks using weighted model.

        Each invoice weighted by: recency (40%) + overdue_days (30%) + exposure (30%)
        Then combine: max (50%) + weighted_avg (30%) + concentration (20%)

        FIX 1: Uses credit_limit from state (not hardcoded)
        FIX 4: Uses exponential decay for recency (smoother than linear)
        """
        import time
        import math

        current_time = time.time()
        credit_limit = float(state.get("credit_limit", 100000.0))  # FIX 1: Get from state

        scores = []
        weighted_scores = []
        total_weight = 0.0

        for invoice in invoice_details:
            score = float(invoice.get("score", 0.0))
            amount = float(invoice.get("amount", 0.0))
            days_overdue = float(invoice.get("days_overdue", 0.0))
            timestamp = float(invoice.get("timestamp", current_time))

            scores.append(score)

            # ===== FACTOR 1: Recency (40%) - FIX 4: exponential decay =====
            # FIX 4: Use exp(-days/30) instead of linear for smoother decay
            # This gives more weight to recent invoices without hard cutoff
            time_delta_days = (current_time - timestamp) / (24 * 3600)
            recency_factor = math.exp(-time_delta_days / 30.0)  # Exponential decay
            recency_factor = max(0.0, min(1.0, recency_factor))

            # ===== FACTOR 2: Overdue Days (30%) =====
            # Overdue > 60 days = max penalty, < 0 = no penalty (0.1 buffer)
            if days_overdue > 0:
                overdue_factor = min(1.0, days_overdue / 60.0)
            else:
                overdue_factor = 0.1  # Buffer for paid/recent invoices

            # ===== FACTOR 3: Exposure (30%) - FIX 1: Use credit_limit from state =====
            exposure_factor = min(1.0, amount / credit_limit) if credit_limit > 0 else 0.0

            # ===== COMBINED WEIGHT =====
            weight = (0.4 * recency_factor) + (0.3 * overdue_factor) + (0.3 * exposure_factor)
            total_weight += weight
            weighted_scores.append(score * weight)

        # ===== COMPUTE AGGREGATED METRICS =====
        max_risk = max(scores) if scores else 0.0
        avg_risk = sum(scores) / len(scores) if scores else 0.0
        concentration = sum(1 for s in scores if s > 0.7) / len(scores) if scores else 0.0
        weighted_avg = sum(weighted_scores) / total_weight if total_weight > 0 else avg_risk

        # ===== FINAL BASE RISK =====
        # 50% worst case + 30% temporal weighted average + 20% concentration
        base_risk = (0.5 * max_risk) + (0.3 * weighted_avg) + (0.2 * concentration)

        logger.debug(
            f"[InvoiceAggregation] invoices={len(scores)} credit_limit={credit_limit:.0f} "
            f"max={max_risk:.3f} weighted_avg={weighted_avg:.3f} "
            f"concentration={concentration:.3f} -> base_risk={base_risk:.3f}"
        )

        return base_risk

    def _compute_risk_legacy(self, state: Dict[str, Any]) -> float:
        """Fallback: Compute risk using legacy risks array."""
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

        # FIX 6: Handle cold start for new customers
        is_new_customer = len(risks) == 0
        if is_new_customer:
            if external == 0 and scraped == 0:
                return 0.0
            cold_risk = (0.6 * external) + (0.4 * scraped)
            return max(0.0, min(1.0, cold_risk))

        # Conservative approach: take max risk
        base_risk = max(risks) if risks else 0.0
        delay_factor = min(1.0, delay / 30.0)
        flag_penalty = 0.15 if litigation_flag else 0.0

        logger.debug(
            f"[LegacyRisk] base={base_risk:.2f} delay={delay_factor:.2f} "
            f"on_time={on_time:.2f} external={external:.2f} scraped={scraped:.2f}"
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

    def _blend_invoice_and_behavioral_risk(self, base_risk: float, state: Dict[str, Any]) -> float:
        """Blend invoice metrics with behavioral signals."""
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
        delay_factor = min(1.0, delay / 30.0)
        flag_penalty = 0.15 if litigation_flag else 0.0

        logger.debug(
            f"[BlendedRisk] base={base_risk:.2f} + "
            f"delay={delay_factor:.2f} on_time={on_time:.2f} "
            f"external={external:.2f} scraped={scraped:.2f}"
        )

        # Combine invoice metrics (primary) with behavioral signals
        adjusted_risk = (
            0.5 * base_risk +           # Invoice metrics (primary)
            0.2 * delay_factor +        # Payment delay behavior
            0.1 * (1 - on_time) +       # On-time payment ratio
            0.1 * external +            # External financial risk
            0.05 * scraped +            # Scraped external data
            flag_penalty                # Litigation flag
        )

        # Hard overrides
        if external > 0.85:
            adjusted_risk = max(adjusted_risk, 0.9)

        if litigation_flag and state.get("external_scraped_risk", 0) > 0.6:
            adjusted_risk = max(adjusted_risk, 0.9)

        return max(0.0, min(1.0, adjusted_risk))

    def _emit_profile(self, customer_id: str, event: Event):
        """
        Emit profile update event.

        FIX 2: ONLY computes risk and metrics (no business decisions).
        FIX 5: ONLY emits if risk changed significantly (>0.02 threshold).
        FIX 8: NEVER emits customer_name=None — looks up from DB via QueryAgent
                and omits the field entirely if no name is available yet.
        """
        state = self._state.get(customer_id)
        if not state:
            return

        logger.debug(
            f"[CustomerProfile] external_risk={state.get('external_risk', 0.0):.2f}"
        )

        # Compute risk
        risk_score = self._compute_customer_risk(state)
        previous_risk = state.get("previous_risk_score")

        # FIX 5: Event spam reduction - only emit if significant change
        if previous_risk is not None:
            risk_delta = abs(risk_score - previous_risk)
            if risk_delta < 0.02:  # Less than 2% change
                logger.debug(
                    f"[CustomerProfile] Risk delta={risk_delta:.4f} < threshold, skipping emit"
                )
                return

        # Update previous risk for next comparison
        state["previous_risk_score"] = risk_score

        outstanding = float(state.get("total_outstanding", 0.0))
        delay = float(state.get("avg_delay", 0.0))

        # FIX 8: Resolve company name — NEVER emit customer_name=None.
        # Priority: in-state name → DB lookup → omit field entirely.
        customer_name = state.get("company_name")
        if not customer_name and self._query_agent:
            try:
                cust = self._query_agent.get_customer(customer_id)
                if cust and cust.get("name"):
                    customer_name = cust["name"]
                    state["company_name"] = customer_name  # Cache for next emit
                    logger.debug(
                        f"[CustomerProfile] Resolved name from DB: {customer_id} → {customer_name}"
                    )
            except Exception as e:
                logger.debug(f"[CustomerProfile] DB name lookup failed (non-fatal): {e}")

        # Build payload — omit customer_name entirely if still None so DBAgent
        # _handle_customer_profile won't set name=NULL in its UPDATE.
        payload: dict = {
            "customer_id": customer_id,
            "risk_score": round(risk_score, 4),
            "total_outstanding": round(outstanding, 2),
            "avg_delay": round(delay, 2),
            "on_time_ratio": round(state.get("on_time_ratio", 0.5), 4),
            "severity": state.get("severity"),
            "external_risk": round(state.get("external_risk", 0.0), 4),
        }
        if customer_name:
            payload["customer_name"] = customer_name

        self.publish_event(
            topic=self.TOPIC_PROFILE,
            event_type="customer.profile.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[CustomerProfile] customer={customer_id} "
            f"risk={risk_score:.4f} outstanding={outstanding:.2f} "
            f"name={'<known>' if customer_name else '<unknown, omitted>'}"
        )
