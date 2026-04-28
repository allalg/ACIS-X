import logging
import time
from typing import List, Any

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class RiskScoringAgent(BaseAgent):
    """
    Risk Scoring Agent for ACIS-X - SOLE AUTHORITY FOR FINAL INVOICE-LEVEL RISK.

    CRITICAL RESPONSIBILITY: Compute final invoice-level risk.scored events.
    This is the ONLY agent that publishes risk.scored - it is the authoritative risk source.

    ARCHITECTURE:
    - Subscribes to: acis.predictions (payment predictions), acis.customers (customer profile context)
    - Consumes: CustomerProfileAgent context (NOT decision), customer metrics
    - Produces: risk.scored (final invoice-level risk), risk.high.detected (alerts)
    - Does NOT: Make collection decisions (CollectionsAgent does), emit final business decisions

    Risk Refinement Factors:
    1. Base prediction from PaymentPredictionAgent
    2. Customer behavior (overdue count, payment history, delays)
    3. Customer external risk context (from AggregatorAgent)
    4. Temporal trends (deterioration, volatility)
    5. Model confidence

    Uses continuous mathematical functions (smooth, ML-optimizable).
    Detects risk trends and temporal patterns for early warnings.
    """

    TOPIC_PREDICTIONS = "acis.predictions"
    TOPIC_RISK = "acis.risk"
    TOPIC_CUSTOMERS = "acis.customers"  # Subscribe to customer profile context
    TOPIC_METRICS = "acis.metrics"  # CRITICAL FIX #2: Subscribe to payment behavior metrics

    def __init__(
        self,
        kafka_client: Any = None,
    ):
        super().__init__(
            agent_name="RiskScoringAgent",
            agent_version="2.2.0",  # Bumped: CRITICAL - Added acis.metrics subscription for payment behavior
            group_id="risk-scoring-group",
            subscribed_topics=[self.TOPIC_PREDICTIONS, self.TOPIC_CUSTOMERS, self.TOPIC_METRICS],
            capabilities=[
                "risk_scoring",
                "risk_classification",
                "trend_detection",
            ],
            kafka_client=kafka_client,
            agent_type="RiskScoringAgent",
        )
        # CRITICAL FIX: Store customer-level aggregated/external risk context with TTL
        # Structure: customer_id → {"data": {...}, "updated_at": timestamp}
        # Updated by _handle_customer_risk_profile() from acis.customers topic
        # Used by _refine_risk_with_context() to influence final invoice risk
        self._customer_risk_context: dict[str, dict[str, Any]] = {}

        # TTL and memory management settings
        self.CONTEXT_TTL_SECONDS = 86400  # 24 hours
        self.MAX_CONTEXT_CUSTOMERS = 10000
        self._last_cleanup_time = time.time()
        self._cleanup_interval_seconds = 300  # Attempt cleanup every 5 minutes
        logger.info("QueryAgent reference set for context enrichment")

    def _handle_customer_risk_profile(self, event: Event) -> None:
        """
        CRITICAL FIX: Store customer-level aggregated/external risk context with TTL.

        This context is then used by _refine_risk_with_context() to influence final invoice-level risk.

        Sources of customer-level risk:
        1. AggregatorAgent: aggregated_risk, financial_risk, litigation_risk
        2. ExternalDataAgent: external_risk, market signals
        3. Risk trends: deteriorating, stable, improving

        Does NOT emit events - only updates internal context with timestamp for TTL tracking.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        # Extract and store all available customer-level risk signals
        context_data = {
            # Core aggregated/external risk
            "aggregated_risk": float(data.get("aggregated_risk") or data.get("risk_score") or 0.0),
            "financial_risk": float(data.get("financial_risk") or 0.0),
            "litigation_risk": float(data.get("litigation_risk") or 0.0),
            "external_risk": float(data.get("external_risk") or 0.0),

            # Risk characterization
            "severity": data.get("severity"),
            "trend": data.get("trend", "stable"),
        }

        # TTL-BASED CLEANUP: Store with timestamp for TTL tracking and cleanup
        self._customer_risk_context[customer_id] = {
            "data": context_data,
            "updated_at": time.time()
        }

        logger.info(
            f"[RiskScoringAgent] Stored customer context: customer={customer_id}, "
            f"aggregated={context_data['aggregated_risk']:.4f}, financial={context_data['financial_risk']:.4f}, "
            f"litigation={context_data['litigation_risk']:.4f}, external={context_data['external_risk']:.4f}, "
            f"severity={context_data['severity']}, trend={context_data['trend']}"
        )

        # Periodic cleanup: Run on time interval (every 5 minutes) instead of event count
        # This is more predictable and doesn't depend on traffic patterns
        current_time = time.time()
        if current_time - self._last_cleanup_time > self._cleanup_interval_seconds:
            self._cleanup_customer_context()
            self._last_cleanup_time = current_time

    def _cleanup_customer_context(self) -> None:
        """
        TTL-based cleanup: Remove stale customer context entries.

        - Removes entries not updated in >24 hours
        - Enforces MAX_CONTEXT_CUSTOMERS limit (removes oldest entries if exceeded)
        - Runs periodically (not per-event) to minimize overhead
        """
        current_time = time.time()

        # STEP 1: Remove expired entries (TTL > 24 hours)
        expired_customers = []
        for customer_id, entry in self._customer_risk_context.items():
            age_seconds = current_time - entry.get("updated_at", current_time)
            if age_seconds > self.CONTEXT_TTL_SECONDS:
                expired_customers.append(customer_id)

        for customer_id in expired_customers:
            del self._customer_risk_context[customer_id]
            logger.debug(f"[RiskScoringAgent] Cleaned up expired context: customer={customer_id}")

        if expired_customers:
            logger.info(f"[RiskScoringAgent] TTL cleanup: Removed {len(expired_customers)} expired entries")

        # STEP 2: Enforce size limit (remove oldest entries if > MAX_CUSTOMERS)
        if len(self._customer_risk_context) > self.MAX_CONTEXT_CUSTOMERS:
            # Sort by updated_at timestamp, remove oldest (least recently updated)
            sorted_entries = sorted(
                self._customer_risk_context.items(),
                key=lambda x: x[1].get("updated_at", 0)
            )

            num_to_remove = len(self._customer_risk_context) - self.MAX_CONTEXT_CUSTOMERS
            for customer_id, _ in sorted_entries[:num_to_remove]:
                del self._customer_risk_context[customer_id]

            logger.warning(
                f"[RiskScoringAgent] Memory limit cleanup: Removed {num_to_remove} oldest entries "
                f"(total now: {len(self._customer_risk_context)})"
            )

    def _handle_customer_metrics(self, event: Event) -> None:
        """
        Handle customer.metrics.updated event from CustomerStateAgent.

        CRITICAL FIX: Store payment behavior metrics as part of customer risk context.

        Metrics include:
        - avg_delay: Average payment delay in days
        - on_time_ratio: Fraction of on-time payments
        - total_outstanding: Total unpaid invoices
        - last_payment_date: Most recent payment date

        These signals influence final invoice-level risk scoring.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            return

        # Extract payment behavior metrics
        metrics_data = {
            "avg_delay": float(data.get("avg_delay") or 0.0),
            "on_time_ratio": float(data.get("on_time_ratio") or 0.0),
            "total_outstanding": float(data.get("total_outstanding") or 0.0),
            "last_payment_date": data.get("last_payment_date"),
        }

        # Store with timestamp for TTL tracking
        if customer_id not in self._customer_risk_context:
            self._customer_risk_context[customer_id] = {"data": {}, "updated_at": time.time()}

        # Update metrics within existing context
        self._customer_risk_context[customer_id]["data"].update(metrics_data)
        self._customer_risk_context[customer_id]["updated_at"] = time.time()

        logger.debug(
            f"[RiskScoringAgent] Updated customer metrics: customer={customer_id}, "
            f"avg_delay={metrics_data['avg_delay']:.2f}, on_time_ratio={metrics_data['on_time_ratio']:.2f}"
        )

    def _get_customer_context(self, customer_id: str) -> dict[str, Any]:
        """
        Lazy-loaded customer context with fallback.

        STEP 1: Try cached context (with TTL check)
        STEP 2: Lazy fallback to QueryAgent if cache miss or stale
        STEP 3: Return context data or empty dict

        This ensures:
        - Fast path for recent data (cache hit)
        - Graceful degradation if context is stale or missing
        - Always returns valid dict (never None)
        """
        # STEP 1: Try cached context
        entry = self._customer_risk_context.get(customer_id)
        if entry:
            context_data = entry.get("data", {})
            age_seconds = time.time() - entry.get("updated_at", 0)

            if age_seconds < self.CONTEXT_TTL_SECONDS:
                logger.debug(
                    f"[RiskScoringAgent] Context cache hit: customer={customer_id}, "
                    f"age={age_seconds:.0f}s"
                )
                return context_data
            else:
                logger.debug(
                    f"[RiskScoringAgent] Context cache stale: customer={customer_id}, "
                    f"age={age_seconds:.0f}s > TTL={self.CONTEXT_TTL_SECONDS}s"
                )

        # STEP 2: Lazy fallback to QueryAgent for enriched data
        # IMPROVEMENT: Optionally enrich context with available metrics
        if True:
            try:
                metrics = QueryClient.query("get_customer_metrics", {"customer_id": customer_id})
                if metrics:
                    logger.debug(
                        f"[RiskScoringAgent] Lazy-loaded enriched context from QueryAgent: customer={customer_id}"
                    )
                    # Extract what we can infer from metrics
                    # Infer trend from payment behavior
                    on_time_ratio = float(metrics.get("on_time_ratio", 0.0))
                    inferred_trend = "improving" if on_time_ratio > 0.7 else ("deteriorating" if on_time_ratio < 0.3 else "stable")

                    return {
                        "aggregated_risk": 0.0,  # Not available from metrics (requires AggregatorAgent)
                        "financial_risk": 0.0,   # Not available from metrics (requires ExternalDataAgent)
                        "litigation_risk": 0.0,  # Not available from metrics (requires LitigationAgent)
                        "external_risk": 0.0,    # Not available from metrics (requires ExternalDataAgent)
                        "severity": None,
                        "trend": inferred_trend,  # Inferred from on_time_ratio
                    }
            except Exception as e:
                logger.warning(f"[RiskScoringAgent] QueryAgent enrichment failed: {e}")

        # STEP 3: Return safe defaults (won't influence risk, but won't crash)
        logger.debug(f"[RiskScoringAgent] No context available: customer={customer_id}, using defaults")
        return {
            "aggregated_risk": 0.0,
            "financial_risk": 0.0,
            "litigation_risk": 0.0,
            "external_risk": 0.0,
            "severity": None,
            "trend": "stable",
        }

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_PREDICTIONS, self.TOPIC_CUSTOMERS, self.TOPIC_METRICS]  # CRITICAL FIX: Added TOPIC_METRICS

    def process_event(self, event: Event) -> None:
        """Process incoming events."""
        if event.event_type == "PaymentRiskPredicted":
            self.handle_event(event)
        elif event.event_type == "customer.profile.updated":  # BUG FIX #5: Match emitted event name from CustomerProfileAgent
            self._handle_customer_risk_profile(event)
        elif event.event_type == "customer.metrics.updated":  # CRITICAL FIX: Handle payment metrics
            self._handle_customer_metrics(event)

    def handle_event(self, event: Event) -> None:
        """Handle PaymentRiskPredicted event and refine risk score."""
        logger.info(f"Received event: {event.event_type} for entity {event.entity_id}")

        # Step 1: Extract data from prediction
        import time
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        base_risk_score = float(data.get("risk_score", 0) or 0)
        confidence = float(data.get("confidence", 0.5) or 0.5)
        base_risk_score = max(0, min(1, base_risk_score))
        confidence = max(0, min(1, confidence))

        # CRITICAL: Extract invoice details needed by MemoryAgent
        amount = float(data.get("amount", 0.0) or 0.0)
        days_overdue = float(data.get("days_overdue", 0.0) or 0.0)
        timestamp = float(data.get("timestamp", time.time()) or time.time())

        # Get prediction reasons
        reasons = data.get("reasons", [])

        # Step 2: Refine risk with customer context (if QueryAgent available)
        adjusted_risk = self._refine_risk_with_context(
            customer_id, invoice_id, base_risk_score, confidence, reasons
        )

        logger.info(
            f"[RiskAgent] invoice={invoice_id}, base={base_risk_score:.2f}, "
            f"confidence={confidence:.2f}, refined={adjusted_risk:.2f}"
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
        logger.debug(f"Risk level: {risk_level}, Severity: {severity}")

        # Step 4: Create risk.scored payload
        # CRITICAL: Include invoice details for MemoryAgent aggregation
        risk_payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "risk_score": round(adjusted_risk, 4),
            "risk_level": risk_level,
            "severity": severity,
            "confidence": confidence,
            "reasons": reasons,
            # NEW: Invoice metadata for structured aggregation
            "amount": amount,
            "days_overdue": days_overdue,
            "timestamp": timestamp,
        }

        # Step 5: Publish risk.scored event
        self.publish_event(
            topic=self.TOPIC_RISK,
            event_type="risk.scored",
            entity_id=customer_id or invoice_id,
            payload=risk_payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"Published risk.scored event for invoice {invoice_id}: "
            f"risk_score={adjusted_risk:.4f}, risk_level={risk_level}"
        )

        # Step 6: Publish risk.high.detected if risk_level is high
        if risk_level == "high":
            high_risk_payload = {
                "customer_id": customer_id,
                "invoice_id": invoice_id,
                "risk_score": adjusted_risk,
            }

            self.publish_event(
                topic=self.TOPIC_RISK,
                event_type="risk.high.detected",
                entity_id=customer_id or invoice_id,
                payload=high_risk_payload,
                correlation_id=event.correlation_id,
            )

            logger.info(
                f"Published risk.high.detected event for invoice {invoice_id}: "
                f"risk_score={adjusted_risk:.4f}"
            )

    def _refine_risk_with_context(
        self,
        customer_id: str,
        invoice_id: str,
        base_risk: float,
        confidence: float,
        reasons: List[str],
    ) -> float:
        """
        Refine base risk score using customer context from DB.

        CRITICAL FIX: Uses get_customer_metrics() for ENRICHED data with computed metrics.
        This is the correct source - NOT raw customer table, but derived+computed metrics.

        Considers enriched customer data:
        1. Overdue invoice count (CRITICAL - highest impact)
        2. Payment behavior (on-time ratio, delay patterns)
        3. Outstanding balance relative to credit limit
        4. High exposure (raw amount > 100k)
        5. Model confidence level

        STABILITY FIX: Uses weighted blending to prevent wild risk swings.
        Formula: adjusted_risk = base_risk + (0.5 * total_adjustment)
        This prevents jumps like 0.3 → 0.85 → makes system more production-safe

        ML READINESS: Feature normalization for stable, interpretable weights
        - Normalizes all inputs to [0, 1] scale for consistent weighting
        - Makes model more robust to different data ranges

        Returns: Adjusted risk score [0, 1]
        """
        # ===== NORMALIZATION CONSTANTS (ML Readiness) =====
        # These define the scale for each feature; all normalized to [0, 1]
        MAX_EXPECTED_OVERDUE = 5  # 5+ overdue invoices = max risk for this factor
        MAX_EXPECTED_DELAY_DAYS = 90  # 90+ days average = max delay penalty
        HIGH_EXPOSURE_THRESHOLD = 500000.0  # 500k+ = maximum exposure signal

        adjusted_risk = base_risk

        try:
            if False  :
                logger.debug("[RiskAgent] QueryAgent not available, using base risk")
                return adjusted_risk

            # CRITICAL FIX: Use get_customer_metrics() which returns ENRICHED metrics
            # This includes: overdue_count, total_outstanding, avg_delay, on_time_ratio
            # NOT raw DB columns, but computed derived metrics for risk scoring
            customer = QueryClient.query("get_customer_metrics", {"customer_id": customer_id}) if customer_id else None
            if not customer:
                logger.debug(f"[RiskAgent] Customer metrics not found for {customer_id}, using base risk")
                return adjusted_risk

            # IMPROVEMENT 1 FIX: CONTINUOUS FUNCTIONS (model-based, not rule-based)
            # Instead of hard-coded thresholds, use smooth mathematical functions
            # This enables: smooth transitions, ML optimization, realistic risk progression

            # ===== FACTOR 1: Overdue invoice count (NORMALIZED) =====
            # Normalize to [0, 1] using MAX_EXPECTED_OVERDUE
            # 0 invoices: 0.0 (clean)
            # 2-3 invoices: 0.4-0.6 (normalized)
            # 5+ invoices: 1.0 (max risk for this factor)
            overdue_count = customer.get("overdue_count", 0)
            normalized_overdue = min(1.0, overdue_count / MAX_EXPECTED_OVERDUE)
            # Apply sensitivity: normalized feature × max impact for this factor (0.30)
            overdue_adjustment = normalized_overdue * 0.30
            if overdue_count > 0:
                reasons.append(f"overdue normalized: count={overdue_count} → normalized={normalized_overdue:.2f} → adjustment={overdue_adjustment:.3f}")
            else:
                reasons.append(f"good: no overdue invoices (clean record bonus)")

            # ===== FACTOR 2: Payment behavior adjustment (already normalized) =====
            # on_time_ratio is already [0, 1]; no normalization needed
            # Formula: (target_ratio - actual_ratio) * sensitivity
            # Penalizes deviation from perfect on-time payment
            # on_time_ratio=0.9: (0.7 - 0.9) × 0.3 = -0.06 (reward!)
            # on_time_ratio=0.0: (0.7 - 0.0) × 0.3 = +0.21 (high penalty for unknown/new customers)
            on_time_ratio = customer.get("on_time_ratio", 0.0)
            payment_adjustment = max(-0.1, min(0.2, (0.7 - on_time_ratio) * 0.3))  # Bounded [-0.1, 0.2]
            reasons.append(f"payment behavior: on_time_ratio={on_time_ratio:.2f} → adjustment={payment_adjustment:+.3f}")

            # ===== FACTOR 3: Delay behavior adjustment (NORMALIZED) =====
            # Normalize to [0, 1] using MAX_EXPECTED_DELAY_DAYS
            # 0 days: 0.0 (clean)
            # 45 days: 0.5 (normalized)
            # 90+ days: 1.0 (max delay penalty)
            avg_delay = customer.get("avg_delay", 0)
            normalized_delay = min(1.0, avg_delay / MAX_EXPECTED_DELAY_DAYS)
            # Apply sensitivity: normalized feature × max impact for this factor (0.25)
            delay_adjustment = normalized_delay * 0.25
            reasons.append(f"delay normalized: avg_delay={avg_delay:.1f}d → normalized={normalized_delay:.2f} → adjustment={delay_adjustment:+.3f}")

            # ===== FACTOR 4: Outstanding balance adjustment (continuous) =====
            # Utilization already normalized to [0, 1]; applies quadratic scaling
            # This creates accelerating risk as utilization increases
            # 50% utilization: (0.5^2) × 0.25 = 0.0625
            # 80% utilization: (0.8^2) × 0.25 = 0.16
            # 100% utilization: (1.0^2) × 0.25 = 0.25
            total_outstanding = customer.get("total_outstanding", 0.0)
            credit_limit = customer.get("credit_limit", 1.0)
            if credit_limit > 0:
                utilization = min(1.0, total_outstanding / credit_limit)
                # Quadratic function for accelerating risk
                outstanding_adjustment = (utilization ** 2) * 0.25  # Non-linear scaling
                reasons.append(f"outstanding: utilization={utilization:.1%} → adjustment={outstanding_adjustment:+.3f}")
            else:
                outstanding_adjustment = 0
                reasons.append("outstanding: no credit limit set")

            # ===== FACTOR 5: High exposure amplifier (NORMALIZED) =====
            # Normalize to [0, 1] using HIGH_EXPOSURE_THRESHOLD
            # Uses log scaling to capture non-linear risk increase
            # 50k: 0.0 (low exposure)
            # 250k: ~0.5 (normalized)
            # 500k+: 1.0 (max exposure)
            high_exposure_adjustment = 0
            if total_outstanding > 50000:
                import math
                # Normalize to [0, 1] then apply log scaling
                exposure_normalized = min(1.0, total_outstanding / HIGH_EXPOSURE_THRESHOLD)
                # Log scaling captures exponential risk increase
                high_exposure_adjustment = exposure_normalized * 0.1 * math.log10(exposure_normalized * 10 + 1)
                high_exposure_adjustment = min(0.25, high_exposure_adjustment)
                reasons.append(f"exposure normalized: outstanding={total_outstanding:.0f} → normalized={exposure_normalized:.2f} → adjustment={high_exposure_adjustment:+.3f}")
            else:
                reasons.append(f"exposure: low exposure (outstanding={total_outstanding:.0f})")

            # FACTOR 5: Confidence adjustment (applied multiplicatively, not additive)
            # Confidence will be applied as multiplier to total_adjustment after weighting
            if confidence < 0.5:
                reasons.append("low model confidence (will be applied as ×1.20 multiplier)")
            elif confidence < 0.7:
                reasons.append("moderate model confidence (will be applied as ×1.10 multiplier)")
            else:
                reasons.append("high model confidence (no multiplier)")

            # Combine all adjustments with WEIGHTS
            # All factors now normalized to [0, 1] → weights directly control impact
            # Weights represent importance: overdue critical (0.40) → payment (0.20) → delays/outstanding (0.15 each)

            # Total adjustment: weighted sum of all normalized factors
            total_adjustment = (
                (0.40 * overdue_adjustment) +           # Overdue count is MOST critical (normalized)
                (0.20 * payment_adjustment) +           # Payment behavior is important (already [0,1])
                (0.15 * delay_adjustment) +             # Delays matter (normalized)
                (0.15 * outstanding_adjustment) +       # Outstanding balance matters (normalized)
                (0.10 * high_exposure_adjustment)       # High exposure is a signal (normalized)
                # Note: confidence is multiplicative, not additive (handled separately)
            )

            # CRITICAL FIX: Incorporate customer-level external risk context with lazy loading
            # Uses _get_customer_context() for safe retrieval with TTL checking and fallback
            external_risk_adjustment = 0
            customer_context = self._get_customer_context(customer_id)

            if customer_context:
                # Blend external risk signals
                aggregated_risk = customer_context.get("aggregated_risk", 0.0)
                financial_risk = customer_context.get("financial_risk", 0.0)
                litigation_risk = customer_context.get("litigation_risk", 0.0)
                external_risk = customer_context.get("external_risk", 0.0)

                # Only apply boost if we have actual external signals (not all zeros)
                if any([aggregated_risk, financial_risk, litigation_risk, external_risk]):
                    # Weighted combination of external signals
                    # External factors have lower weight than behavioral (they're supplementary)
                    blended_external = (
                        (0.40 * aggregated_risk) +          # Aggregated risk (40% of external signals)
                        (0.30 * financial_risk) +           # Financial risk (30%)
                        (0.20 * litigation_risk) +          # Litigation risk (20%)
                        (0.10 * external_risk)              # Market/screener external risk (10%)
                    )

                    # Apply as multiplicative boost to total_adjustment
                    # This way external risk amplifies behavioral signals, not overrides them
                    external_risk_adjustment = blended_external * 0.2  # Max 20% boost from external signals
                    total_adjustment += external_risk_adjustment

                    reasons.append(
                        f"external context: aggregated={aggregated_risk:.3f}, "
                        f"financial={financial_risk:.3f}, litigation={litigation_risk:.3f}, "
                        f"market={external_risk:.3f} → boost={external_risk_adjustment:+.3f}"
                    )
                else:
                    reasons.append("external context: loaded but no signals available")
            else:
                reasons.append("external context: not yet available (lazy fallback used defaults)")

            # Apply confidence adjustment multiplicatively (not additive)
            # This way, low confidence increases by percentage, not flat amount
            if confidence < 0.5:
                # Low confidence: increase adjustment by 20%
                total_adjustment = total_adjustment * 1.20
                reasons.append(f"low confidence multiplier: ×1.20")
            elif confidence < 0.7:
                # Moderate confidence: increase adjustment by 10%
                total_adjustment = total_adjustment * 1.10
                reasons.append(f"moderate confidence multiplier: ×1.10")
            # High confidence: no multiplier (×1.00)

            # IMPROVEMENT 3: TEMPORAL TREND DETECTION (early warning signals)
            # Detect rapid deterioration, improving trends, volatility
            temporal_adjustment = 0
            try:
                velocity_data = QueryClient.query("get_risk_velocity", {"customer_id": customer_id})
            except Exception:
                velocity_data = None
            if velocity_data:
                    velocity = velocity_data.get("velocity", 0)
                    trend = velocity_data.get("trend", "stable")
                    volatility = velocity_data.get("volatility", 0)

                    # RAPID DETERIORATION WARNING
                    if trend == "deteriorating_fast":
                        # Risk jumping up quickly - major concern
                        temporal_adjustment += 0.15
                        reasons.append(f"ALERT: rapid deterioration (velocity={velocity:+.3f})")
                    elif trend == "deteriorating_slow":
                        # Gradual increase - minor concern
                        temporal_adjustment += 0.05
                        reasons.append(f"slowly deteriorating (velocity={velocity:+.3f})")
                    elif trend == "improving":
                        # Risk decreasing - reduce pressure
                        temporal_adjustment -= 0.08
                        reasons.append(f"improving trend (velocity={velocity:+.3f})")
                    else:
                        # Stable trajectory
                        reasons.append(f"stable trajectory (velocity={velocity:+.3f})")

                    # HIGH VOLATILITY = RISKY
                    if volatility > 0.10:
                        temporal_adjustment += 0.05
                        reasons.append(f"high volatility (σ={volatility:.3f}) - unstable")
                    elif volatility > 0.05:
                        temporal_adjustment += 0.02
                        reasons.append(f"moderate volatility (σ={volatility:.3f})")

            # CRITICAL FIX: Separate temporal influence from base adjustment
            # Base adjustment: weighted sum of core risk factors (overdue, payment, delay, outstanding, exposure)
            base_adjustment = max(-0.3, min(0.85, total_adjustment))

            # Apply weighted blending to base factors
            adjusted_risk = base_risk + (0.5 * base_adjustment)

            # CRITICAL FIX: Apply temporal adjustment SEPARATELY to avoid feedback loop
            # Temporal signals (velocity, trend, volatility) should influence but NOT dominate
            # Clamped temporal_adjustment to safe bounds
            temporal_adjustment = max(-0.2, min(0.3, temporal_adjustment))
            adjusted_risk += 0.3 * temporal_adjustment

            # Clamp to [0, 1]
            adjusted_risk = max(0, min(1, adjusted_risk))

            logger.debug(
                f"[RiskAgent] Risk refinement for {customer_id}: "
                f"base={base_risk:.3f} + "
                f"(0.5 × [0.40×{overdue_adjustment:+.3f} + 0.20×{payment_adjustment:+.3f} + "
                f"0.15×{delay_adjustment:+.3f} + 0.15×{outstanding_adjustment:+.3f} + "
                f"0.10×{high_exposure_adjustment:+.3f}]) + "
                f"(0.3 × temporal[{temporal_adjustment:+.3f}]) "
                f"= adjusted={adjusted_risk:.3f}"
            )

            return adjusted_risk

        except Exception as e:
            logger.error(f"[RiskAgent] Error refining risk: {e}")
            import traceback
            traceback.print_exc()
            # On error, return base risk
            return base_risk
