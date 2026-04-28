"""
Collections Agent for ACIS-X - Policy Decision Engine.

Converts risk intelligence into actionable credit control decisions.
Supports both invoice-level and customer-level collection actions.

Subscribes to: acis.risk
Publishes to: acis.collections

Event Types:
- Handles: risk.scored, risk.high.detected
- Emits: collection.action (unified event type)
"""

import logging
import threading
import time
from collections import deque
from typing import List, Any, Dict, Set, Tuple, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class CollectionsAgent(BaseAgent):
    """
    Collections Agent for ACIS-X - Intelligent Policy Engine.

    Consumes:
    - risk.scored (invoice-level risk with metadata)
    - risk.high.detected (customer-level high risk)

    Produces:
    - collection.action (unified decision event with rich context)

    Decision Model:
    1. Fetch customer metrics (outstanding, delay, on_time_ratio, overdue_count)
    2. Fetch customer profile risk_score
    3. Blend: final_risk = 0.6*invoice_risk + 0.4*customer_risk
    4. Calculate severity with exposure, delay, behavior factors
    5. Apply decision rules based on severity thresholds
    6. Apply state-aware escalations (overdue_count, delay penalties)
    7. Maintain duplicate prevention with 3-factor key (customer, invoice, action)
    """

    TOPIC_RISK = "acis.risk"
    TOPIC_COLLECTIONS = "acis.collections"

    def __init__(
        self,
        kafka_client: Any,
    ):
        super().__init__(
            agent_name="CollectionsAgent",
            agent_version="2.0.0",  # Upgraded to policy engine
            group_id="collections-group",
            subscribed_topics=[self.TOPIC_RISK],
            capabilities=[
                "collection_management",
                "credit_control",
                "policy_decision_engine",
                "context_aware_routing",
            ],
            kafka_client=kafka_client,
            agent_type="CollectionsAgent",
        )

        # Priority mapping: string -> numeric score (for queuing, ML, ranking)
        self._priority_scores = {
            "medium": 0.5,
            "high": 0.8,
            "critical": 1.0,
        }

        # Policy configuration: severity thresholds (config-driven, adaptable)
        self.policy_config = {
            "thresholds": {
                "no_action": 0.3,
                "send_reminder": 0.6,
                "escalate_invoice": 0.8,
                "hold_credit": 0.9,
                "legal_escalation": 1.0,
            }
        }

        # Duplicate prevention: 3-factor key (customer_id, invoice_id, action)
        self._processed_actions_set: Set[Tuple[str, str, str]] = set()
        self._processed_actions_queue: deque = deque(maxlen=10000)
        self._processed_lock = threading.Lock()

        # Time-based cooldown to prevent action spam (5 min cooldown per action)
        self._last_action_time: Dict[Tuple[str, str, str], float] = {}
        self._cooldown_seconds = 300  # 5 minutes

    def subscribe(self) -> List[str]:
        """Return topics to subscribe to."""
        return [self.TOPIC_RISK]

    def _add_processed_action(self, action_key: Tuple[str, str, str]) -> None:
        """
        Add action to processed tracking with bounded memory.
        Key: (customer_id, invoice_id, action_type)
        """
        with self._processed_lock:
            if len(self._processed_actions_queue) >= 10000:
                oldest = self._processed_actions_queue[0]
                self._processed_actions_set.discard(oldest)

            self._processed_actions_set.add(action_key)
            self._processed_actions_queue.append(action_key)

    def _is_processed_action(self, action_key: Tuple[str, str, str]) -> bool:
        """Check if action was already processed (O(1) lookup)."""
        with self._processed_lock:
            return action_key in self._processed_actions_set

    def process_event(self, event: Event) -> None:
        """Process incoming risk events."""
        if event.event_type == "risk.scored":
            self._handle_risk_score_updated(event)
        elif event.event_type == "risk.high.detected":
            self._handle_high_risk_detected(event)

    # ========== INVOICE-LEVEL POLICY ENGINE ==========

    def _handle_risk_score_updated(self, event: Event) -> None:
        """
        Handle risk.scored event (invoice-level) with intelligent policy engine.

        STEP 1: Enrich input with customer context
        STEP 2: Blend invoice + customer risk
        STEP 3: Calculate severity with behavioral factors
        STEP 4: Apply decision rules
        STEP 5: Apply state-aware escalations
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        invoice_risk_score = float(data.get("risk_score") or 0.0)

        if not customer_id or not invoice_id:
            logger.warning(
                f"[CollectionsAgent] risk.scored missing customer_id or invoice_id"
            )
            return

        # Generate unique action_id for tracing
        action_id = f"{customer_id}-{invoice_id}-{int(time.time() * 1000)}"

        logger.info(
            f"[CollectionsAgent] [{action_id}] Processing risk.scored: "
            f"customer={customer_id}, invoice={invoice_id}, risk={invoice_risk_score:.3f}"
        )

        try:
            # ===== STEP 1: ENRICH INPUT DATA =====
            customer_metrics = QueryClient.query("get_customer_metrics", {"customer_id": customer_id})
            if not customer_metrics:
                logger.warning(
                    f"[CollectionsAgent] No metrics found for {customer_id}, using defaults"
                )
                customer_metrics = self._default_metrics()

            total_outstanding = float(customer_metrics.get("total_outstanding") or 0.0)
            avg_delay = float(customer_metrics.get("avg_delay") or 0.0)
            on_time_ratio = float(customer_metrics.get("on_time_ratio") or 0.0)
            overdue_count = int(customer_metrics.get("overdue_count") or 0)
            credit_limit = float(customer_metrics.get("credit_limit") or 100000.0)

            # Extract invoice-level data
            invoice_amount = float(data.get("amount") or 0.0)
            invoice_days_overdue = float(data.get("days_overdue") or 0.0)

            logger.debug(
                f"[CollectionsAgent] [{action_id}] Customer context: "
                f"outstanding={total_outstanding:.2f}, delay={avg_delay:.1f}d, "
                f"on_time_ratio={on_time_ratio:.2f}, overdue={overdue_count}"
            )

            # ===== STEP 2: USE FINAL RISK FROM RiskScoringAgent =====
            # CRITICAL FIX: invoice_risk_score is already FINAL from RiskScoringAgent
            # RiskScoringAgent already refined it with customer context + external risk
            # DO NOT blend with stale DB risk_score - that would double-count customer signals
            # Instead, use invoice_risk_score directly for severity calculation
            final_risk = invoice_risk_score  # This is already final from RiskScoringAgent

            logger.debug(
                f"[CollectionsAgent] [{action_id}] Using final invoice risk: {final_risk:.3f} "
                f"(already refined by RiskScoringAgent with customer context + external signals)"
            )

            # ===== STEP 3: CALCULATE SEVERITY (for policy thresholds) =====
            # SEVERITY is a POLICY metric - different from RISK (which is technical)
            # Risk quantifies probability, Severity quantifies business impact
            # Severity = blend of: risk + exposure + delay + behavior patterns
            # Context factors influence escalation decision, not risk calculation

            # BUG FIX #4: Initialize severity_score from weighted blend before applying trends
            # IMPROVEMENT 1: Exposure relative to customer credit limit (not fixed 100k)
            exposure_factor = min(1.0, total_outstanding / max(credit_limit, 1.0)) if total_outstanding > 0 else 0.0
            delay_factor = min(1.0, avg_delay / 60.0)  # Normalize by 60 days
            behavior_penalty = 1.0 - on_time_ratio

            # Initialize severity_score from weighted blend
            severity_score = (
                (0.6 * final_risk) +
                (0.15 * exposure_factor) +
                (0.15 * delay_factor) +
                (0.1 * behavior_penalty)
            )
            severity_score = max(0.0, min(1.0, severity_score))

            # IMPROVEMENT 2: Add risk trend signal for predictive escalation
            risk_trend = customer_metrics.get("risk_trend", "stable")
            reason_for_trend = None
            if risk_trend == "deteriorating_fast":
                severity_score += 0.1
                reason_for_trend = "Rapid risk deterioration"
            elif risk_trend == "improving":
                severity_score -= 0.05
                reason_for_trend = "Improving payment behavior"

            # Clamp after trend adjustment
            severity_score = max(0.0, min(1.0, severity_score))

            logger.info(
                f"[CollectionsAgent] [{action_id}] Severity calculation (weighted blend): "
                f"0.6x{final_risk:.3f} + 0.15x{exposure_factor:.3f} (limit={credit_limit:.0f}) + "
                f"0.15x{delay_factor:.3f} + 0.1x{behavior_penalty:.3f}"
                + (f" + {reason_for_trend}" if reason_for_trend else "")
                + f" -> severity={severity_score:.3f}"
            )

            # ===== STEP 5: DECISION ENGINE =====
            action, priority, reason = self._compute_action(
                severity_score=severity_score,
                overdue_count=overdue_count,
                avg_delay=avg_delay,
                invoice_days_overdue=invoice_days_overdue,
                on_time_ratio=on_time_ratio,
                risk_trend=risk_trend,
            )

            if action is None:
                logger.debug(
                    f"[CollectionsAgent] [{action_id}] customer={customer_id}, invoice={invoice_id}: "
                    f"No action (severity={severity_score:.3f})"
                )
                return

            # Check for duplicate action
            action_key = (customer_id, invoice_id, action)
            if self._is_processed_action(action_key):
                logger.debug(
                    f"[CollectionsAgent] [{action_id}] Duplicate action ignored: {action_key}"
                )
                return

            # IMPROVEMENT 3: Time-based cooldown to prevent action spam (5 min per action)
            if self._is_in_cooldown(action_key):
                logger.debug(
                    f"[CollectionsAgent] [{action_id}] Action in cooldown: {action_key}"
                )
                return

            # Mark as processed
            self._add_processed_action(action_key)
            self._update_action_time(action_key)

            # Emit unified action event
            self._emit_action(
                customer_id=customer_id,
                invoice_id=invoice_id,
                action=action,
                priority=priority,
                reason=reason,
                severity_score=severity_score,
                context={
                    "final_risk": final_risk,  # Already refined by RiskScoringAgent
                    "invoice_risk": invoice_risk_score,  # From payment prediction
                    "outstanding": total_outstanding,
                    "delay": avg_delay,
                    "overdue_count": overdue_count,
                    "invoice_amount": invoice_amount,
                    # REMOVED: customer_risk (was duplicate of RiskScoringAgent's work)
                },
                event=event,
                action_id=action_id,
            )

        except Exception as e:
            logger.error(f"[CollectionsAgent] Error processing invoice risk: {e}")
            import traceback
            traceback.print_exc()

    def _compute_action(
        self,
        severity_score: float,
        overdue_count: int,
        avg_delay: float,
        invoice_days_overdue: float,
        on_time_ratio: float,
        risk_trend: str = "stable",
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Compute action and priority using config-driven decision rules.

        Uses policy_config["thresholds"] for severity -> action mapping.
        State-aware escalations:
        - If overdue_count > 3: escalate to next severity
        - If avg_delay > 45d: escalate to next severity
        """
        thresholds = self.policy_config["thresholds"]
        base_action = None
        base_priority = None
        reason_parts = []

        # === Base decision by severity (config-driven) ===
        if severity_score < thresholds["no_action"]:
            return None, None, None  # No action

        elif severity_score < thresholds["send_reminder"]:
            base_action = "send_reminder"
            base_priority = "medium"
            reason_parts.append(f"Moderate risk (score={severity_score:.2f})")

        elif severity_score < thresholds["escalate_invoice"]:
            base_action = "escalate_invoice"
            base_priority = "high"
            reason_parts.append(f"High risk (score={severity_score:.2f})")

        elif severity_score < thresholds["hold_credit"]:
            base_action = "hold_credit"
            base_priority = "high"
            reason_parts.append(f"Very high risk (score={severity_score:.2f})")

        else:  # >= hold_credit threshold
            base_action = "legal_escalation"
            base_priority = "critical"
            reason_parts.append(f"Critical risk (score={severity_score:.2f})")

        # Add trend signal to reason
        if risk_trend == "deteriorating_fast":
            reason_parts.append("Rapid risk deterioration")
        elif risk_trend == "improving":
            reason_parts.append("Improving payment behavior")

        # === STEP 6: State-aware escalation ===
        escalated_action = base_action
        escalated_priority = base_priority

        if overdue_count > 3:
            reason_parts.append(f"Multiple delinquencies ({overdue_count} overdue)")
            # Escalate to next level
            escalation_map = {
                "send_reminder": "escalate_invoice",
                "escalate_invoice": "hold_credit",
                "hold_credit": "legal_escalation",
                "legal_escalation": "legal_escalation",
            }
            escalated_action = escalation_map.get(base_action, base_action)
            if escalated_priority == "medium":
                escalated_priority = "high"
            elif escalated_priority == "high":
                escalated_priority = "critical"

        if avg_delay > 45:
            reason_parts.append(f"Chronic delays ({avg_delay:.1f}d avg)")
            # Additional escalation
            escalation_map = {
                "send_reminder": "escalate_invoice",
                "escalate_invoice": "hold_credit",
                "hold_credit": "legal_escalation",
                "legal_escalation": "legal_escalation",
            }
            escalated_action = escalation_map.get(escalated_action, escalated_action)
            if escalated_priority in ["medium", "high"]:
                escalated_priority = "critical" if escalated_priority == "high" else "high"

        if invoice_days_overdue > 90:
            reason_parts.append(f"Long overdue invoice ({invoice_days_overdue:.0f}d)")
            if escalated_action == "send_reminder":
                escalated_action = "escalate_invoice"
            escalated_priority = "critical"

        reason = ", ".join(reason_parts) if reason_parts else "Policy decision"

        return escalated_action, escalated_priority, reason

    # ========== CUSTOMER-LEVEL POLICY ENGINE ==========

    def _handle_high_risk_detected(self, event: Event) -> None:
        """
        Handle risk.high.detected event (customer-level).

        Computes severity using customer metrics only.
        Triggers credit_hold or legal_escalation.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")
        customer_risk_score = float(data.get("risk_score") or 0.0)

        if not customer_id:
            logger.warning(
                f"[CollectionsAgent] risk.high.detected missing customer_id"
            )
            return

        # Generate unique action_id for tracing
        action_id = f"{customer_id}-customer-{int(time.time() * 1000)}"

        logger.info(
            f"[CollectionsAgent] [{action_id}] Processing risk.high.detected: "
            f"customer={customer_id}, risk={customer_risk_score:.3f}"
        )

        try:
            # Extract customer metrics
            customer_metrics = QueryClient.query("get_customer_metrics", {"customer_id": customer_id})
            if not customer_metrics:
                logger.warning(
                    f"[CollectionsAgent] No metrics for {customer_id}, using defaults"
                )
                customer_metrics = self._default_metrics()

            overdue_invoices = QueryClient.query("get_overdue_invoices", {"customer_id": customer_id})
            overdue_count = len(overdue_invoices)

            if overdue_count == 0:
                logger.debug(
                    f"[CollectionsAgent] [{action_id}] customer={customer_id}: No overdue invoices"
                )
                return

            total_outstanding = float(customer_metrics.get("total_outstanding") or 0.0)
            avg_delay = float(customer_metrics.get("avg_delay") or 0.0)
            on_time_ratio = float(customer_metrics.get("on_time_ratio") or 0.0)
            credit_limit = float(customer_metrics.get("credit_limit") or 100000.0)

            logger.debug(
                f"[CollectionsAgent] [{action_id}] Customer-level metrics: "
                f"outstanding={total_outstanding:.2f}, delay={avg_delay:.1f}d, "
                f"on_time_ratio={on_time_ratio:.2f}, overdue={overdue_count}"
            )

            # Calculate severity for customer-level
            # IMPROVEMENT 1: Exposure relative to credit limit
            exposure_factor = min(1.0, total_outstanding / max(credit_limit, 1.0))
            delay_factor = min(1.0, avg_delay / 60.0)
            behavior_penalty = 1.0 - on_time_ratio

            # IMPROVEMENT 2: Severity weighted blend (not additive)
            # Initialize severity_score first
            severity_score = (
                (0.6 * customer_risk_score) +
                (0.15 * exposure_factor) +
                (0.15 * delay_factor) +
                (0.1 * behavior_penalty)
            )
            severity_score = max(0.0, min(1.0, severity_score))

            # IMPROVEMENT 2: Extract risk trend (as in invoice-level) and apply
            risk_trend = customer_metrics.get("risk_trend", "stable")
            reason_for_trend = None
            if risk_trend == "deteriorating_fast":
                severity_score += 0.1
                reason_for_trend = "Rapid risk deterioration"
            elif risk_trend == "improving":
                severity_score -= 0.05
                reason_for_trend = "Improving payment behavior"

            # Clamp after trend adjustment
            severity_score = max(0.0, min(1.0, severity_score))

            logger.info(
                f"[CollectionsAgent] [{action_id}] Customer-level severity (weighted blend): {severity_score:.3f} "
                f"(risk={customer_risk_score:.3f}, exposure={exposure_factor:.3f}, "
                f"delay={delay_factor:.3f}, behavior={behavior_penalty:.3f})"
                + (f" + {reason_for_trend}" if reason_for_trend else "")
            )

            # Customer-level: only credit_hold or legal_escalation
            if severity_score >= 0.8:
                action = "legal_escalation"
                priority = "critical"
                reason = f"Critical customer risk ({severity_score:.2f}), {overdue_count} overdue invoices"
            elif severity_score >= 0.6 or overdue_count >= 3:
                action = "hold_credit"
                priority = "high"
                reason = f"High customer risk ({severity_score:.2f}), {overdue_count} overdue invoices"
            else:
                logger.debug(
                    f"[CollectionsAgent] customer={customer_id}: "
                    f"Insufficient severity for customer action ({severity_score:.3f})"
                )
                return

            # Check duplicate
            action_key = (customer_id, "customer", action)
            if self._is_processed_action(action_key):
                logger.debug(
                    f"[CollectionsAgent] Duplicate customer action ignored: {action_key}"
                )
                return

            # IMPROVEMENT 3: Time-based cooldown
            if self._is_in_cooldown(action_key):
                logger.debug(
                    f"[CollectionsAgent] Customer action in cooldown: {action_key}"
                )
                return

            self._add_processed_action(action_key)
            self._update_action_time(action_key)

            # Emit unified action event
            self._emit_action(
                customer_id=customer_id,
                invoice_id="customer_level",
                action=action,
                priority=priority,
                reason=reason,
                severity_score=severity_score,
                context={
                    "customer_risk": customer_risk_score,
                    "outstanding": total_outstanding,
                    "delay": avg_delay,
                    "overdue_count": overdue_count,
                },
                event=event,
            )

        except Exception as e:
            logger.error(f"[CollectionsAgent] Error processing customer high risk: {e}")
            import traceback
            traceback.print_exc()

    def _is_in_cooldown(self, action_key: Tuple[str, str, str]) -> bool:
        """Check if action is within cooldown window (5 minutes)."""
        with self._processed_lock:
            last_time = self._last_action_time.get(action_key, 0)
            now = time.time()
            return (now - last_time) < self._cooldown_seconds

    def _update_action_time(self, action_key: Tuple[str, str, str]) -> None:
        """Update last action time."""
        with self._processed_lock:
            self._last_action_time[action_key] = time.time()

    # ========== UNIFIED EMIT FUNCTION ==========

    def _emit_action(
        self,
        customer_id: str,
        invoice_id: str,
        action: str,
        priority: str,
        reason: str,
        severity_score: float,
        context: Dict[str, Any],
        event: Event,
        action_id: Optional[str] = None,
    ) -> None:
        """
        Emit unified collection.action event with rich context.

        Payload includes:
        - Action type and priority
        - Severity score for downstream priority queues
        - Full context for auditing and analysis
        - Correlation for event tracing
        - Action ID for distributed tracing
        """
        payload = {
            "customer_id": customer_id,
            "invoice_id": invoice_id,
            "action": action,
            "priority": priority,
            "severity_score": round(severity_score, 4),
            "reason": reason,
            "context": context,  # Rich context for auditing
            "action_id": action_id,  # Tracing ID
        }

        self.publish_event(
            topic=self.TOPIC_COLLECTIONS,
            event_type="collection.action",
            entity_id=customer_id,
            payload=payload,
            correlation_id=event.correlation_id,
        )

        logger.info(
            f"[CollectionsAgent] [{action_id}] customer={customer_id}, invoice={invoice_id} -> "
            f"action={action}, priority={priority}, severity={severity_score:.3f}"
        )

    # ========== HELPER FUNCTIONS ==========

    def _default_metrics(self) -> Dict[str, Any]:
        """Return default metrics for new/missing customers."""
        return {
            "total_outstanding": 0.0,
            "avg_delay": 0.0,
            "on_time_ratio": 0.0,
            "overdue_count": 0,
            "credit_limit": 100000.0,
        }
