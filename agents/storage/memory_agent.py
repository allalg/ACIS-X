import logging
import threading
import sqlite3
from collections import deque
from datetime import datetime
from typing import List, Any, Dict, Optional, Set

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class MemoryAgent(BaseAgent):
    """
    Memory Agent for ACIS-X.

    Maintains in-memory customer state DERIVED FROM DATABASE.
    Recomputes state from DB on every event (NOT incremental).
    Provides fast access to aggregated customer metrics.

    This is STATELESS - all truth comes from DB via QueryAgent.

    Subscribes to:
    - acis.invoices (invoice.created, invoice.overdue, etc.)
    - acis.payments (payment.received)
    - acis.risk (risk.scored, risk.high.detected)
    - acis.metrics (customer.metrics.updated)

    Publishes to:
    - acis.memory (customer.state.updated)
    """

    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_RISK = "acis.risk"
    TOPIC_METRICS = "acis.metrics"
    TOPIC_MEMORY = "acis.memory"

    def __init__(
        self,
        kafka_client: Any,
        query_agent: Optional[Any] = None,
    ):
        super().__init__(
            agent_name="MemoryAgent",
            agent_version="1.0.0",
            group_id="memory-agent-group",
            subscribed_topics=[
                self.TOPIC_INVOICES,
                self.TOPIC_PAYMENTS,
                self.TOPIC_RISK,
                self.TOPIC_METRICS,
            ],
            capabilities=[
                "state_management",
                "customer_memory",
            ],
            kafka_client=kafka_client,
            agent_type="MemoryAgent",
        )

        # In-memory customer state (DERIVED, NOT authoritative)
        self.customer_state: Dict[str, Dict[str, Any]] = {}
        self._state_lock = threading.Lock()

        # ISSUE 2 FIX: O(1) idempotency check using hybrid Set+Deque
        # Set for fast lookup (O(1)), Deque for bounded storage
        self.processed_events_set: Set[str] = set()
        self.processed_events_queue: deque = deque(maxlen=10000)
        self._processed_lock = threading.Lock()

        # IMPROVEMENT 2 FIX: Temporal risk history for trend detection
        # Tracks last 20 risk scores per customer for velocity/deterioration analysis
        self.risk_history: Dict[str, deque] = {}  # customer_id → deque of (timestamp, risk_score)
        self._history_lock = threading.Lock()

        # Reference to QueryAgent (DB source of truth)
        self.query_agent = query_agent

        # SQLite for metrics persistence (optional but recommended)
        self._db_path = "acis.db"
        self._db_lock = threading.Lock()

    def set_query_agent(self, query_agent: Any) -> None:
        """Set QueryAgent reference for DB state recomputation."""
        self.query_agent = query_agent
        logger.info("QueryAgent reference set for state recomputation")

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [
            self.TOPIC_INVOICES,
            self.TOPIC_PAYMENTS,
            self.TOPIC_RISK,
            self.TOPIC_METRICS,
        ]

    def start(self) -> None:
        """
        Override BaseAgent start to rebuild state from DB on startup.

        This ensures MemoryAgent is pre-populated with all customers
        before processing events. Prevents RiskScoringAgent from getting
        empty state in the first few seconds.
        """
        self.rebuild_state_from_db()
        super().start()

    def rebuild_state_from_db(self) -> None:
        """
        CRITICAL: Rebuild MemoryAgent state from DB on startup.

        Populates customer_state with all existing customers so that:
        - RiskScoringAgent has correct input from the start
        - No stale empty state during startup
        - System is consistent after restart
        """
        if not self.query_agent:
            logger.error("[MemoryAgent] QueryAgent not available, cannot rebuild state")
            return

        try:
            logger.info("[MemoryAgent] Starting state rebuild from database...")

            customers = self.query_agent.get_all_customers()
            logger.info(f"[MemoryAgent] Found {len(customers)} customers to rebuild")

            with self._state_lock:
                for cust in customers:
                    customer_id = cust.get("customer_id")
                    if not customer_id:
                        continue

                    # Recompute state from DB
                    recomputed = self._recompute_state(customer_id)

                    # Create full state entry
                    self.customer_state[customer_id] = {
                        "customer_id": customer_id,
                        "total_outstanding": recomputed.get("total_outstanding", 0.0),
                        "overdue_count": recomputed.get("overdue_count", 0),
                        "avg_delay": 0.0,  # Will be updated by metrics event
                        "on_time_ratio": 0.5,  # Will be updated by metrics event
                        "risk_score": cust.get("risk_score", 0.0),
                        "last_updated": datetime.utcnow().isoformat(),
                        # CRITICAL FIX: Initialize invoice details storage
                        "invoice_details": [],
                        "risks": [],
                    }

                    logger.debug(
                        f"[MemoryAgent] Rebuilt state for {customer_id}: "
                        f"outstanding={recomputed.get('total_outstanding')} "
                        f"overdue={recomputed.get('overdue_count')}"
                    )

            logger.info(f"[MemoryAgent] State rebuild complete: {len(self.customer_state)} customers loaded")

        except Exception as e:
            logger.error(f"[MemoryAgent] Error rebuilding state from DB: {e}")
            import traceback
            traceback.print_exc()

    def process_event(self, event: Event) -> None:
        """
        Process incoming events and update customer state.

        ISSUE 2 FIX: O(1) idempotency check using Set for fast lookup + Deque for bounded storage
        Both are kept in sync: Set for O(1) lookup, Deque for bounded FIFO tracking.
        """
        # ISSUE 2 FIX: O(1) lookup with Set, bounded O(1) add/remove with Deque
        with self._processed_lock:
            if event.event_id in self.processed_events_set:
                logger.debug(f"[MemoryAgent] Duplicate event {event.event_id}, skipping")
                return

            # Check if deque is at max capacity before adding
            # If so, we'll need to remove the oldest from the set after append
            at_capacity = len(self.processed_events_queue) >= 10000
            if at_capacity:
                oldest_event_id = self.processed_events_queue[0]  # Get oldest before it auto-drops

            # Add to both Set (for fast lookup) and Queue (for bounded storage)
            self.processed_events_set.add(event.event_id)
            self.processed_events_queue.append(event.event_id)

            # After append with maxlen, auto-drop oldest occurs
            # So remove it from set too to keep in sync
            if at_capacity:
                self.processed_events_set.discard(oldest_event_id)

        event_type = event.event_type

        if event_type.startswith("invoice."):
            self._handle_invoice_change(event)
        elif event_type == "payment.received":
            self._handle_payment_received(event)
        elif event_type == "risk.scored":  # FIX: standardized event name
            self._handle_risk_scored(event)
        elif event_type == "risk.high.detected":  # FIX: new standardized event
            self._handle_risk_scored(event)  # Handle same as risk.scored
        elif event_type == "customer.metrics.updated":
            self._handle_metrics_updated(event)

    def _recompute_state(self, customer_id: str) -> Dict[str, Any]:
        """
        CORE FIX: Recompute customer state from DB (NOT incremental).

        This is the source of truth computation. All metrics derived from DB queries.
        Handles:
        - Duplicate events (recompute gives same result)
        - Out-of-order events (DB is authoritative)
        - System restarts (DB survives)
        """
        if not self.query_agent:
            logger.error("[MemoryAgent] QueryAgent not available, cannot recompute state")
            return {}

        try:
            # Get all non-paid invoices
            invoices = self.query_agent.get_invoices_by_customer(customer_id)

            # Compute total outstanding from remaining amounts
            total_outstanding = sum(
                inv.get("remaining_amount", 0.0) for inv in invoices
            )

            # Get overdue invoices
            overdue = self.query_agent.get_overdue_invoices(customer_id)
            overdue_count = len(overdue)

            logger.debug(
                f"[MemoryAgent] Recomputed customer {customer_id}: "
                f"outstanding={total_outstanding} overdue={overdue_count}"
            )

            return {
                "total_outstanding": total_outstanding,
                "overdue_count": overdue_count,
            }

        except Exception as e:
            logger.error(f"[MemoryAgent] Error recomputing state for {customer_id}: {e}")
            return {
                "total_outstanding": 0.0,
                "overdue_count": 0,
            }

    def _get_or_create_customer_state(self, customer_id: str) -> Dict[str, Any]:
        """Get existing customer state or create new one."""
        if customer_id not in self.customer_state:
            self.customer_state[customer_id] = {
                "customer_id": customer_id,
                "total_outstanding": 0.0,
                "overdue_count": 0,
                "avg_delay": 0.0,
                "on_time_ratio": 0.5,
                "risk_score": 0.0,
                "last_updated": datetime.utcnow().isoformat(),
                # CRITICAL FIX: Add structured invoice details storage
                "invoice_details": [],
                "risks": [],
            }
            logger.info(f"[MemoryAgent] Created new state for customer: {customer_id}")
        return self.customer_state[customer_id]

    def _handle_invoice_change(self, event: Event) -> None:
        """
        Handle ANY invoice change (created, overdue, disputed, cancelled).
        FIXED: Recompute from DB instead of incrementing.
        IMPROVED: Only publish if state actually changed.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("[MemoryAgent] Invoice event missing customer_id, skipping")
            return

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)

            # Save previous state for change detection
            previous_outstanding = state["total_outstanding"]
            previous_overdue = state["overdue_count"]

            # RECOMPUTE from DB (not increment)
            recomputed = self._recompute_state(customer_id)
            state["total_outstanding"] = recomputed.get("total_outstanding", 0.0)
            state["overdue_count"] = recomputed.get("overdue_count", 0)
            state["last_updated"] = datetime.utcnow().isoformat()

            # ONLY publish if state actually changed
            if state["total_outstanding"] != previous_outstanding or state["overdue_count"] != previous_overdue:
                logger.info(
                    f"[MemoryAgent] {event.event_type} for {customer_id}: "
                    f"outstanding={state['total_outstanding']} (was {previous_outstanding}) "
                    f"overdue={state['overdue_count']} (was {previous_overdue})"
                )
                self._publish_state_update(customer_id, state, event.correlation_id)
                self._persist_metrics(customer_id, state)
            else:
                logger.debug(
                    f"[MemoryAgent] {event.event_type} for {customer_id}: "
                    f"no state change, skipping publish"
                )

    def _handle_payment_received(self, event: Event) -> None:
        """
        Handle payment.received event.
        FIXED: Recompute from DB instead of decrementing.
        FIXED: Update last_payment_date when actual payment received.
        IMPROVED: Only publish if state actually changed.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("[MemoryAgent] payment.received missing customer_id, skipping")
            return

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)

            # Save previous state for change detection
            previous_outstanding = state["total_outstanding"]
            previous_overdue = state["overdue_count"]

            # RECOMPUTE from DB (not decrement)
            recomputed = self._recompute_state(customer_id)
            state["total_outstanding"] = recomputed.get("total_outstanding", 0.0)
            state["overdue_count"] = recomputed.get("overdue_count", 0)
            state["last_updated"] = datetime.utcnow().isoformat()

            # ONLY publish if state actually changed
            if state["total_outstanding"] != previous_outstanding or state["overdue_count"] != previous_overdue:
                logger.info(
                    f"[MemoryAgent] payment for {customer_id}: "
                    f"outstanding={state['total_outstanding']} (was {previous_outstanding}) "
                    f"overdue={state['overdue_count']} (was {previous_overdue})"
                )
                self._publish_state_update(customer_id, state, event.correlation_id)
            else:
                logger.debug(
                    f"[MemoryAgent] payment for {customer_id}: "
                    f"no state change, skipping publish"
                )

            # ONLY persist metrics on actual payment event
            self._persist_metrics(customer_id, state, update_last_payment=True)

    def _handle_risk_scored(self, event: Event) -> None:
        """Handle risk.scored or risk.high.detected event - update risk_score.
        CRITICAL FIX: Store structured invoice details (amount, days_overdue, timestamp) for aggregation.
        IMPROVEMENT 2: Record risk score in history for trend detection.
        IMPROVED: Only publish if risk_score actually changed.
        """
        import time
        data = event.payload or {}
        customer_id = data.get("customer_id")
        risk_score = data.get("risk_score", 0.0)

        if not customer_id:
            logger.warning("[MemoryAgent] risk event missing customer_id, skipping")
            return

        try:
            risk_score = float(risk_score) if risk_score else 0.0
        except (ValueError, TypeError):
            risk_score = 0.0

        # CRITICAL FIX: Extract invoice details for structured aggregation
        amount = float(data.get("amount", 0.0) or 0.0)
        days_overdue = float(data.get("days_overdue", 0.0) or 0.0)
        timestamp = float(data.get("timestamp", time.time()) or time.time())

        # IMPROVEMENT 2: Record in history for temporal analysis
        self.record_risk_score(customer_id, risk_score)

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)
            previous_risk = state["risk_score"]
            state["risk_score"] = risk_score
            state["last_updated"] = datetime.utcnow().isoformat()

            # CRITICAL FIX: Store structured invoice data
            # Initialize invoice_details if not present
            if "invoice_details" not in state:
                state["invoice_details"] = []

            # Store this invoice's risk data with metadata
            state["invoice_details"].append({
                "score": risk_score,
                "amount": amount,
                "days_overdue": days_overdue,
                "timestamp": timestamp
            })

            # Keep only last 20 invoices for memory efficiency
            state["invoice_details"] = state["invoice_details"][-20:]

            # Keep old risks for compatibility
            if "risks" not in state:
                state["risks"] = []
            state["risks"].append(risk_score)
            state["risks"] = state["risks"][-20:]

            # ONLY publish if risk_score actually changed
            if state["risk_score"] != previous_risk:
                logger.info(
                    f"[MemoryAgent] Updated risk for {customer_id}: "
                    f"risk_score={state['risk_score']:.4f} (was {previous_risk:.4f}) "
                    f"with invoice detail: amount={amount:.2f}, days_overdue={days_overdue:.1f}"
                )
                self._publish_state_update(customer_id, state, event.correlation_id)
                self._persist_metrics(customer_id, state)
            else:
                logger.debug(
                    f"[MemoryAgent] Risk for {customer_id}: "
                    f"no change, skipping publish"
                )

    def _handle_metrics_updated(self, event: Event) -> None:
        """Handle customer.metrics.updated event - update customer metrics.
        IMPROVED: Only publish if metrics actually changed.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("[MemoryAgent] metrics.updated missing customer_id, skipping")
            return

        with self._state_lock:
            state = self._get_or_create_customer_state(customer_id)

            # Save previous metrics for change detection
            previous_delay = state["avg_delay"]
            previous_ratio = state["on_time_ratio"]

            # Update metrics directly from event
            state["avg_delay"] = data.get("avg_delay", state.get("avg_delay", 0.0))
            state["on_time_ratio"] = data.get("on_time_ratio", state.get("on_time_ratio", 0.5))
            state["last_updated"] = datetime.utcnow().isoformat()

            # ONLY publish if metrics actually changed
            if state["avg_delay"] != previous_delay or state["on_time_ratio"] != previous_ratio:
                logger.info(
                    f"[MemoryAgent] Updated metrics for {customer_id}: "
                    f"avg_delay={state['avg_delay']} (was {previous_delay}) "
                    f"on_time_ratio={state['on_time_ratio']} (was {previous_ratio})"
                )
                self._publish_state_update(customer_id, state, event.correlation_id)
                self._persist_metrics(customer_id, state)
            else:
                logger.debug(
                    f"[MemoryAgent] Metrics for {customer_id}: "
                    f"no change, skipping publish"
                )

    def _publish_state_update(
        self,
        customer_id: str,
        state: Dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Publish customer state update event.
        IMPROVED: Now includes all computed metrics.
        """
        payload = {
            "customer_id": customer_id,
            "total_outstanding": round(state["total_outstanding"], 2),
            "overdue_count": state.get("overdue_count", 0),
            "risk_score": round(state["risk_score"], 4),
            "avg_delay": round(state["avg_delay"], 2),
            "on_time_ratio": round(state["on_time_ratio"], 4),
        }

        self.publish_event(
            topic=self.TOPIC_MEMORY,
            event_type="customer.state.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=correlation_id,
        )

        logger.debug(f"[MemoryAgent] Published state update for {customer_id}")

    def _persist_metrics(self, customer_id: str, state: Dict[str, Any], update_last_payment: bool = False) -> None:
        """
        Persist metrics to DB for recovery after restart.
        Writes to customer_metrics table.
        Uses IMMEDIATE isolation level for safety.

        Args:
            customer_id: Customer identifier
            state: Customer state dict with metrics
            update_last_payment: Only set to True when actual payment received
                                 False for other events (invoice, risk, metrics)
        """
        try:
            conn = sqlite3.connect(self._db_path, isolation_level="IMMEDIATE")
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()

            # IMPROVEMENT: Only update last_payment_date on actual payment events
            # For other events, the existing last_payment_date is preserved
            last_payment_date = now if update_last_payment else None

            if update_last_payment:
                # Payment event: update all metrics including last_payment_date
                cursor.execute("""
                    INSERT INTO customer_metrics (
                        customer_id,
                        total_outstanding,
                        avg_delay,
                        on_time_ratio,
                        last_payment_date,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO UPDATE SET
                        total_outstanding=excluded.total_outstanding,
                        avg_delay=excluded.avg_delay,
                        on_time_ratio=excluded.on_time_ratio,
                        last_payment_date=excluded.last_payment_date,
                        updated_at=excluded.updated_at
                """, (
                    customer_id,
                    state["total_outstanding"],
                    state["avg_delay"],
                    state["on_time_ratio"],
                    last_payment_date,
                    now
                ))
            else:
                # Non-payment event: update metrics but preserve existing last_payment_date
                cursor.execute("""
                    INSERT INTO customer_metrics (
                        customer_id,
                        total_outstanding,
                        avg_delay,
                        on_time_ratio,
                        last_payment_date,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO UPDATE SET
                        total_outstanding=excluded.total_outstanding,
                        avg_delay=excluded.avg_delay,
                        on_time_ratio=excluded.on_time_ratio,
                        updated_at=excluded.updated_at
                """, (
                    customer_id,
                    state["total_outstanding"],
                    state["avg_delay"],
                    state["on_time_ratio"],
                    None,  # Don't update last_payment_date
                    now
                ))

            conn.commit()
            conn.close()

            logger.debug(f"[MemoryAgent] Persisted metrics for {customer_id} (update_last_payment={update_last_payment})")

        except Exception as e:
            logger.warning(f"[MemoryAgent] Failed to persist metrics for {customer_id}: {e}")

    def get_customer_state(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Get current state for a customer (for internal use by QueryAgent)."""
        with self._state_lock:
            return self.customer_state.get(customer_id)

    def record_risk_score(self, customer_id: str, risk_score: float) -> None:
        """
        IMPROVEMENT 2: Record risk score for temporal trend analysis.

        Tracks last 20 risk scores with timestamps to enable:
        - Velocity detection (rapid deterioration)
        - Trend analysis (improving vs declining)
        - Volatility assessment (unstable risk)
        """
        with self._history_lock:
            if customer_id not in self.risk_history:
                self.risk_history[customer_id] = deque(maxlen=20)
            self.risk_history[customer_id].append({
                "timestamp": datetime.utcnow().isoformat(),
                "risk_score": risk_score
            })

    def get_risk_velocity(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        IMPROVEMENT 2: Analyze risk trend for early warning signals.

        Returns:
            {
                'velocity': change in risk (positive=deteriorating),
                'trend': 'deteriorating'|'stable'|'improving',
                'volatility': std_dev of recent scores,
                'last_3_scores': [...]
            }
            Or None if insufficient history
        """
        with self._history_lock:
            if customer_id not in self.risk_history:
                return None

            history = self.risk_history[customer_id]
            if len(history) < 2:
                return None

            scores = [h["risk_score"] for h in history]

            # Calculate velocity (change over time)
            if len(scores) >= 3:
                last_3 = scores[-3:]
                velocity = last_3[-1] - last_3[0]  # Positive = deteriorating
            else:
                velocity = scores[-1] - scores[0]

            # Calculate trend
            if velocity > 0.15:
                trend = "deteriorating_fast"
            elif velocity > 0.05:
                trend = "deteriorating_slow"
            elif velocity < -0.1:
                trend = "improving"
            else:
                trend = "stable"

            # Calculate volatility (standard deviation)
            if len(scores) >= 3:
                import statistics
                volatility = statistics.stdev(scores[-5:] if len(scores) >= 5 else scores)
            else:
                volatility = 0.0

            return {
                "velocity": velocity,
                "trend": trend,
                "volatility": volatility,
                "last_n_scores": list(scores[-5:]),  # Last 5 for inspection
                "history_length": len(history),
            }
