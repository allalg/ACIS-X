import logging
import random
import sqlite3
import threading
from datetime import datetime
from typing import List, Any, Dict, Optional, Tuple, Set

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class CustomerStateAgent(BaseAgent):
    """
    Customer State Agent for ACIS-X.

    Computes customer behavior metrics (avg_delay, on_time_ratio, total_outstanding)
    using incremental updates triggered by invoice and payment events.

    Subscribes to:
    - acis.invoices (invoice.created)
    - acis.payments (payment.received, payment.partial)

    Publishes to:
    - acis.metrics (customer.metrics.updated)

    Persists metrics to customer_metrics table.

    Optimization: Uses incremental cache updates instead of full recompute on each event.
    """

    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_METRICS = "acis.metrics"
    DB_PATH = "acis.db"

    def __init__(
        self,
        kafka_client: Any,
        query_agent: Any,
        db_path: Optional[str] = None,
    ):
        super().__init__(
            agent_name="CustomerStateAgent",
            agent_version="1.0.0",
            group_id="customer-state-group",
            subscribed_topics=[self.TOPIC_INVOICES, self.TOPIC_PAYMENTS],
            capabilities=[
                "customer_behavior_analysis",
                "credit_metrics_computation",
            ],
            kafka_client=kafka_client,
            agent_type="CustomerStateAgent",
        )

        self._query_agent = query_agent
        self._db_path = db_path or self.DB_PATH
        self._db_lock = threading.Lock()

        # Incremental metrics cache per customer
        # Structure: customer_id -> {
        #   "total_outstanding": float,
        #   "total_paid_invoices": int,
        #   "total_on_time_count": int,
        #   "sum_of_delays": float,
        #   "processed_paid_invoices": Set[str],  # invoice_ids already counted
        #   "last_updated": str
        # }
        self._metrics_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.Lock()

        # Pending payment tracking for retry on DB race conditions
        # Structure: (customer_id, invoice_id) -> {"payment_date": str, "retry_count": int}
        self._pending_payments: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._pending_lock = threading.Lock()

        # Track permanently failed invoices (never retry after max attempts)
        self._failed_invoices: set = set()
        self._failed_lock = threading.Lock()

        # Recursion guard for reconciliation
        self._reconciling: set = set()
        self._reconciling_lock = threading.Lock()

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INVOICES, self.TOPIC_PAYMENTS]

    def process_event(self, event: Event) -> None:
        """Process incoming events using incremental updates."""
        event_type = event.event_type

        if event_type == "invoice.created":
            self._handle_invoice_created(event)
        elif event_type in ["payment.received", "payment.partial"]:
            self._handle_payment_received(event)

    def _handle_invoice_created(self, event: Event) -> None:
        """Handle invoice.created event with incremental update."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("invoice.created event missing customer_id")
            return

        # Parse amount safely
        try:
            amount = float(data.get("amount", 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount in invoice.created event: {data.get('amount')}")
            return

        # Initialize cache if needed
        if not self._ensure_cache_initialized(customer_id):
            logger.warning(f"Failed to initialize cache for customer {customer_id}")
            return

        # Incremental update: just add to outstanding
        with self._cache_lock:
            cache = self._metrics_cache.get(customer_id)
            if cache is None:
                logger.warning(f"Cache disappeared for customer {customer_id}")
                return
            cache["total_outstanding"] += amount
            cache["last_updated"] = datetime.utcnow().isoformat()

        # Publish and persist
        self._publish_and_persist_metrics(customer_id, event.correlation_id)
        logger.debug(f"Invoice created: added {amount:.2f} to outstanding for customer {customer_id}")

        # Probabilistic cache reconciliation
        self._maybe_reconcile_cache(customer_id)

    def _handle_payment_received(self, event: Event) -> None:
        """Handle payment.received and payment.partial events with incremental update."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")
        payment_date_str = data.get("payment_date")

        if not customer_id or not invoice_id:
            logger.warning("payment.received event missing customer_id or invoice_id")
            return

        # Parse amount safely
        try:
            payment_amount = float(data.get("amount", 0))
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount in payment.received event: {data.get('amount')}")
            return

        # Initialize cache if needed
        if not self._ensure_cache_initialized(customer_id):
            logger.warning(f"Failed to initialize cache for customer {customer_id}")
            return

        # Update outstanding
        with self._cache_lock:
            cache = self._metrics_cache.get(customer_id)
            if cache is None:
                logger.warning(f"Cache disappeared for customer {customer_id}")
                return
            cache["total_outstanding"] = max(0.0, cache["total_outstanding"] - payment_amount)
            cache["last_updated"] = datetime.utcnow().isoformat()

        # Check if invoice is now paid and update delay metrics
        self._update_paid_invoice_metrics(customer_id, invoice_id, payment_date_str)

        # Publish and persist
        self._publish_and_persist_metrics(customer_id, event.correlation_id)
        logger.debug(f"Payment received: updated metrics for customer {customer_id}")

        # Probabilistic cache reconciliation
        self._maybe_reconcile_cache(customer_id)

    def _ensure_cache_initialized(self, customer_id: str) -> bool:
        """
        Ensure metrics cache is initialized for customer.
        On first event, performs full recompute (cold start).
        """
        with self._cache_lock:
            if customer_id in self._metrics_cache:
                return True

        # Cold start: initialize from DB (outside lock to allow concurrent queries)
        try:
            metrics = self._compute_customer_metrics(customer_id)
            if metrics is None:
                return False

            invoices = self._query_agent.get_all_invoices_by_customer(customer_id)
            invoice_ids = [
                inv.get("invoice_id") for inv in invoices if inv.get("invoice_id")
            ]

            # Calculate initial cache values from paid invoices
            paid_invoices = [inv for inv in invoices if inv.get("status") == "paid"]
            payments = self._get_payments_for_invoices(invoice_ids)

            total_paid_invoices = len(paid_invoices)
            total_on_time_count, sum_of_delays = self._calculate_delay_stats(
                paid_invoices, payments
            )
            processed_paid_invoice_ids = {inv.get("invoice_id") for inv in paid_invoices}

            # Check again inside lock - another thread may have initialized
            with self._cache_lock:
                if customer_id in self._metrics_cache:
                    return True

                self._metrics_cache[customer_id] = {
                    "total_outstanding": metrics["total_outstanding"],
                    "avg_delay": metrics["avg_delay"],
                    "on_time_ratio": metrics["on_time_ratio"],
                    "last_payment_date": metrics.get("last_payment_date"),  # CRITICAL FIX #4
                    "total_paid_invoices": total_paid_invoices,
                    "total_on_time_count": total_on_time_count,
                    "sum_of_delays": sum_of_delays,
                    "processed_paid_invoices": processed_paid_invoice_ids,
                    "processed_overdue_invoices": set(),  # Track overdue invoices to prevent double-counting
                    "last_updated": datetime.utcnow().isoformat(),
                }

            logger.info(f"Initialized incremental cache for customer {customer_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize cache for {customer_id}: {e}")
            return False

    def _update_paid_invoice_metrics(
        self,
        customer_id: str,
        invoice_id: str,
        payment_date_str: Optional[str],
    ) -> None:
        """
        Update metrics when payment is received or invoice becomes permanently unpaid.
        Processes fully paid invoices (status == 'paid').
        Also processes overdue invoices that exceed retry limit (won't be paid).
        Handles partial payments by waiting for final payment.
        Handles DB race conditions via retry queue.
        """
        key = (customer_id, invoice_id)

        # Check if already permanently failed (exhausted retries)
        with self._failed_lock:
            if key in self._failed_invoices:
                logger.debug(f"Invoice {invoice_id} is in failed set, skipping")
                return

        # Get invoice data (for due_date and status check)
        invoice = self._query_agent.get_invoice(invoice_id)
        if not invoice:
            logger.warning(f"Invoice {invoice_id} not found in query agent")
            return

        # Check if invoice is actually paid
        if invoice.get("status") != "paid":
            # Not yet paid - check remaining amount before re-queueing
            remaining = invoice.get("remaining_amount", invoice.get("amount", 0))
            logger.debug(
                f"Invoice {invoice_id} not yet marked paid: status={invoice.get('status')}, remaining={remaining}"
            )

            # Only re-queue if there's actually a remaining amount
            # (partial payments awaiting completion)
            if remaining <= 0:
                logger.debug(f"Invoice {invoice_id} has no remaining amount, marking as failed")
                with self._failed_lock:
                    self._failed_invoices.add(key)
                # Fall through to calculate delay for overdue invoices
                # This ensures unpaid overdue invoices contribute to delay metrics
                if invoice.get("status") == "overdue":
                    # Check if this overdue invoice was already processed
                    with self._cache_lock:
                        cache = self._metrics_cache.get(customer_id)
                        if cache is None:
                            return
                        if invoice_id in cache.get("processed_overdue_invoices", set()):
                            logger.debug(f"Overdue invoice {invoice_id} already processed, skipping")
                            return
                        # Mark as processed before calculating
                        cache.setdefault("processed_overdue_invoices", set()).add(invoice_id)

                    self._calculate_and_update_delay(customer_id, invoice_id, invoice)
                return

            with self._pending_lock:
                # Only add if not already pending (preserve retry_count across burst events)
                if key not in self._pending_payments:
                    self._pending_payments[key] = {
                        "payment_date": payment_date_str,
                        "retry_count": 0,
                    }
            # Note: Don't trigger reconciliation here - it will be picked up by
            # _maybe_reconcile_cache during the next event or periodic reconciliation
            return

        # Remove from pending queue if present (now fully paid)
        with self._pending_lock:
            self._pending_payments.pop(key, None)

        # Check if already counted and mark as processed (atomic operation)
        with self._cache_lock:
            cache = self._metrics_cache.get(customer_id)
            if cache is None:
                logger.warning(f"Cache not initialized for customer {customer_id}")
                return

            # Skip if already counted (payment_received might be replayed)
            if invoice_id in cache["processed_paid_invoices"]:
                logger.debug(f"Invoice {invoice_id} already counted in metrics")
                return

            cache["processed_paid_invoices"].add(invoice_id)

        # Calculate delay for paid invoice
        self._calculate_and_update_delay(customer_id, invoice_id, invoice, payment_date_str)

    def _calculate_and_update_delay(
        self,
        customer_id: str,
        invoice_id: str,
        invoice: Dict[str, Any],
        payment_date_str: Optional[str] = None,
    ) -> None:
        """
        Calculate delay metrics for an invoice and update cache.
        For paid invoices: uses actual payment_date.
        For unpaid overdue invoices: uses current date as proxy for "payment".
        """
        try:
            due_date_str = invoice.get("due_date")
            if not due_date_str:
                logger.warning(f"Missing due_date for invoice {invoice_id}")
                return

            # For unpaid overdue invoices, use current date to calculate delay
            if payment_date_str is None:
                if invoice.get("status") == "overdue":
                    payment_date_str = datetime.utcnow().isoformat()
                    logger.info(
                        f"Calculating delay for unpaid overdue invoice {invoice_id} using current date"
                    )
                else:
                    logger.warning(f"Missing payment_date and invoice not overdue for invoice {invoice_id}")
                    return

            due_date = datetime.fromisoformat(due_date_str)
            payment_date = datetime.fromisoformat(payment_date_str)

            delay_days = max((payment_date - due_date).days, 0)
            is_on_time = (payment_date - due_date).days <= 0

            # Update cache with delay metrics
            with self._cache_lock:
                cache = self._metrics_cache.get(customer_id)
                if cache is None:
                    logger.warning(f"Cache not initialized for customer {customer_id}")
                    return

                cache["total_paid_invoices"] += 1
                cache["sum_of_delays"] += delay_days
                cache["last_payment_date"] = payment_date_str  # CRITICAL FIX #4: Track last payment date
                if is_on_time:
                    cache["total_on_time_count"] += 1

                # Recalculate metrics from cache
                if cache["total_paid_invoices"] > 0:
                    cache["avg_delay"] = (
                        cache["sum_of_delays"] / cache["total_paid_invoices"]
                    )
                    cache["on_time_ratio"] = (
                        cache["total_on_time_count"] / cache["total_paid_invoices"]
                    )

            logger.debug(
                f"Invoice {invoice_id} delay calculated: delay={delay_days} days, on_time={is_on_time}, "
                f"status={invoice.get('status')}"
            )

        except (ValueError, TypeError, AttributeError) as e:
            logger.error(f"Error calculating delay for invoice {invoice_id}: {e}")

    def _reconcile_with_db(self, customer_id: str) -> None:
        """
        Periodic cache reconciliation with DB to detect drift.
        Also retries pending payments that may have been queued due to race conditions.
        Corrects cache if events were missed or replayed.
        Safe to call frequently - expensive operations happen only on drift.
        """
        # Recursion guard: prevent infinite loops if reconciliation triggers retry
        with self._reconciling_lock:
            if customer_id in self._reconciling:
                logger.debug(f"Reconciliation already in progress for {customer_id}, skipping")
                return
            self._reconciling.add(customer_id)

        try:
            # Retry pending payments for this customer
            self._retry_pending_payments(customer_id)

            # Get fresh metrics from DB
            fresh_metrics = self._compute_customer_metrics(customer_id)
            if fresh_metrics is None:
                return

            with self._cache_lock:
                cache = self._metrics_cache.get(customer_id)
                if cache is None:
                    return

                # Check for drift in total_outstanding (most likely to diverge)
                old_outstanding = cache["total_outstanding"]
                new_outstanding = fresh_metrics["total_outstanding"]

                if abs(old_outstanding - new_outstanding) > 0.01:
                    logger.warning(
                        f"Cache drift detected for customer {customer_id}: "
                        f"outstanding was {old_outstanding:.2f}, corrected to {new_outstanding:.2f}"
                    )
                    # Restore fresh metrics
                    cache["total_outstanding"] = new_outstanding
                    cache["avg_delay"] = fresh_metrics["avg_delay"]
                    cache["on_time_ratio"] = fresh_metrics["on_time_ratio"]
                    cache["last_updated"] = datetime.utcnow().isoformat()

        except Exception as e:
            logger.error(f"Cache reconciliation failed for customer {customer_id}: {e}")
        finally:
            with self._reconciling_lock:
                self._reconciling.discard(customer_id)

    def _retry_pending_payments(self, customer_id: str) -> None:
        """
        Retry pending payments that were queued due to DB race conditions.
        Called during reconciliation to ensure no payments are lost.
        """
        with self._pending_lock:
            pending_items = [
                (cid, iid, data)
                for (cid, iid), data in list(self._pending_payments.items())
                if cid == customer_id
            ]

        # Early return if nothing to retry
        if not pending_items:
            logger.debug(f"No pending payments to retry for customer {customer_id}")
            return

        for cid, invoice_id, data in pending_items:
            payment_date_str = data.get("payment_date")
            retry_count = data.get("retry_count", 0)

            # Check retry limit BEFORE retrying
            if retry_count >= 10:
                logger.error(
                    f"Giving up on pending payment for invoice {invoice_id} after 10 retries"
                )
                with self._pending_lock:
                    self._pending_payments.pop((cid, invoice_id), None)
                with self._failed_lock:
                    self._failed_invoices.add((cid, invoice_id))
                continue

            # Retry payment metric update
            self._update_paid_invoice_metrics(cid, invoice_id, payment_date_str)

            # Update retry count
            with self._pending_lock:
                if (cid, invoice_id) in self._pending_payments:
                    self._pending_payments[(cid, invoice_id)]["retry_count"] = retry_count + 1

    def _maybe_reconcile_cache(self, customer_id: str) -> None:
        """
        FIX 4: Deterministic retry logic.

        Trigger cache reconciliation when:
        1. Pending payments exist for this customer (deterministic retry)
        2. Otherwise, use low-overhead probabilistic reconciliation (5% chance)

        This ensures reliability: pending payments are ALWAYS retried,
        while still maintaining cache coherency without excessive DB queries.
        """
        # FIX 4: First check if there are PENDING PAYMENTS for this customer
        # If yes, ALWAYS retry (deterministic)
        with self._pending_lock:
            has_pending = any(
                (cid, iid) for (cid, iid) in self._pending_payments.keys()
                if cid == customer_id
            )

        if has_pending:
            logger.debug(f"Pending payments exist for {customer_id}, triggering deterministic retry")
            self._reconcile_with_db(customer_id)
            return

        # FIX 4: Otherwise use probabilistic reconciliation (5% chance)
        # This is safe for normal operation - keeps cache fresh without constant DB queries
        if random.random() < 0.05:
            self._reconcile_with_db(customer_id)

    def _calculate_delay_stats(
        self,
        paid_invoices: List[Dict[str, Any]],
        payments: List[Dict[str, Any]],
    ) -> Tuple[int, float]:
        """
        Calculate on_time_count and sum_of_delays for paid invoices.

        Returns:
            Tuple of (total_on_time_count, sum_of_delays)
        """
        payment_map: Dict[str, List[Dict[str, Any]]] = {}
        for payment in payments:
            invoice_id = payment.get("invoice_id")
            if invoice_id:
                payment_map.setdefault(invoice_id, []).append(payment)

        on_time_count = 0
        sum_delays = 0.0

        for invoice in paid_invoices:
            invoice_id = invoice.get("invoice_id")
            due_date_str = invoice.get("due_date")
            payment_list = payment_map.get(invoice_id, [])

            if not payment_list or not due_date_str:
                continue

            try:
                latest_payment = max(
                    payment_list, key=lambda x: x.get("payment_date", "")
                )
                payment_date_str = latest_payment.get("payment_date")

                if not payment_date_str:
                    continue

                due_date = datetime.fromisoformat(due_date_str)
                payment_date = datetime.fromisoformat(payment_date_str)

                delay_days = max((payment_date - due_date).days, 0)
                sum_delays += delay_days

                if (payment_date - due_date).days <= 0:
                    on_time_count += 1

            except (ValueError, TypeError, AttributeError):
                continue

        return on_time_count, sum_delays

    def _publish_and_persist_metrics(
        self, customer_id: str, correlation_id: Optional[str] = None
    ) -> None:
        """Publish and persist metrics from cache."""
        # Get cache data INSIDE lock, release before external calls
        with self._cache_lock:
            cache = self._metrics_cache.get(customer_id)
            if cache is None:
                return
            # Copy cache data to dict (release lock ASAP)
            cache_data = dict(cache)

        # Call external QueryAgent OUTSIDE lock to avoid lock contention
        customer = self._query_agent.get_customer(customer_id)
        company_name = customer.get("name") if customer else None
        credit_limit = customer.get("credit_limit") if customer else 0

        payload = {
            "customer_id": customer_id,
            "company_name": company_name,
            "credit_limit": credit_limit,
            "total_outstanding": cache_data["total_outstanding"],
            "avg_delay": cache_data["avg_delay"],
            "on_time_ratio": cache_data["on_time_ratio"],
            "last_payment_date": cache_data.get("last_payment_date"),  # CRITICAL FIX #4
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.publish_event(
            topic=self.TOPIC_METRICS,
            event_type="customer.metrics.updated",
            entity_id=customer_id,
            payload=payload,
            correlation_id=correlation_id,
        )

        # Persist to DB
        metrics_dict = {
            "total_outstanding": payload["total_outstanding"],
            "avg_delay": payload["avg_delay"],
            "on_time_ratio": payload["on_time_ratio"],
            "last_payment_date": payload.get("last_payment_date"),  # CRITICAL FIX #4
        }
        self._persist_metrics(customer_id, metrics_dict)

        logger.info(
            f"Published metrics for customer {customer_id}: "
            f"outstanding={payload['total_outstanding']:.2f}, "
            f"avg_delay={payload['avg_delay']:.2f}, "
            f"on_time_ratio={payload['on_time_ratio']:.2f}, "
            f"last_payment={payload.get('last_payment_date')}, "
            f"company_name={company_name}"
        )

    def _compute_customer_metrics(self, customer_id: str) -> Optional[Dict[str, float]]:
        """
        Compute customer behavior metrics (full recompute).

        Used for cold start initialization only.

        Returns:
            Dict with keys: total_outstanding, avg_delay, on_time_ratio, last_payment_date
            None if error occurs
        """
        try:
            invoices = self._query_agent.get_all_invoices_by_customer(customer_id)

            if not invoices:
                logger.debug(f"No invoices found for customer {customer_id}")
                return {
                    "total_outstanding": 0.0,
                    "avg_delay": 0.0,
                    "on_time_ratio": 0.0,  # ISSUE 3 FIX: Changed from 0.5 to 0.0 (safer default)
                    "last_payment_date": None,  # CRITICAL FIX #4
                }

            invoice_ids = [
                inv.get("invoice_id") for inv in invoices if inv.get("invoice_id")
            ]

            # Get total outstanding (unpaid invoices)
            # Use remaining_amount if available (to account for partial payments)
            # Otherwise fall back to full amount
            total_outstanding = sum(
                float(inv.get("remaining_amount", inv.get("amount", 0)))
                for inv in invoices
                if inv.get("status") != "paid"
            )

            # Get payments and compute delay metrics
            payments = self._get_payments_for_invoices(invoice_ids)
            avg_delay, on_time_ratio = self._compute_payment_metrics(invoices, payments)

            # CRITICAL FIX #4: Calculate last_payment_date
            last_payment_date = None
            if payments:
                latest_payment = max(payments, key=lambda x: x.get("payment_date", ""))
                last_payment_date = latest_payment.get("payment_date")

            logger.debug(
                f"Computed initial metrics for customer {customer_id}: "
                f"{len(invoices)} invoices, "
                f"avg_delay={avg_delay:.2f}, "
                f"on_time_ratio={on_time_ratio:.2f}, "
                f"last_payment={last_payment_date}"
            )

            return {
                "total_outstanding": total_outstanding,
                "avg_delay": avg_delay,
                "on_time_ratio": on_time_ratio,
                "last_payment_date": last_payment_date,  # CRITICAL FIX #4
            }

        except Exception as e:
            logger.error(f"Error computing metrics for customer {customer_id}: {e}")
            return None

    def _get_payments_for_invoices(self, invoice_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get all payments for given invoices.

        Returns:
            List of payment dicts from database
        """
        if not invoice_ids:
            return []

        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                placeholders = ",".join("?" * len(invoice_ids))

                cursor.execute(
                    f"""
                    SELECT payment_id, invoice_id, payment_date, amount
                    FROM payments
                    WHERE invoice_id IN ({placeholders})
                """,
                    invoice_ids,
                )

                rows = cursor.fetchall()
                conn.close()

                return [dict(row) for row in rows]

            except sqlite3.Error as e:
                logger.error(f"Database error fetching payments: {e}")
                return []

    def _compute_payment_metrics(
        self, invoices: List[Dict[str, Any]], payments: List[Dict[str, Any]]
    ) -> Tuple[float, float]:
        """
        Compute avg_delay and on_time_ratio from invoices and payments.

        Handles multiple payments per invoice by using the latest payment date.

        Returns:
            Tuple of (avg_delay, on_time_ratio)
        """
        payment_map: Dict[str, List[Dict[str, Any]]] = {}
        for payment in payments:
            invoice_id = payment.get("invoice_id")
            if invoice_id:
                payment_map.setdefault(invoice_id, []).append(payment)

        paid_invoices = [inv for inv in invoices if inv.get("status") == "paid"]

        if not paid_invoices:
            return 0.0, 0.0  # ISSUE 3 FIX: Changed from 0.5 to 0.0 (safer default)

        delays = []
        on_time_count = 0

        for invoice in paid_invoices:
            invoice_id = invoice.get("invoice_id")
            due_date_str = invoice.get("due_date")
            payment_list = payment_map.get(invoice_id, [])

            if not payment_list or not due_date_str:
                continue

            try:
                latest_payment = max(
                    payment_list, key=lambda x: x.get("payment_date", "")
                )
                payment_date_str = latest_payment.get("payment_date")

                if not payment_date_str:
                    continue

                due_date = datetime.fromisoformat(due_date_str)
                payment_date = datetime.fromisoformat(payment_date_str)

                delay_days = max((payment_date - due_date).days, 0)

                delays.append(delay_days)

                if (payment_date - due_date).days <= 0:
                    on_time_count += 1

            except (ValueError, TypeError, AttributeError) as e:
                logger.debug(f"Error parsing dates for invoice {invoice_id}: {e}")
                continue

        if delays:
            avg_delay = sum(delays) / len(delays)
        else:
            avg_delay = 0.0

        if paid_invoices:
            on_time_ratio = on_time_count / len(paid_invoices)
        else:
            on_time_ratio = 0.0  # ISSUE 3 FIX: Changed from 0.5 to 0.0 (safer default)

        return avg_delay, on_time_ratio

    def _persist_metrics(
        self, customer_id: str, metrics: Dict[str, float]
    ) -> None:
        """
        Persist metrics to customer_metrics table.

        Creates or updates record.
        """
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                cursor = conn.cursor()

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO customer_metrics
                    (customer_id, total_outstanding, avg_delay, on_time_ratio, last_payment_date, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        customer_id,
                        metrics["total_outstanding"],
                        metrics["avg_delay"],
                        metrics["on_time_ratio"],
                        metrics.get("last_payment_date"),  # CRITICAL FIX #4
                        datetime.utcnow().isoformat(),
                    ),
                )

                conn.commit()
                conn.close()

                logger.debug(f"Persisted metrics for customer {customer_id}")

            except sqlite3.Error as e:
                logger.error(f"Database error persisting metrics for {customer_id}: {e}")
