import logging
import random
import sqlite3
import threading
from datetime import datetime
from typing import List, Any, Dict, Optional, Tuple, Set

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

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
    IGNORE_STALE_EVENTS_ON_STARTUP = True

    def __init__(
        self,
        kafka_client: Any,
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
        
        # Per-customer rebuild locks (FIX C)
        self._rebuild_locks: dict[str, threading.Lock] = {}

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_INVOICES, self.TOPIC_PAYMENTS]

    def process_event(self, event: Event) -> None:
        """Process incoming events and trigger bulk rebuild."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning(f"Event {event.event_type} missing customer_id")
            return

        self._rebuild_customer_metrics(customer_id, event.correlation_id)

    def _rebuild_customer_metrics(self, customer_id: str, correlation_id: Optional[str] = None) -> None:
        """
        Rebuild customer metrics locally in one pass using a bulk invoice query (FIX B).
        """
        lock = self._rebuild_locks.setdefault(customer_id, threading.Lock())
        if not lock.acquire(blocking=False):
            logger.debug(f"Skipping concurrent rebuild for {customer_id} — already in progress")
            return
            
        try:
            # FIX B: Bulk query — single call to QueryAgent
            result = QueryClient.query(
                "get_invoices_by_customer",
                {"customer_id": customer_id},
                timeout=8
            )
            
            invoice_list = result.get("invoices", []) if isinstance(result, dict) else (result or [])

            # Compute total outstanding locally
            total_outstanding = 0.0
            for inv in invoice_list:
                if inv.get("status") != "paid":
                    # Sum remaining_amount if available, else total_amount/amount
                    amount = float(inv.get("remaining_amount") or inv.get("total_amount") or inv.get("amount") or 0.0)
                    total_outstanding += max(amount, 0.0)

            # Get payments to compute delays
            invoice_ids = [inv.get("invoice_id") for inv in invoice_list if inv.get("invoice_id")]
            payments = self._get_payments_for_invoices(invoice_ids)
            
            avg_delay, on_time_ratio = self._compute_payment_metrics(invoice_list, payments)

            # Calculate last payment date
            last_payment_date = None
            if payments:
                latest_payment = max(payments, key=lambda x: x.get("payment_date", ""))
                last_payment_date = latest_payment.get("payment_date")

            # Get static customer info
            customer = QueryClient.query("get_customer", {"customer_id": customer_id})
            company_name = customer.get("name") if customer else None
            credit_limit = customer.get("credit_limit") if customer else 0

            payload = {
                "customer_id": customer_id,
                "company_name": company_name,
                "credit_limit": credit_limit,
                "total_outstanding": total_outstanding,
                "avg_delay": avg_delay,
                "on_time_ratio": on_time_ratio,
                "last_payment_date": last_payment_date,
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Publish metrics
            self.publish_event(
                topic=self.TOPIC_METRICS,
                event_type="customer.metrics.updated",
                entity_id=customer_id,
                payload=payload,
                correlation_id=correlation_id,
            )

            # Persist to DB
            metrics_dict = {
                "total_outstanding": total_outstanding,
                "avg_delay": avg_delay,
                "on_time_ratio": on_time_ratio,
                "last_payment_date": last_payment_date,
            }
            self._persist_metrics(customer_id, metrics_dict)

            logger.info(
                f"Published metrics for customer {customer_id}: "
                f"outstanding={total_outstanding:.2f}, "
                f"avg_delay={avg_delay:.2f}, "
                f"on_time_ratio={on_time_ratio:.2f}, "
                f"last_payment={last_payment_date}, "
                f"company_name={company_name}"
            )

        except Exception as e:
            logger.error(f"Error rebuilding metrics for {customer_id}: {e}")
        finally:
            lock.release()

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
                conn.execute("PRAGMA foreign_keys = ON")
                cursor = conn.cursor()

                now = datetime.utcnow().isoformat()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (customer_id, now, now),
                )
                # Immediate name backfill: resolve name from customer_risk_profile
                # if the profile event hasn't been processed by DBAgent yet.
                profile_row = cursor.execute(
                    """
                    SELECT company_name FROM customer_risk_profile
                    WHERE customer_id = ?
                      AND company_name IS NOT NULL
                      AND company_name NOT LIKE 'cust_%'
                    LIMIT 1
                    """,
                    (customer_id,),
                ).fetchone()
                if profile_row and profile_row[0]:
                    cursor.execute(
                        "UPDATE customers SET name = ?, updated_at = ? "
                        "WHERE customer_id = ? AND name IS NULL",
                        (profile_row[0], now, customer_id),
                    )

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
                        now,
                    ),
                )

                conn.commit()
                conn.close()

                logger.debug(f"Persisted metrics for customer {customer_id}")

            except sqlite3.Error as e:
                logger.error(f"Database error persisting metrics for {customer_id}: {e}")
