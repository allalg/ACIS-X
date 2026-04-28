import logging
import sqlite3
import threading
from typing import List, Any, Dict, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

logger = logging.getLogger(__name__)


class QueryAgent(BaseAgent):
    """
    Query Agent for ACIS-X.

    Read-only agent providing query services for customer, invoice, and state data.
    Uses local cache with SQLite fallback for efficient lookups.

    This is a synchronous helper agent - it does NOT publish events for queries.
    """

    DB_PATH = "acis.db"
    # Replay from earliest so the invoice cache is correctly populated after
    # a restart without requiring a manual cache warm-up step.
    OFFSET_RESET = "earliest"

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
    ):
        super().__init__(
            agent_name="QueryAgent",
            agent_version="1.0.0",
            group_id="query-agent-group",
            subscribed_topics=["acis.query.request", "acis.invoices"],
            capabilities=[
                "query_service",
                "read_access",
            ],
            kafka_client=kafka_client,
            agent_type="QueryAgent",
        )

        self._db_path = db_path or self.DB_PATH
        self._db_lock = threading.Lock()

        # Local cache
        self._customer_cache: Dict[str, Dict[str, Any]] = {}
        self._invoice_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_lock = threading.Lock()

        # Tracks which customer IDs have had ALL their invoices loaded from the DB
        # so that a partial cache warm (from individual get_invoice() calls) does
        # not suppress the full bulk-load in get_invoices_by_customer().
        self._loaded_customer_ids: set = set()

        # Reference to MemoryAgent for state queries (optional, set via set_memory_agent())
        self._memory_agent = None

    def set_memory_agent(self, agent: Any) -> None:
        """
        Wire a MemoryAgent instance for in-process state lookups.

        Must be called after both QueryAgent and MemoryAgent are constructed
        (only effective when both run in the same process).

        Args:
            agent: A MemoryAgent instance, or None to clear the reference.
        """
        self._memory_agent = agent
        logger.info(
            "[QueryAgent] MemoryAgent reference %s",
            "set" if agent is not None else "cleared",
        )

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return ["acis.query.request", "acis.invoices"]

    def process_event(self, event: Event) -> None:
        """Process query requests and publish responses."""
        if event.event_type in [
            "invoice.created",
            "invoice.updated",
            "invoice.overdue",
            "invoice.disputed",
            "invoice.cancelled",
        ]:
            payload = event.payload or {}
            invoice_id = payload.get("invoice_id")
            if invoice_id:
                self.update_invoice_cache(invoice_id, payload)
            return

        if event.event_type == "query.request":
            payload = event.payload or {}
            query_type = payload.get("query_type")
            data = payload.get("data", {})
            
            response_data = None
            if query_type == "get_customer":
                response_data = self.get_customer(data.get("customer_id"))
            elif query_type == "get_customer_metrics":
                response_data = self.get_customer_metrics(data.get("customer_id"))
            elif query_type == "get_all_customers":
                response_data = self.get_all_customers()
            elif query_type == "get_invoice":
                response_data = self.get_invoice(data.get("invoice_id"))
            elif query_type == "get_invoices_by_customer":
                response_data = self.get_invoices_by_customer(data.get("customer_id"))
            elif query_type == "get_all_invoices_by_customer":
                response_data = self.get_all_invoices_by_customer(data.get("customer_id"))
            elif query_type == "get_overdue_invoices":
                response_data = self.get_overdue_invoices(data.get("customer_id"))
            elif query_type == "get_unpaid_invoices":
                response_data = self.get_unpaid_invoices()
            elif query_type == "update_customer_cache":
                response_data = self.update_customer_cache(data.get("customer_id"), data.get("customer_data"))
            elif query_type == "update_invoice_cache":
                response_data = self.update_invoice_cache(data.get("invoice_id"), data.get("invoice_data"))
            elif query_type == "invalidate_customer_cache":
                response_data = self.invalidate_customer_cache(data.get("customer_id"))
            elif query_type == "invalidate_invoice_cache":
                response_data = self.invalidate_invoice_cache(data.get("invoice_id"))
            else:
                if query_type == "get_risk_velocity":
                    logger.debug("Skipping MemoryAgent-owned query_type: get_risk_velocity")
                    return
                logger.warning(f"Unknown query_type: {query_type}")
                return
                
            response_payload = {
                "query_type": query_type,
                "data": response_data
            }
            
            self.publish_event(
                topic="acis.query.response",
                event_type="query.response",
                entity_id=event.entity_id,
                payload=response_payload,
                correlation_id=event.correlation_id
            )

    def start(self) -> None:
        """
        Start QueryAgent with full BaseAgent lifecycle.

        QueryAgent remains producer-only because subscribe() returns an empty list,
        but it should still register, emit heartbeats, and clean up consistently.
        """
        super().start()
        logger.info("QueryAgent started (read-only mode, producer-only lifecycle)")

    def get_customer(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get customer data by ID.

        First checks local cache, then queries SQLite DB.

        Args:
            customer_id: The customer ID to look up.

        Returns:
            Customer data dict or None if not found.
        """
        if not customer_id:
            return None

        # Check cache first
        with self._cache_lock:
            if customer_id in self._customer_cache:
                logger.debug(f"Cache hit for customer: {customer_id}")
                return self._customer_cache[customer_id]

        # Query database
        logger.debug(f"Cache miss for customer: {customer_id}, querying database")
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT customer_id, name, credit_limit, risk_score, updated_at
                    FROM customers
                    WHERE customer_id = ?
                """, (customer_id,))

                row = cursor.fetchone()
                conn.close()

                if row:
                    result = dict(row)
                    # Update cache
                    with self._cache_lock:
                        self._customer_cache[customer_id] = result
                    return result
                return None
            except Exception as e:
                logger.error(f"Error querying customer {customer_id}: {e}")
                return None



    def get_customer_metrics(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get ENRICHED customer data with computed metrics for risk scoring.

        ISSUE 1 FIX: Tries MemoryAgent FIRST (real-time cache layer)
        ISSUE 2 FIX: Establishes clear source of truth hierarchy:
        - MemoryAgent = REAL-TIME source of truth (primary)
        - DB = PERSISTENCE layer (fallback)

        Returns:
            Dict with:
            - customer_id, credit_limit, risk_score (from customers)
            - total_outstanding, avg_delay, on_time_ratio (from memory/metrics)
            - overdue_count (from memory/computed from invoices)
        """
        if not customer_id:
            return None

        # ISSUE 1 FIX: Try MemoryAgent FIRST (real-time cache)
        if self._memory_agent is not None:
            try:
                memory_state = self._memory_agent.get_customer_state(customer_id)
                if memory_state:
                    logger.debug(f"[QueryAgent] Using MemoryAgent cache for {customer_id} (real-time source)")
                    # Enrich with static data from DB (credit_limit only changes during administrative action)
                    customer_static = self.get_customer(customer_id)
                    if customer_static:
                        return {
                            "customer_id": customer_id,
                            "credit_limit": customer_static.get("credit_limit", 0.0),
                            "risk_score": memory_state.get("risk_score", 0.0),
                            "total_outstanding": memory_state.get("total_outstanding", 0.0),
                            "avg_delay": memory_state.get("avg_delay", 0.0),
                            "on_time_ratio": memory_state.get("on_time_ratio", 0.5),
                            "overdue_count": memory_state.get("overdue_count", 0),
                        }
            except Exception as e:
                logger.debug(f"[QueryAgent] MemoryAgent lookup failed for {customer_id}: {e}, falling back to DB")

        # FALLBACK: Use DB (when MemoryAgent unavailable)
        logger.debug(f"[QueryAgent] Falling back to DB for {customer_id} (MemoryAgent unavailable)")
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                # Get customer base + metrics
                cursor.execute("""
                    SELECT
                        c.customer_id,
                        c.name,
                        c.credit_limit,
                        c.risk_score,
                        COALESCE(m.total_outstanding, 0.0) as total_outstanding,
                        COALESCE(m.avg_delay, 0.0) as avg_delay,
                        COALESCE(m.on_time_ratio, 0.5) as on_time_ratio,
                        m.last_payment_date,
                        m.updated_at
                    FROM customers c
                    LEFT JOIN customer_metrics m ON c.customer_id = m.customer_id
                    WHERE c.customer_id = ?
                """, (customer_id,))

                row = cursor.fetchone()
                if not row:
                    conn.close()
                    return None

                result = dict(row)

                # Compute overdue_count from invoices
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM invoices
                    WHERE customer_id = ? AND status = 'overdue'
                """, (customer_id,))

                overdue_row = cursor.fetchone()
                result["overdue_count"] = overdue_row["count"] if overdue_row else 0

                conn.close()

                logger.debug(
                    f"[QueryAgent] Fetched enriched metrics from DB for {customer_id}: "
                    f"outstanding={result['total_outstanding']:.2f}, "
                    f"overdue={result['overdue_count']}, "
                    f"on_time_ratio={result['on_time_ratio']:.2f}"
                )
                return result

            except sqlite3.Error as e:
                logger.error(f"Database error fetching metrics for {customer_id}: {e}")

        return None

    def get_all_customers(self) -> List[Dict[str, Any]]:
        """
        Get all customers from DB for startup rebuild.

        Returns:
            List of customer dicts with customer_id, name, credit_limit, risk_score, updated_at
        """
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT customer_id, name, credit_limit, risk_score, updated_at
                    FROM customers
                    ORDER BY customer_id
                """)

                rows = cursor.fetchall()
                conn.close()

                result = [dict(row) for row in rows]
                logger.info(f"Fetched {len(result)} customers from DB for rebuild")
                return result

            except sqlite3.Error as e:
                logger.error(f"Database error querying all customers: {e}")

        return []

    def get_invoice(self, invoice_id: str) -> Optional[Dict[str, Any]]:
        """
        Get invoice data by ID.

        First checks local cache, then queries SQLite DB.

        Args:
            invoice_id: The invoice ID to look up.

        Returns:
            Invoice data dict or None if not found.
        """
        if not invoice_id:
            return None

        # Check cache first
        with self._cache_lock:
            if invoice_id in self._invoice_cache:
                logger.debug(f"Cache hit for invoice: {invoice_id}")
                return self._invoice_cache[invoice_id]

        # Query database
        logger.debug(f"Cache miss for invoice: {invoice_id}, querying database")
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        invoice_id,
                        customer_id,
                        COALESCE(total_amount, 0.0) AS total_amount,
                        COALESCE(paid_amount, 0.0) AS paid_amount,
                        CASE
                            WHEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0)) > 0
                            THEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0))
                            ELSE 0.0
                        END AS remaining_amount,
                        due_date,
                        status
                    FROM invoices
                    WHERE invoice_id = ?
                """, (invoice_id,))

                row = cursor.fetchone()
                conn.close()

                if row:
                    result = dict(row)
                    # Update cache
                    with self._cache_lock:
                        self._invoice_cache[invoice_id] = result
                    logger.debug(f"Fetched invoice from DB: {invoice_id}")
                    return result

            except sqlite3.Error as e:
                logger.error(f"Database error querying invoice {invoice_id}: {e}")

        return None

    def get_invoices_by_customer(self, customer_id: str) -> Dict[str, Any]:
        """
        Get all invoices for a customer.

        Checks the in-memory cache first.  If the cache holds no invoices for
        this customer (e.g. on cold start or after a cache invalidation) the
        method falls back to the database via _load_customer_invoices_from_db(),
        which also warms the cache for subsequent calls.

        Args:
            customer_id: The customer ID to look up.

        Returns:
            Dict with key ``invoices`` containing a list of invoice dicts.
        """
        if not customer_id:
            return {"invoices": []}

        with self._cache_lock:
            invoices = [
                inv_data for inv_data in self._invoice_cache.values()
                if inv_data.get("customer_id") == customer_id
            ]

        if not invoices:
            # Cache empty for this customer — could be cold start or a full
            # cache invalidation.  Load all invoices from DB.
            needs_db_load = True
        else:
            # Cache may be partially warm (e.g. from individual get_invoice()
            # calls) but we haven't yet done a full bulk-load for this customer.
            with self._cache_lock:
                needs_db_load = customer_id not in self._loaded_customer_ids

        if needs_db_load:
            logger.debug(
                f"[QueryAgent] Bulk-loading invoices from DB for customer {customer_id}"
            )
            self._load_customer_invoices_from_db(customer_id)
            with self._cache_lock:
                invoices = [
                    inv_data for inv_data in self._invoice_cache.values()
                    if inv_data.get("customer_id") == customer_id
                ]

        logger.debug(f"Fetched {len(invoices)} invoices for customer {customer_id}")
        return {"invoices": invoices}

    def _load_customer_invoices_from_db(self, customer_id: str) -> None:
        """
        Load all invoices for *customer_id* from the database into _invoice_cache.

        Uses the same SQL projection as get_all_invoices_by_customer() so that
        every entry in the cache carries the computed ``remaining_amount`` column.
        Existing cache entries for other customers are preserved.

        This method is intentionally side-effect-only; callers read the cache
        themselves after calling it.

        Args:
            customer_id: The customer whose invoices should be fetched and cached.
        """
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        invoice_id,
                        customer_id,
                        COALESCE(total_amount, 0.0) AS total_amount,
                        COALESCE(paid_amount, 0.0) AS paid_amount,
                        CASE
                            WHEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0)) > 0
                            THEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0))
                            ELSE 0.0
                        END AS remaining_amount,
                        due_date,
                        status
                    FROM invoices
                    WHERE customer_id = ?
                    ORDER BY due_date DESC
                """, (customer_id,))

                rows = cursor.fetchall()
                conn.close()

                if rows:
                    with self._cache_lock:
                        for row in rows:
                            record = dict(row)
                            self._invoice_cache[record["invoice_id"]] = record

                # Mark this customer as fully loaded regardless of row count
                # (a customer with zero invoices should not be re-queried).
                with self._cache_lock:
                    self._loaded_customer_ids.add(customer_id)

                logger.debug(
                    f"[QueryAgent] Loaded {len(rows)} invoices from DB into cache "
                    f"for customer {customer_id}"
                )

            except sqlite3.Error as e:
                logger.error(
                    f"[QueryAgent] DB error loading invoices for customer {customer_id}: {e}"
                )

    def get_all_invoices_by_customer(self, customer_id: str) -> List[Dict[str, Any]]:
        """
        Get all invoices for a customer (paid and unpaid).

        Args:
            customer_id: The customer ID to look up.

        Returns:
            List of invoice dicts, or empty list if none found.
        """
        if not customer_id:
            return []

        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        invoice_id,
                        customer_id,
                        COALESCE(total_amount, 0.0) AS total_amount,
                        COALESCE(paid_amount, 0.0) AS paid_amount,
                        CASE
                            WHEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0)) > 0
                            THEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0))
                            ELSE 0.0
                        END AS remaining_amount,
                        due_date,
                        status
                    FROM invoices
                    WHERE customer_id = ?
                    ORDER BY due_date DESC
                """, (customer_id,))

                rows = cursor.fetchall()
                conn.close()

                result = [dict(row) for row in rows]
                logger.debug(f"Fetched {len(result)} total invoices for customer {customer_id}")
                return result

            except sqlite3.Error as e:
                logger.error(f"Database error querying all invoices for customer {customer_id}: {e}")

        return []

    def get_overdue_invoices(self, customer_id: str) -> List[Dict[str, Any]]:
        """
        Get all overdue invoices for a customer.

        Args:
            customer_id: The customer ID to look up.

        Returns:
            List of invoice dicts with status='overdue', or empty list if none found.
        """
        if not customer_id:
            return []

        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        invoice_id,
                        customer_id,
                        COALESCE(total_amount, 0.0) AS total_amount,
                        COALESCE(paid_amount, 0.0) AS paid_amount,
                        CASE
                            WHEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0)) > 0
                            THEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0))
                            ELSE 0.0
                        END AS remaining_amount,
                        due_date,
                        status
                    FROM invoices
                    WHERE customer_id = ?
                    AND status = 'overdue'
                """, (customer_id,))

                rows = cursor.fetchall()
                conn.close()

                result = [dict(row) for row in rows]
                logger.debug(f"Fetched {len(result)} overdue invoices for customer {customer_id}")
                return result

            except sqlite3.Error as e:
                logger.error(f"Database error querying overdue invoices for customer {customer_id}: {e}")

        return []

    def get_unpaid_invoices(self) -> List[Dict[str, Any]]:
        """
        Fetch all pending invoices from DB.

        Returns:
            List[dict]: Each dict contains:
                - invoice_id
                - customer_id
                - total_amount
                - paid_amount
                - remaining_amount (computed)
                - due_date
                - status
        """
        with self._db_lock:
            try:
                conn = sqlite3.connect(self._db_path)
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT
                        invoice_id,
                        customer_id,
                        COALESCE(total_amount, 0.0) AS total_amount,
                        COALESCE(paid_amount, 0.0) AS paid_amount,
                        CASE
                            WHEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0)) > 0
                            THEN (COALESCE(total_amount, 0.0) - COALESCE(paid_amount, 0.0))
                            ELSE 0.0
                        END AS remaining_amount,
                        due_date,
                        status
                    FROM invoices
                    WHERE status = 'pending'
                """)

                rows = cursor.fetchall()
                conn.close()

                invoices = []
                for row in rows:
                    invoices.append({
                        "invoice_id": row[0],
                        "customer_id": row[1],
                        "total_amount": row[2],
                        "paid_amount": row[3],
                        "remaining_amount": row[4],
                        "due_date": row[5],
                        "status": row[6],
                    })

                logger.debug(f"Fetched {len(invoices)} unpaid invoices from DB")
                return invoices

            except sqlite3.Error as e:
                logger.error(f"Database error querying unpaid invoices: {e}")

        return []

    def get_customer_state(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get customer state (in-memory derived state).

        First checks MemoryAgent if available, then falls back to DB.

        Args:
            customer_id: The customer ID to look up.

        Returns:
            Customer state dict or None if not found.
        """
        if not customer_id:
            return None

        # Check MemoryAgent first if available
        if self._memory_agent is not None:
            state = self._memory_agent.get_customer_state(customer_id)
            if state:
                logger.debug(f"Got customer state from MemoryAgent: {customer_id}")
                return state

        # Fall back to DB for customer record
        logger.info(f"Falling back from MemoryAgent cache to DB for customer state: {customer_id}")
        customer = self.get_customer(customer_id)
        if customer:
            return {
                "customer_id": customer_id,
                "total_outstanding": 0.0,  # Not tracked in customers table
                "risk_score": customer.get("risk_score", 0.0),
                "last_updated": customer.get("last_updated"),
            }

        return None

    def invalidate_customer_cache(self, customer_id: str) -> None:
        """Invalidate cached customer data."""
        with self._cache_lock:
            self._customer_cache.pop(customer_id, None)
        logger.debug(f"Invalidated customer cache: {customer_id}")

    def invalidate_invoice_cache(self, invoice_id: str) -> None:
        """Invalidate cached invoice data."""
        with self._cache_lock:
            self._invoice_cache.pop(invoice_id, None)
        logger.debug(f"Invalidated invoice cache: {invoice_id}")

    def clear_cache(self) -> None:
        """Clear all cached data."""
        with self._cache_lock:
            self._customer_cache.clear()
            self._invoice_cache.clear()
        logger.info("Cleared all query agent caches")

    def update_customer_cache(self, customer_id: str, customer_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update or invalidate customer cache.
        If customer_data is provided, it pre-populates the cache.
        Otherwise, it invalidates and fetches fresh data.
        """
        if not customer_id:
            return False
            
        with self._cache_lock:
            if customer_data:
                self._customer_cache[customer_id] = customer_data
                logger.debug(f"[FIX #2] Pre-populated customer cache for {customer_id}")
            else:
                self._customer_cache.pop(customer_id, None)
                
        if not customer_data:
            self.get_customer(customer_id)
            
        return True

    def update_invoice_cache(self, invoice_id: str, invoice_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Update or invalidate invoice cache.
        If invoice_data is provided, it pre-populates the cache.
        Otherwise, it invalidates and fetches fresh data.
        """
        if not invoice_id:
            return False
            
        with self._cache_lock:
            if invoice_data:
                existing = self._invoice_cache.get(invoice_id, {})
                self._invoice_cache[invoice_id] = {**existing, **invoice_data}
                logger.debug(f"[FIX #2] Pre-populated invoice cache for {invoice_id}")
            else:
                self._invoice_cache.pop(invoice_id, None)
                
        if not invoice_data:
            self.get_invoice(invoice_id)
            
        return True
