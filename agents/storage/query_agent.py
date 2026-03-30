import logging
import sqlite3
import threading
from typing import List, Any, Dict, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class QueryAgent(BaseAgent):
    """
    Query Agent for ACIS-X.

    Read-only agent providing query services for customer, invoice, and state data.
    Uses local cache with SQLite fallback for efficient lookups.

    This is a synchronous helper agent - it does NOT publish events for queries.
    """

    DB_PATH = "acis.db"

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
        memory_agent: Optional[Any] = None,
    ):
        super().__init__(
            agent_name="QueryAgent",
            agent_version="1.0.0",
            group_id="query-agent-group",
            subscribed_topics=[],
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

        # Reference to MemoryAgent for state queries (optional)
        self._memory_agent = memory_agent

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to (empty for query agent)."""
        return []

    def process_event(self, event: Event) -> None:
        """QueryAgent does minimal event processing."""
        pass

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
                    SELECT customer_id, credit_limit, risk_score, last_updated
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
                    logger.debug(f"Fetched customer from DB: {customer_id}")
                    return result

            except sqlite3.Error as e:
                logger.error(f"Database error querying customer {customer_id}: {e}")

        return None

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
                    SELECT invoice_id, customer_id, amount, due_date, status
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
