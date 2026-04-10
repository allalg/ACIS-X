import json
import logging
import sqlite3
import threading
import time
from datetime import datetime
from typing import List, Any, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event

logger = logging.getLogger(__name__)


class DBAgent(BaseAgent):
    """
    Database Agent for ACIS-X.

    Single DB writer that persists invoice, payment, and collection events
    to SQLite database. Handles idempotent writes and ensures data consistency.

    Subscribes to:
    - acis.invoices (invoice.created, invoice.overdue, etc.)
    - acis.payments (payment.received, payment.partial)
    - acis.collections (collection.reminder, collection.escalation, collection.action)
    - acis.customers (customer.profile.updated)
    """

    TOPIC_INVOICES = "acis.invoices"
    TOPIC_PAYMENTS = "acis.payments"
    TOPIC_COLLECTIONS = "acis.collections"
    TOPIC_CUSTOMERS = "acis.customers"
    TOPIC_METRICS = "acis.metrics"
    TOPIC_RISK = "acis.risk"

    DB_PATH = "acis.db"

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
        query_agent: Optional[Any] = None,
    ):
        super().__init__(
            agent_name="DBAgent",
            agent_version="1.0.0",
            group_id="db-agent-group",
            subscribed_topics=[
                self.TOPIC_INVOICES,
                self.TOPIC_PAYMENTS,
                self.TOPIC_COLLECTIONS,
                self.TOPIC_CUSTOMERS,
                self.TOPIC_METRICS,
                self.TOPIC_RISK,
            ],
            capabilities=[
                "db_write",
                "persistent_storage",
            ],
            kafka_client=kafka_client,
            agent_type="DBAgent",
        )

        self._db_path = db_path or self.DB_PATH
        self._db_lock = threading.Lock()
        self._query_agent = query_agent
        self._init_database()

    def set_query_agent(self, query_agent: Any) -> None:
        """Set reference to QueryAgent for cache invalidation."""
        self._query_agent = query_agent
        logger.info("QueryAgent reference set for cache invalidation")

    def _init_database(self) -> None:
        """Initialize SQLite database and create tables if not exists."""
        import time

        with self._db_lock:
            # FIX #6: Handle corrupted database by detecting and cleaning it up
            corruption_detected = False

            try:
                # First, try to detect corruption by attempting to open and parse the database
                test_conn = sqlite3.connect(self._db_path, timeout=10.0)
                test_cursor = test_conn.cursor()
                test_cursor.execute("PRAGMA integrity_check")
                integrity = test_cursor.fetchone()[0]
                test_conn.close()  # Close connection BEFORE deleting

                if integrity != "ok":
                    logger.warning(f"Database integrity check failed: {integrity}, removing corrupted file")
                    corruption_detected = True
            except sqlite3.DatabaseError as e:
                # Database is corrupted
                if "malformed" in str(e).lower():
                    logger.warning(f"Database corrupted, preparing cleanup: {e}")
                    corruption_detected = True
                test_conn = None

            # If corruption detected, delete the corrupted files
            if corruption_detected:
                import os
                for attempt in range(3):
                    try:
                        # Try to remove main file and WAL files
                        for suffix in ["", "-wal", "-shm"]:
                            filepath = self._db_path + suffix
                            if os.path.exists(filepath):
                                try:
                                    os.remove(filepath)
                                    logger.info(f"Deleted corrupted database file: {filepath}")
                                except:
                                    pass
                        break
                    except Exception as e:
                        if attempt < 2:
                            logger.debug(f"Delete attempt {attempt + 1} failed, retrying: {e}")
                            time.sleep(0.5)
                        else:
                            logger.error(f"Could not remove corrupted database after 3 attempts: {e}")
                            raise

            # Now open with fresh database (or cleaned-up file)
            # Use timeout and WAL mode for better concurrent access
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                # FIX #6: Use DEFERRED and optimized pragmas (WAL removed - was causing issues)
                # WAL mode can corrupt on older SQLite versions, so use standard mode with proper timeouts
                cursor.execute("PRAGMA synchronous=NORMAL")  # Balance safety and speed
                cursor.execute("PRAGMA cache_size=-32000")   # 32MB cache
                cursor.execute("PRAGMA temp_store=MEMORY")
                cursor.execute("PRAGMA journal_mode=TRUNCATE")  # More stable than DELETE

                # Enable foreign key constraints
                cursor.execute("PRAGMA foreign_keys = ON")

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customers (
                        customer_id TEXT PRIMARY KEY,
                        name TEXT,
                        credit_limit REAL DEFAULT 0,
                        risk_score REAL DEFAULT 0,
                        status TEXT DEFAULT 'active',
                        created_at TEXT,
                        updated_at TEXT
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS invoices (
                        invoice_id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        total_amount REAL,
                        paid_amount REAL DEFAULT 0,
                        issued_date TEXT,
                        due_date TEXT,
                        status TEXT DEFAULT 'pending',
                        created_at TEXT,
                        updated_at TEXT,
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS payments (
                        payment_id TEXT PRIMARY KEY,
                        invoice_id TEXT,
                        customer_id TEXT,
                        amount REAL,
                        payment_date TEXT,
                        created_at TEXT,
                        FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id),
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS collections_log (
                        id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        invoice_id TEXT,
                        action TEXT,
                        stage TEXT,
                        priority TEXT,
                        reason TEXT,
                        timestamp TEXT,
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customer_metrics (
                        customer_id TEXT PRIMARY KEY,
                        total_outstanding REAL DEFAULT 0,
                        avg_delay REAL DEFAULT 0,
                        on_time_ratio REAL DEFAULT 0,
                        last_payment_date TEXT,
                        updated_at TEXT,
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS external_financials (
                        company_name TEXT PRIMARY KEY,
                        pe REAL,
                        roe REAL,
                        roce REAL,
                        debt REAL,
                        market_cap REAL,
                        sales_growth REAL,
                        profit_growth REAL,
                        operating_margin REAL,
                        interest_coverage REAL,
                        risk REAL,
                        updated_at TEXT
                    )
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS external_litigation (
                        id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        company_name TEXT,
                        litigation_risk REAL,
                        severity TEXT,
                        case_count INTEGER,
                        case_types TEXT,
                        cases TEXT,
                        source TEXT,
                        confidence REAL,
                        created_at TEXT
                    )
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoice_id
                    ON invoices(invoice_id)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_invoice_customer
                    ON invoices(customer_id)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_litigation_customer
                    ON external_litigation(customer_id)
                """)

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_payments_invoice
                    ON payments(invoice_id)
                """)

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customer_risk_profile (
                        id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        company_name TEXT,
                        financial_risk REAL,
                        litigation_risk REAL,
                        combined_risk REAL,
                        severity TEXT,
                        financial_source TEXT,
                        litigation_source TEXT,
                        confidence REAL,
                        created_at TEXT
                    )
                """)

                conn.commit()
                logger.info(f"Database initialized at {self._db_path}")
                logger.info("Upgraded database schema to v5 (customer risk profile ready)")
            finally:
                conn.close()

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [
            self.TOPIC_INVOICES,
            self.TOPIC_PAYMENTS,
            self.TOPIC_COLLECTIONS,
            self.TOPIC_CUSTOMERS,
            self.TOPIC_METRICS,
            self.TOPIC_RISK,
        ]

    def process_event(self, event: Event) -> None:
        """Process incoming events and persist to database."""
        event_type = event.event_type

        if event_type.startswith("invoice."):
            self._handle_invoice_upsert(event)
        elif event_type == "payment.received":
            self._handle_payment_received(event)
        elif event_type.startswith("collection."):
            self._handle_collection_action(event)
        elif event_type == "customer.profile.updated":
            self._handle_customer_profile(event)
        elif event_type == "external.litigation.updated":  # FIX: standardized name
            self._handle_litigation_event(event)
        elif event_type == "risk.profile.updated":  # FIX: standardized name
            self._handle_customer_risk_profile(event)

    def _handle_invoice_upsert(self, event: Event) -> None:
        """
        Handle all invoice.* events using UPSERT logic.
        Supports:
        - invoice.created
        - invoice.overdue
        - invoice.disputed
        - invoice.cancelled
        """
        data = event.payload or {}
        invoice_id = data.get("invoice_id")
        customer_id = data.get("customer_id")
        total_amount = data.get("amount") or data.get("total_amount")  # Accept both field names
        due_date = data.get("due_date")
        status = data.get("status", "pending")
        issued_date = data.get("created_at") or data.get("issued_date")
        now = datetime.utcnow().isoformat()

        if not invoice_id:
            logger.warning("Invoice event missing invoice_id, skipping")
            return

        if not issued_date:
            issued_date = now

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO invoices (
                        invoice_id,
                        customer_id,
                        total_amount,
                        paid_amount,
                        issued_date,
                        due_date,
                        status,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(invoice_id) DO UPDATE SET
                        customer_id=excluded.customer_id,
                        total_amount=excluded.total_amount,
                        due_date=excluded.due_date,
                        status=excluded.status,
                        updated_at=excluded.updated_at
                """, (
                    invoice_id,
                    customer_id,
                    total_amount,
                    0,  # Initialize paid_amount to 0
                    issued_date,
                    due_date,
                    status,
                    now,
                    now
                ))

                # Ensure customer exists
                if customer_id:
                    cursor.execute("""
                        INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                    """, (customer_id, now, now))

                conn.commit()
                logger.info(f"[DBAgent] Upserted invoice: {invoice_id} status={status} total_amount={total_amount}")

                # FIX #2: Pre-populate cache instead of just invalidating
                # This prevents cache misses for agents querying immediately after write
                if self._query_agent:
                    invoice_cache_data = {
                        "invoice_id": invoice_id,
                        "customer_id": customer_id,
                        "total_amount": total_amount,
                        "paid_amount": 0,
                        "remaining_amount": total_amount,
                        "due_date": due_date,
                        "status": status,
                    }
                    self._query_agent.update_invoice_cache(invoice_id, invoice_cache_data)
                    if customer_id:
                        self._query_agent.invalidate_customer_cache(customer_id)
            finally:
                conn.close()

    def _handle_payment_received(self, event: Event) -> None:
        """Handle payment.received event - insert payment and update invoice paid_amount and status."""
        data = event.payload or {}
        payment_id = data.get("payment_id")
        invoice_id = data.get("invoice_id")
        amount = data.get("amount")
        payment_date = data.get("payment_date") or datetime.utcnow().isoformat()
        now = datetime.utcnow().isoformat()

        if not payment_id:
            logger.warning("payment.received event missing payment_id, skipping")
            return

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                # Resolve customer_id from invoice if not provided
                # WITH RETRY: payment may arrive before invoice insert (race condition)
                customer_id = data.get("customer_id")
                if not customer_id and invoice_id:
                    for attempt in range(3):
                        cursor.execute(
                            "SELECT customer_id FROM invoices WHERE invoice_id = ?",
                            (invoice_id,)
                        )
                        row = cursor.fetchone()
                        if row:
                            customer_id = row[0]
                            logger.info(f"[DBAgent] Resolved customer_id from invoice: {invoice_id} -> {customer_id}")
                            break
                        elif attempt < 2:
                            logger.warning(f"[DBAgent] Invoice {invoice_id} not found, retrying... (attempt {attempt + 1}/3)")
                            time.sleep(0.1)

                    if not customer_id:
                        logger.error(f"[DBAgent] Could not find invoice {invoice_id} after 3 retries, cannot process payment")
                        return

                # Insert payment (idempotent)
                cursor.execute("""
                    INSERT OR IGNORE INTO payments (
                        payment_id,
                        invoice_id,
                        customer_id,
                        amount,
                        payment_date,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (payment_id, invoice_id, customer_id, amount, payment_date, now))

                if cursor.rowcount > 0:
                    logger.info(f"[DBAgent] Inserted payment: {payment_id} for invoice: {invoice_id}, amount={amount}")

                    # Update invoice paid_amount and determine status
                    if invoice_id:
                        # Get current invoice state
                        cursor.execute(
                            "SELECT total_amount, paid_amount FROM invoices WHERE invoice_id = ?",
                            (invoice_id,)
                        )
                        invoice_row = cursor.fetchone()
                        if invoice_row:
                            total_amount = invoice_row[0]
                            current_paid = invoice_row[1] or 0
                            new_paid = current_paid + (amount or 0)

                            # Determine status based on remaining amount
                            remaining = total_amount - new_paid if total_amount else 0
                            if remaining <= 0:
                                status = "paid"
                            elif new_paid > 0:
                                status = "partial"
                            else:
                                status = "pending"

                            cursor.execute("""
                                UPDATE invoices
                                SET paid_amount = ?, status = ?, updated_at = ?
                                WHERE invoice_id = ?
                            """, (new_paid, status, now, invoice_id))

                            logger.info(
                                f"[DBAgent] Updated invoice: {invoice_id} paid={new_paid} remaining={remaining} status={status}"
                            )
                        else:
                            logger.warning(f"[DBAgent] Invoice {invoice_id} not found for payment update")
                else:
                    logger.debug(f"[DBAgent] Payment {payment_id} already exists, skipped")

                conn.commit()

                # FIX #2: Pre-populate cache instead of just invalidating
                if self._query_agent:
                    if invoice_id:
                        # After payment, invoice state has changed (paid_amount, status)
                        # Invalidate to force refresh on next read
                        self._query_agent.invalidate_invoice_cache(invoice_id)
                    if customer_id:
                        self._query_agent.invalidate_customer_cache(customer_id)
            finally:
                conn.close()

    def _handle_collection_action(self, event: Event) -> None:
        """Handle all collection.* events - insert into collections_log."""
        data = event.payload or {}
        collection_id = data.get("id") or data.get("collection_id") or event.event_id
        customer_id = data.get("customer_id")
        invoice_id = data.get("invoice_id")

        # Map action_type or action field
        action = data.get("action") or data.get("action_type") or event.event_type

        # Map stage from event type if not in payload
        stage = data.get("stage") or event.event_type

        # New fields for analytics
        priority = data.get("priority")
        reason = data.get("reason")
        timestamp = data.get("timestamp") or datetime.utcnow().isoformat()

        if not collection_id:
            logger.warning(f"{event.event_type} event missing id, skipping")
            return

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR IGNORE INTO collections_log (
                        id,
                        customer_id,
                        invoice_id,
                        action,
                        stage,
                        priority,
                        reason,
                        timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (collection_id, customer_id, invoice_id, action, stage, priority, reason, timestamp))
                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(
                        f"[DBAgent] Inserted collection log: {collection_id} for customer: {customer_id}, "
                        f"action: {action}, priority: {priority}, reason: {reason}"
                    )
                else:
                    logger.debug(f"[DBAgent] Collection log {collection_id} already exists, skipped")

                # Invalidate caches
                if self._query_agent and customer_id:
                    self._query_agent.invalidate_customer_cache(customer_id)
            finally:
                conn.close()

    def _handle_customer_profile(self, event: Event) -> None:
        """Handle customer.profile.updated event - upsert customer profile data."""
        data = event.payload or {}
        customer_id = data.get("customer_id")

        if not customer_id:
            logger.warning("customer.profile.updated event missing customer_id, skipping")
            return

        # CRITICAL FIX #7: Only update name if provided (don't overwrite with NULL)
        # CustomerProfileAgent publishes events without customer_name - we should preserve existing names
        name = data.get("customer_name") or data.get("name")
        risk_score = data.get("risk_score", 0.0)
        credit_limit = data.get("credit_limit", 0.0)
        status = data.get("status", "active")
        now = datetime.utcnow().isoformat()

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                # FIX #7: Use conditional UPDATE for name field
                # If name is provided, update it; if not, preserve existing value
                if name:
                    # Name is provided - update it
                    cursor.execute("""
                        INSERT INTO customers (customer_id, name, risk_score, credit_limit, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(customer_id) DO UPDATE SET
                            name=excluded.name,
                            risk_score=excluded.risk_score,
                            credit_limit=excluded.credit_limit,
                            status=excluded.status,
                            updated_at=excluded.updated_at
                    """, (customer_id, name, risk_score, credit_limit, status, now, now))
                else:
                    # Name not provided - preserve existing value with COALESCE
                    cursor.execute("""
                        INSERT INTO customers (customer_id, name, risk_score, credit_limit, status, created_at, updated_at)
                        VALUES (?, NULL, ?, ?, ?, ?, ?)
                        ON CONFLICT(customer_id) DO UPDATE SET
                            name=COALESCE(excluded.name, customers.name),
                            risk_score=excluded.risk_score,
                            credit_limit=excluded.credit_limit,
                            status=excluded.status,
                            updated_at=excluded.updated_at
                    """, (customer_id, risk_score, credit_limit, status, now, now))

                conn.commit()
                log_name = name if name else "(preserved existing)"
                logger.info(f"[DBAgent] Updated customer profile: {customer_id} name={log_name} risk={risk_score} status={status} limit={credit_limit}")

                # FIX #2: Pre-populate customer cache after successful DB write
                if self._query_agent:
                    customer_cache_data = {
                        "customer_id": customer_id,
                        "credit_limit": credit_limit,
                        "risk_score": risk_score,
                        "updated_at": now,
                    }
                    self._query_agent.update_customer_cache(customer_id, customer_cache_data)
            finally:
                conn.close()

    def _handle_litigation_event(self, event: Event) -> None:
        """Handle LitigationRiskUpdated event - insert litigation risk data."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        company_name = data.get("company_name")
        litigation_risk = data.get("litigation_risk", 0.0)
        severity = data.get("severity")
        case_count = data.get("case_count", 0)
        case_types = data.get("case_types", [])
        cases = data.get("cases", [])
        source = data.get("source")
        confidence = data.get("confidence", 0.0)
        created_at = datetime.utcnow().isoformat()

        if not customer_id:
            logger.warning("LitigationRiskUpdated event missing customer_id, skipping")
            return

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                # Ensure customer exists
                cursor.execute("""
                    INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                    VALUES (?, ?, ?)
                """, (customer_id, created_at, created_at))

                cursor.execute("""
                    INSERT OR IGNORE INTO external_litigation (
                        id,
                        customer_id,
                        company_name,
                        litigation_risk,
                        severity,
                        case_count,
                        case_types,
                        cases,
                        source,
                        confidence,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    customer_id,
                    company_name,
                    litigation_risk,
                    severity,
                    case_count,
                    json.dumps(case_types or []),
                    json.dumps(cases or []),
                    source,
                    confidence,
                    created_at
                ))
                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(
                        f"[DBAgent] Stored litigation: customer={customer_id}, "
                        f"risk={litigation_risk}, cases={case_count}"
                    )
                else:
                    logger.debug(f"[DBAgent] Litigation record {event.event_id} already exists, skipped")

                # Invalidate cache
                if self._query_agent:
                    self._query_agent.invalidate_customer_cache(customer_id)
            finally:
                conn.close()

    def _handle_customer_risk_profile(self, event: Event) -> None:
        """Handle CustomerRiskProfileUpdated event - insert aggregated risk data."""
        data = event.payload or {}

        customer_id = data.get("customer_id")
        company_name = data.get("company_name")

        if not customer_id:
            logger.warning("CustomerRiskProfileUpdated missing customer_id, skipping")
            return

        financial_risk = data.get("financial_risk", 0.0)
        litigation_risk = data.get("litigation_risk", 0.0)
        combined_risk = data.get("combined_risk", 0.0)

        severity = data.get("severity")
        financial_source = data.get("financial_source")
        litigation_source = data.get("litigation_source")

        confidence = data.get("confidence", 0.0)
        created_at = datetime.utcnow().isoformat()

        with self._db_lock:
            conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT OR IGNORE INTO customer_risk_profile (
                        id,
                        customer_id,
                        company_name,
                        financial_risk,
                        litigation_risk,
                        combined_risk,
                        severity,
                        financial_source,
                        litigation_source,
                        confidence,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    customer_id,
                    company_name,
                    financial_risk,
                    litigation_risk,
                    combined_risk,
                    severity,
                    financial_source,
                    litigation_source,
                    confidence,
                    created_at
                ))

                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(
                        f"[DBAgent] Stored aggregated risk: customer={customer_id}, combined={combined_risk}"
                    )
                else:
                    logger.debug(
                        f"[DBAgent] Risk profile {event.event_id} already exists, skipped"
                    )

                # Invalidate cache
                if self._query_agent:
                    self._query_agent.invalidate_customer_cache(customer_id)

            finally:
                conn.close()
