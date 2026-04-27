import json
import logging
import re
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
    UNIT_SUFFIX_PATTERN = re.compile(r"\s*-\s*Unit\s+\d+\b.*$", re.IGNORECASE)

    # Idempotency: bounded in-memory set of processed risk profile event IDs
    MAX_PROCESSED_IDS = 20000

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
        query_agent: Optional[Any] = None,
    ):
        super().__init__(
            agent_name="DBAgent",
            agent_version="2.0.0",  # Bumped: null-safe UPSERT + idempotency
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

        # Idempotency: track processed risk profile event IDs (bounded OrderedDict)
        from collections import OrderedDict
        self._processed_risk_events: OrderedDict = OrderedDict()

        self._init_database()

    def set_query_agent(self, query_agent: Any) -> None:
        """Set reference to QueryAgent for cache invalidation."""
        self._query_agent = query_agent
        logger.info("QueryAgent reference set for cache invalidation")

    def _get_connection(self) -> sqlite3.Connection:
        """Create a DB connection with FK enforcement enabled."""
        conn = sqlite3.connect(self._db_path, timeout=30.0, isolation_level="DEFERRED")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

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
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Enable WAL mode for concurrent reads while DBAgent is writing.
                # Multiple agents (MemoryAgent, CustomerStateAgent, QueryAgent) read
                # from the same DB; WAL lets them do so without waiting for writes.
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")  # Balance safety and speed
                cursor.execute("PRAGMA cache_size=-32000")   # 32MB cache
                cursor.execute("PRAGMA temp_store=MEMORY")

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
                # Unique index on company name — prevents same company appearing
                # under multiple customer IDs even if ScenarioGenerator retries.
                cursor.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_name_unique
                    ON customers (name)
                    WHERE name IS NOT NULL
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
                        customer_id TEXT PRIMARY KEY,
                        id TEXT UNIQUE,
                        company_name TEXT,
                        litigation_risk REAL,
                        severity TEXT,
                        case_count INTEGER,
                        case_types TEXT,
                        cases TEXT,
                        evidence TEXT,
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
                        customer_id TEXT PRIMARY KEY,
                        id TEXT UNIQUE,
                        company_name TEXT,
                        financial_risk REAL,
                        litigation_risk REAL,
                        combined_risk REAL,
                        severity TEXT,
                        financial_source TEXT,
                        litigation_source TEXT,
                        confidence REAL,
                        created_at TEXT,
                        updated_at TEXT
                    )
                """)

                conn.commit()
                self._cleanup_legacy_company_names(conn)
                self._repair_payment_integrity(conn)
                logger.info(f"Database initialized at {self._db_path}")
                logger.info("Upgraded database schema to v5 (customer risk profile ready)")
            finally:
                conn.close()

    def _repair_payment_integrity(self, conn: sqlite3.Connection) -> None:
        """
        Repair legacy payment/invoice integrity issues.

        - Backfill placeholder invoices for orphan payments.
        - Recompute invoice paid_amount/status from payment records.
        - Clamp paid_amount to total_amount to avoid negative remaining balance drift.
        """
        cursor = conn.cursor()
        now = datetime.utcnow().isoformat()

        # Ensure all customers referenced by payments exist before any invoice backfill.
        # Use a SELECT-based approach to avoid creating stub rows for customers
        # that do not yet have a profile — on a fresh DB, payments table is empty
        # so this is a no-op.  We use DEFERRED FK mode for this insert only.
        payment_customer_ids = cursor.execute(
            "SELECT DISTINCT customer_id FROM payments WHERE customer_id IS NOT NULL"
        ).fetchall()
        for (cid,) in payment_customer_ids:
            cursor.execute(
                "INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at) VALUES (?, ?, ?)",
                (cid, now, now),
            )
            self._backfill_customer_name(conn, cid)

        # 1) Create placeholder invoices for orphan payments to preserve historical payments.
        cursor.execute(
            """
            INSERT INTO invoices (
                invoice_id, customer_id, total_amount, paid_amount, issued_date, due_date, status, created_at, updated_at
            )
            SELECT
                p.invoice_id,
                COALESCE(MIN(p.customer_id), 'unknown_customer'),
                COALESCE(SUM(p.amount), 0.0),
                COALESCE(SUM(p.amount), 0.0),
                ?,
                ?,
                'paid',
                ?,
                ?
            FROM payments p
            LEFT JOIN invoices i ON i.invoice_id = p.invoice_id
            WHERE p.invoice_id IS NOT NULL
              AND i.invoice_id IS NULL
            GROUP BY p.invoice_id
            """,
            (now, now, now, now),
        )

        # Ensure customers exist for placeholder invoices.
        invoice_customer_ids = cursor.execute(
            "SELECT DISTINCT customer_id FROM invoices WHERE customer_id IS NOT NULL"
        ).fetchall()
        for (cid,) in invoice_customer_ids:
            cursor.execute(
                "INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at) VALUES (?, ?, ?)",
                (cid, now, now),
            )
            self._backfill_customer_name(conn, cid)

        # 2) Recompute paid_amount from payments and update status deterministically.
        cursor.execute(
            """
            UPDATE invoices
            SET
                paid_amount = CASE
                    WHEN (
                        SELECT SUM(COALESCE(amount, 0.0))
                        FROM payments p
                        WHERE p.invoice_id = invoices.invoice_id
                    ) IS NULL THEN MIN(COALESCE(total_amount, 0.0), COALESCE(paid_amount, 0.0))
                    ELSE MIN(
                        COALESCE(total_amount, 0.0),
                        COALESCE((
                            SELECT SUM(COALESCE(amount, 0.0))
                            FROM payments p
                            WHERE p.invoice_id = invoices.invoice_id
                        ), 0.0)
                    )
                END,
                status = CASE
                    WHEN COALESCE(total_amount, 0.0) <= 0 THEN status
                    WHEN COALESCE((
                        SELECT SUM(COALESCE(amount, 0.0))
                        FROM payments p
                        WHERE p.invoice_id = invoices.invoice_id
                    ), COALESCE(paid_amount, 0.0)) >= COALESCE(total_amount, 0.0) THEN 'paid'
                    WHEN COALESCE((
                        SELECT SUM(COALESCE(amount, 0.0))
                        FROM payments p
                        WHERE p.invoice_id = invoices.invoice_id
                    ), 0.0) > 0 THEN 'partial'
                    ELSE status
                END,
                updated_at = ?
            WHERE invoice_id IS NOT NULL
            """,
            (now,),
        )

    def _sanitize_company_name(self, name: Optional[str]) -> Optional[str]:
        """Normalize company names by stripping synthetic '- Unit <n>' suffixes."""
        if not name:
            return name

        cleaned = self.UNIT_SUFFIX_PATTERN.sub("", name).strip()
        return cleaned or name

    def _backfill_customer_name(self, conn: sqlite3.Connection, customer_id: str) -> None:
        """If a customer_id row has name=NULL, attempt to fill it from customer_risk_profile.

        Called immediately after every bare stub INSERT so the window in which a
        nameless row exists is as short as possible.
        """
        try:
            row = conn.execute(
                "SELECT name FROM customers WHERE customer_id = ?", (customer_id,)
            ).fetchone()
            if row and row[0] is not None:
                return  # Already has a real name — nothing to do

            # Attempt to resolve from customer_risk_profile
            profile_row = conn.execute(
                """
                SELECT company_name FROM customer_risk_profile
                WHERE customer_id = ?
                  AND company_name IS NOT NULL
                  AND company_name NOT LIKE 'cust\_%' ESCAPE '\\'
                LIMIT 1
                """,
                (customer_id,),
            ).fetchone()

            if profile_row and profile_row[0]:
                sanitized = self._sanitize_company_name(profile_row[0])
                if sanitized:
                    conn.execute(
                        "UPDATE customers SET name = ?, updated_at = ? "
                        "WHERE customer_id = ? AND name IS NULL",
                        (sanitized, datetime.utcnow().isoformat(), customer_id),
                    )
                    logger.info(
                        "[DBAgent] Backfilled name '%s' for customer %s from risk profile",
                        sanitized, customer_id,
                    )
        except Exception as e:
            logger.debug("[DBAgent] _backfill_customer_name failed for %s (non-fatal): %s", customer_id, e)

    def _cleanup_legacy_company_names(self, conn: sqlite3.Connection) -> None:
        """Backfill cleanup for already-persisted synthetic unit suffixes."""
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT customer_id, name
            FROM customers
            WHERE name IS NOT NULL AND name LIKE '% - Unit %'
            """
        )
        customer_rows = cursor.fetchall()

        cleaned_customers = 0
        for customer_id, name in customer_rows:
            sanitized = self._sanitize_company_name(name)
            if sanitized != name:
                cursor.execute(
                    "UPDATE customers SET name=?, updated_at=? WHERE customer_id=?",
                    (sanitized, datetime.utcnow().isoformat(), customer_id),
                )
                cleaned_customers += 1

        cursor.execute(
            """
            SELECT customer_id, company_name
            FROM customer_risk_profile
            WHERE company_name IS NOT NULL AND company_name LIKE '% - Unit %'
            """
        )
        risk_rows = cursor.fetchall()

        cleaned_risk_profiles = 0
        for customer_id, company_name in risk_rows:
            sanitized = self._sanitize_company_name(company_name)
            if sanitized != company_name:
                cursor.execute(
                    "UPDATE customer_risk_profile SET company_name=?, updated_at=? WHERE customer_id=?",
                    (sanitized, datetime.utcnow().isoformat(), customer_id),
                )
                cleaned_risk_profiles += 1

        if cleaned_customers or cleaned_risk_profiles:
            conn.commit()
            logger.info(
                "[DBAgent] Cleaned legacy company names: customers=%s, customer_risk_profile=%s",
                cleaned_customers,
                cleaned_risk_profiles,
            )

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
        elif event_type in {"payment.received", "payment.partial"}:
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
        raw_total_amount = data.get("amount")
        if raw_total_amount is None:
            raw_total_amount = data.get("total_amount")
        due_date = data.get("due_date")
        status = data.get("status", "pending")
        issued_date = data.get("created_at") or data.get("issued_date")
        now = datetime.utcnow().isoformat()

        total_amount = None
        if raw_total_amount is not None:
            try:
                total_amount = float(raw_total_amount)
            except (TypeError, ValueError):
                logger.warning(
                    "[DBAgent] Invalid invoice amount for %s: %r; preserving existing amount if available",
                    invoice_id,
                    raw_total_amount,
                )

        if not invoice_id:
            logger.warning("Invoice event missing invoice_id, skipping")
            return

        if not issued_date:
            issued_date = now

        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                if customer_id:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                        """,
                        (customer_id, now, now),
                    )
                    self._backfill_customer_name(conn, customer_id)

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
                        customer_id=COALESCE(excluded.customer_id, invoices.customer_id),
                        total_amount=COALESCE(?, invoices.total_amount, 0.0),
                        due_date=COALESCE(excluded.due_date, invoices.due_date),
                        status=excluded.status,
                        updated_at=excluded.updated_at
                """, (
                    invoice_id,
                    customer_id,
                    total_amount if total_amount is not None else 0.0,
                    0.0,  # Initialize paid_amount to 0
                    issued_date,
                    due_date,
                    status,
                    now,
                    now,
                    total_amount,
                ))

                # Ensure customer exists (second guard after invoice insert)
                if customer_id:
                    cursor.execute("""
                        INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                    """, (customer_id, now, now))
                    self._backfill_customer_name(conn, customer_id)

                cursor.execute(
                    """
                    SELECT
                        COALESCE(total_amount, 0.0),
                        COALESCE(paid_amount, 0.0)
                    FROM invoices
                    WHERE invoice_id = ?
                    """,
                    (invoice_id,),
                )
                invoice_row = cursor.fetchone()
                resolved_total_amount = float(invoice_row[0]) if invoice_row else 0.0
                resolved_paid_amount = float(invoice_row[1]) if invoice_row else 0.0
                resolved_remaining_amount = max(
                    resolved_total_amount - resolved_paid_amount,
                    0.0,
                )

                conn.commit()
                logger.info(
                    f"[DBAgent] Upserted invoice: {invoice_id} status={status} "
                    f"total_amount={resolved_total_amount}"
                )

                # FIX #2: Pre-populate cache instead of just invalidating
                # This prevents cache misses for agents querying immediately after write
                if self._query_agent:
                    invoice_cache_data = {
                        "invoice_id": invoice_id,
                        "customer_id": customer_id,
                        "total_amount": resolved_total_amount,
                        "paid_amount": resolved_paid_amount,
                        "remaining_amount": resolved_remaining_amount,
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
        raw_amount = data.get("amount")
        payment_date = data.get("payment_date") or datetime.utcnow().isoformat()
        now = datetime.utcnow().isoformat()

        if not payment_id:
            logger.warning("payment.received event missing payment_id, skipping")
            return

        try:
            amount = float(raw_amount or 0.0)
        except (TypeError, ValueError):
            logger.warning(
                "[DBAgent] Invalid payment amount for payment_id=%s invoice_id=%s: %r",
                payment_id,
                invoice_id,
                raw_amount,
            )
            return

        if amount <= 0:
            logger.warning(
                "[DBAgent] Payment amount must be positive, got %r for payment_id=%s invoice_id=%s — rejected",
                raw_amount, payment_id, invoice_id,
            )
            return

        with self._db_lock:
            conn = self._get_connection()
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

                # Ensure parent rows exist before inserting payment
                # (payments can arrive before invoice.created due to Kafka ordering)

                # 1) Ensure customer exists
                if customer_id:
                    cursor.execute("""
                        INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                    """, (customer_id, now, now))
                    self._backfill_customer_name(conn, customer_id)

                # 2) Ensure invoice exists — create placeholder if not yet
                if invoice_id:
                    cursor.execute("""
                        INSERT OR IGNORE INTO invoices (
                            invoice_id, customer_id, total_amount, paid_amount,
                            issued_date, due_date, status, created_at, updated_at
                        ) VALUES (?, ?, 0.0, 0.0, ?, ?, 'pending', ?, ?)
                    """, (invoice_id, customer_id, now, now, now, now))
                    if cursor.rowcount > 0:
                        logger.info(
                            f"[DBAgent] Created placeholder invoice {invoice_id} for payment "
                            f"(will be amended when invoice.created arrives)"
                        )

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
                            new_paid = current_paid + amount
                            if total_amount is not None:
                                new_paid = min(float(total_amount), new_paid)

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
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                if customer_id:
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                        VALUES (?, ?, ?)
                        """,
                        (customer_id, timestamp, timestamp),
                    )
                    self._backfill_customer_name(conn, customer_id)
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
        """Handle customer.profile.updated event - upsert customer profile data.

        Strategy:
          Step 1 — INSERT OR IGNORE: create the row if it does not yet exist,
                   seeding it with whatever fields this event carries.
          Step 2 — Targeted UPDATE: build the SET clause dynamically from only
                   the keys that were actually present in the payload.
                   Fields absent from the payload are left untouched, so agents
                   that own different columns (ScenarioGeneratorAgent → credit_limit,
                   CustomerProfileAgent → risk_score) never overwrite each other.
        """
        data = event.payload or {}
        customer_id = data.get("customer_id")
        if not customer_id:
            logger.warning("customer.profile.updated event missing customer_id, skipping")
            return

        now = datetime.utcnow().isoformat()

        # Only extract fields that are genuinely present in this event's payload
        name         = self._sanitize_company_name(data.get("customer_name") or data.get("name"))
        has_risk     = "risk_score"   in data
        has_limit    = "credit_limit" in data
        has_status   = "status"       in data

        risk_score   = float(data["risk_score"])   if has_risk  else None
        credit_limit = float(data["credit_limit"]) if has_limit else None
        status       = data["status"]              if has_status else "active"

        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Step 1: Ensure the customer row exists.
                # CRITICAL: Do NOT insert name=NULL — it creates stub rows we can't fix later.
                # Only include name in the INSERT if we have a real value.
                if name is not None:
                    cursor.execute("""
                        INSERT OR IGNORE INTO customers
                            (customer_id, name, risk_score, credit_limit, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        customer_id,
                        name,
                        risk_score   if risk_score   is not None else 0.0,
                        credit_limit if credit_limit is not None else 0.0,
                        status,
                        now, now,
                    ))
                else:
                    # No name — only insert the bare skeleton if the row is truly missing.
                    # Skip if the row already exists so we don't clobber a real name.
                    cursor.execute("""
                        INSERT OR IGNORE INTO customers
                            (customer_id, risk_score, credit_limit, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        customer_id,
                        risk_score   if risk_score   is not None else 0.0,
                        credit_limit if credit_limit is not None else 0.0,
                        status,
                        now, now,
                    ))
                    # Immediately attempt backfill in case profile arrived earlier
                    self._backfill_customer_name(conn, customer_id)

                # Step 2: Unconditionally update name when a real name is present.
                # This ensures NULL rows (created by stub inserts from invoice/payment handlers)
                # are overwritten as soon as the profile event arrives.
                update_fields = []
                update_params = []

                if name is not None:
                    update_fields.append("name = ?")
                    update_params.append(name)

                if has_risk:
                    update_fields.append("risk_score = ?")
                    update_params.append(risk_score)

                if has_limit:
                    update_fields.append("credit_limit = ?")
                    update_params.append(credit_limit)

                if has_status:
                    update_fields.append("status = ?")
                    update_params.append(status)

                if update_fields:
                    update_fields.append("updated_at = ?")
                    update_params.append(now)
                    update_params.append(customer_id)
                    cursor.execute(
                        f"UPDATE customers SET {', '.join(update_fields)} WHERE customer_id = ?",
                        update_params,
                    )

                conn.commit()
                logger.info(
                    f"[DBAgent] Upserted customer: {customer_id} "
                    f"name={name!r} risk={risk_score} limit={credit_limit} status={status}"
                )

                # Update query-agent cache with only the fields we know about
                if self._query_agent:
                    cache_patch = {"customer_id": customer_id, "updated_at": now}
                    if name         is not None: cache_patch["name"]         = name
                    if has_risk:                 cache_patch["risk_score"]   = risk_score
                    if has_limit:                cache_patch["credit_limit"] = credit_limit
                    self._query_agent.update_customer_cache(customer_id, cache_patch)
            finally:
                conn.close()

    def _handle_litigation_event(self, event: Event) -> None:
        """Handle LitigationRiskUpdated event - insert litigation risk data."""
        data = event.payload or {}
        customer_id = data.get("customer_id")
        company_name = data.get("company_name")
        company_name = self._sanitize_company_name(company_name)
        litigation_risk = data.get("litigation_risk", 0.0)
        severity = data.get("severity")
        case_count = data.get("case_count") or data.get("nclt_case_count", 0)
        case_types = data.get("case_types", [])
        cases = data.get("cases") or data.get("nclt_cases", [])
        evidence = data.get("evidence", "")
        source = data.get("source")
        confidence = data.get("confidence", 0.0)
        created_at = datetime.utcnow().isoformat()

        if not customer_id:
            logger.warning("LitigationRiskUpdated event missing customer_id, skipping")
            return

        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Ensure customer exists
                cursor.execute("""
                    INSERT OR IGNORE INTO customers (customer_id, created_at, updated_at)
                    VALUES (?, ?, ?)
                """, (customer_id, created_at, created_at))
                # Attempt name backfill in case profile event hasn't arrived yet
                self._backfill_customer_name(conn, customer_id)
                # Also try the company_name from this litigation event directly
                if company_name:
                    conn.execute(
                        "UPDATE customers SET name = ?, updated_at = ? "
                        "WHERE customer_id = ? AND name IS NULL",
                        (company_name, created_at, customer_id),
                    )

                cursor.execute("""
                    INSERT OR REPLACE INTO external_litigation (
                        id,
                        customer_id,
                        company_name,
                        litigation_risk,
                        severity,
                        case_count,
                        case_types,
                        cases,
                        evidence,
                        source,
                        confidence,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event.event_id,
                    customer_id,
                    company_name,
                    litigation_risk,
                    severity,
                    case_count,
                    json.dumps(case_types or []),
                    json.dumps(cases or []),
                    evidence,
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
        """
        Handle risk.profile.updated event — upsert aggregated risk data.

        KEY FIXES:
        1. IDEMPOTENCY: skip if this event_id was already processed.
        2. NULL-SAFE UPSERT: financial_risk, financial_source and company_name
           are only overwritten when the incoming value is NOT NULL, so a
           throttled follow-up event (financial_risk=None) can never destroy
           a previously stored valid score.
        3. FRESHNESS CHECK: incoming event's generated_at must be >= the stored
           updated_at, otherwise the write is silently skipped.
        """
        import re as _re
        data = event.payload or {}

        customer_id = data.get("customer_id")
        company_name = data.get("company_name")
        company_name = self._sanitize_company_name(company_name)

        if not customer_id:
            logger.warning("CustomerRiskProfileUpdated missing customer_id, skipping")
            return

        # --- IDEMPOTENCY GUARD ---
        event_id = event.event_id
        if event_id and event_id in self._processed_risk_events:
            logger.debug(f"[DBAgent] Skipping duplicate risk profile event_id={event_id}")
            return

        # Guard: if company_name looks like a customer_id fallback (e.g. "cust_00003"),
        # try to resolve it from the customers table before persisting.
        if not company_name or _re.match(r'^cust_\d+$', company_name):
            conn_check = self._get_connection()
            try:
                row = conn_check.execute(
                    "SELECT name FROM customers WHERE customer_id = ?", (customer_id,)
                ).fetchone()
                if row and row[0]:
                    company_name = self._sanitize_company_name(row[0])
                    logger.debug(f"[DBAgent] Resolved company_name from customers table: {company_name}")
                else:
                    company_name = None  # persist NULL rather than a useless placeholder
            except Exception:
                company_name = None
            finally:
                conn_check.close()

        # financial_risk may be None (private company / failed fetch) — preserve existing in that case
        financial_risk = data.get("financial_risk")   # intentionally NOT defaulting to 0.0
        litigation_risk = data.get("litigation_risk", 0.0)
        combined_risk = data.get("combined_risk", 0.0)

        severity = data.get("severity")
        financial_source = data.get("financial_source")  # may be None
        litigation_source = data.get("litigation_source")

        confidence = data.get("confidence", 0.0)
        now_iso = datetime.utcnow().isoformat()

        # Parse the event's generated_at for the freshness guard
        incoming_generated_at = data.get("generated_at")

        with self._db_lock:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # --- FRESHNESS GUARD ---
                # Check the stored updated_at; if the incoming event is older, skip.
                existing_row = cursor.execute(
                    "SELECT updated_at FROM customer_risk_profile WHERE customer_id = ?",
                    (customer_id,)
                ).fetchone()
                if existing_row and existing_row[0] and incoming_generated_at:
                    try:
                        if incoming_generated_at < existing_row[0]:
                            logger.debug(
                                f"[DBAgent] Discarding stale risk profile event for {customer_id}: "
                                f"event generated_at={incoming_generated_at} < stored updated_at={existing_row[0]}"
                            )
                            return
                    except Exception:
                        pass  # If comparison fails, proceed

                # --- NULL-SAFE UPSERT ---
                # Use INSERT ... ON CONFLICT DO UPDATE with COALESCE so that:
                #   - financial_risk: only overwritten if the new value is NOT NULL
                #   - financial_source: same protection
                #   - company_name: same protection
                # This is the core fix for Bug 2 — an event with financial_risk=None
                # (throttled ExternalDataAgent) can no longer wipe out a real score.
                cursor.execute("""
                    INSERT INTO customer_risk_profile (
                        customer_id,
                        id,
                        company_name,
                        financial_risk,
                        litigation_risk,
                        combined_risk,
                        severity,
                        financial_source,
                        litigation_source,
                        confidence,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO UPDATE SET
                        id               = excluded.id,
                        company_name     = COALESCE(excluded.company_name,    customer_risk_profile.company_name),
                        financial_risk   = COALESCE(excluded.financial_risk,  customer_risk_profile.financial_risk),
                        litigation_risk  = excluded.litigation_risk,
                        combined_risk    = excluded.combined_risk,
                        severity         = excluded.severity,
                        financial_source = COALESCE(excluded.financial_source, customer_risk_profile.financial_source),
                        litigation_source = excluded.litigation_source,
                        confidence       = excluded.confidence,
                        updated_at       = excluded.updated_at
                """, (
                    customer_id,
                    event_id,
                    company_name,
                    financial_risk,
                    litigation_risk,
                    combined_risk,
                    severity,
                    financial_source,
                    litigation_source,
                    confidence,
                    now_iso,   # created_at (preserved by COALESCE for existing rows)
                    now_iso,   # updated_at (always refreshed)
                ))

                conn.commit()

                logger.info(
                    f"[DBAgent] Stored aggregated risk: customer={customer_id}, combined={combined_risk}, "
                    f"financial={financial_risk if financial_risk is not None else '(preserved)'}"
                )

                # --- MARK EVENT AS PROCESSED (bounded eviction) ---
                if event_id:
                    if len(self._processed_risk_events) >= self.MAX_PROCESSED_IDS:
                        self._processed_risk_events.popitem(last=False)
                    self._processed_risk_events[event_id] = True

                # Invalidate cache
                if self._query_agent:
                    self._query_agent.invalidate_customer_cache(customer_id)

            finally:
                conn.close()
