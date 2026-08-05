import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import List, Any, Optional

from agents.base.base_agent import BaseAgent
from schemas.event_schema import Event
from utils.query_client import QueryClient

# Handler modules extracted from this file (Phase 2 refactor)
from agents.storage.handlers import (
    SCHEMA_DDL,
    SCHEMA_MIGRATIONS,
    SCHEMA_VERSION,
    handle_invoice_upsert,
    handle_payment_received,
    handle_collection_action,
    handle_customer_profile,
    handle_metrics_updated,
    handle_litigation_event,
    handle_customer_risk_profile,
    handle_risk_scored,
)

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

    # Consumer auto_offset_reset for this agent type.
    # "earliest" ensures DBAgent replays all events after a restart so no
    # invoices, payments, or risk profiles are silently dropped.
    OFFSET_RESET = "earliest"

    # Idempotency: bounded in-memory set of processed risk profile event IDs
    MAX_PROCESSED_IDS = 20000

    def __init__(
        self,
        kafka_client: Any,
        db_path: Optional[str] = None,
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

        self._db_path = db_path or os.getenv("ACIS_DB_PATH", self.DB_PATH)
        self._db_lock = threading.Lock()

        # Idempotency: track processed risk profile event IDs (bounded OrderedDict)
        from collections import OrderedDict
        self._processed_risk_events: OrderedDict = OrderedDict()

        # Housekeeping: prune old event_log rows periodically (at most once/hour)
        self._last_event_log_cleanup = datetime.now(timezone.utc).replace(tzinfo=None)

        self._init_database()
        logger.info("QueryAgent reference set for cache invalidation")

    def _get_uri_path(self) -> str:
        if self._db_path.startswith("file:"):
            return self._db_path
        abs_path = os.path.abspath(self._db_path).replace("\\", "/")
        if not abs_path.startswith("/"):
            abs_path = "/" + abs_path
        return f"file:{abs_path}?nolock=1"

    def _get_connection(self) -> sqlite3.Connection:
        """Create a DB connection with FK enforcement enabled."""
        conn = sqlite3.connect(self._get_uri_path(), uri=True, timeout=30.0, isolation_level="DEFERRED")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _finalize_handler_connection(self, conn: sqlite3.Connection) -> None:
        """Close a handler connection without committing failed partial writes."""
        try:
            if conn.in_transaction:
                conn.rollback()
            else:
                self._maybe_prune_event_log(conn)
        finally:
            conn.close()

    def _ensure_customer_exists(
        self,
        conn: sqlite3.Connection,
        customer_id: Optional[str],
        name: Optional[str] = None,
    ) -> None:
        """Create a customer parent row with a valid name for FK-dependent writes."""
        if not customer_id:
            return

        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        safe_name = self._sanitize_company_name(name) or customer_id
        conn.execute(
            """
            INSERT OR IGNORE INTO customers (customer_id, name, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (customer_id, safe_name, now, now),
        )
        self._backfill_customer_name(conn, customer_id)

    def _init_database(self) -> None:
        """Initialize SQLite database and create tables if not exists.

        Schema DDL is defined in agents.storage.handlers.schema and executed
        in order.  Migrations (ALTER TABLE) are applied best-effort.
        """
        with self._db_lock:
            # FIX #6: Handle corrupted database by detecting and cleaning it up
            corruption_detected = False

            try:
                test_conn = sqlite3.connect(self._get_uri_path(), uri=True, timeout=10.0)
                test_cursor = test_conn.cursor()
                test_cursor.execute("PRAGMA integrity_check")
                integrity = test_cursor.fetchone()[0]
                test_conn.close()

                if integrity != "ok":
                    logger.warning(f"Database integrity check failed: {integrity}, removing corrupted file")
                    corruption_detected = True
            except sqlite3.DatabaseError as e:
                if "malformed" in str(e).lower():
                    logger.warning(f"Database corrupted, preparing cleanup: {e}")
                    corruption_detected = True
                test_conn = None

            if corruption_detected:
                for attempt in range(3):
                    try:
                        for suffix in ["", "-wal", "-shm"]:
                            filepath = self._db_path + suffix
                            if os.path.exists(filepath):
                                try:
                                    os.remove(filepath)
                                    logger.info(f"Deleted corrupted database file: {filepath}")
                                except Exception:
                                    pass
                        break
                    except Exception as e:
                        if attempt < 2:
                            logger.debug(f"Delete attempt {attempt + 1} failed, retrying: {e}")
                            time.sleep(0.5)
                        else:
                            logger.error(f"Could not remove corrupted database after 3 attempts: {e}")
                            raise

            conn = self._get_connection()
            try:
                cursor = conn.cursor()

                # Execute all DDL from the schema module
                for ddl in SCHEMA_DDL:
                    cursor.execute(ddl)

                # Apply migrations (add columns if missing)
                for migration in SCHEMA_MIGRATIONS:
                    try:
                        cursor.execute(migration)
                    except sqlite3.OperationalError:
                        pass  # Column already exists

                self._cleanup_legacy_company_names(conn)
                self._repair_payment_integrity(conn)
                conn.commit()
                logger.info(f"Database initialized at {self._db_path}")
                logger.info(f"Database schema {SCHEMA_VERSION} ready")
            finally:
                conn.close()

    def _repair_payment_integrity(self, conn: sqlite3.Connection) -> None:
        """
        Repair legacy payment/invoice integrity issues.

        **Transaction contract**: this method MUST only be called within a
        transaction that is managed (opened and committed/rolled-back) by the
        caller.  It purposely contains no ``conn.commit()`` calls so that all
        SQL it accumulates is either committed atomically alongside the caller's
        other work, or rolled back as a whole on failure.
        Callers are responsible for issuing exactly one ``conn.commit()`` after
        this method returns.

        Repairs performed:
        - Backfill placeholder invoices for orphan payments.
        - Recompute invoice paid_amount/status from payment records.
        - Clamp paid_amount to total_amount to avoid negative remaining balance
          drift.
        """
        cursor = conn.cursor()
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

        # Ensure all customers referenced by payments exist before any invoice backfill.
        # Use a SELECT-based approach to avoid creating stub rows for customers
        # that do not yet have a profile — on a fresh DB, payments table is empty
        # so this is a no-op.  We use DEFERRED FK mode for this insert only.
        payment_customer_ids = cursor.execute(
            "SELECT DISTINCT customer_id FROM payments WHERE customer_id IS NOT NULL"
        ).fetchall()
        for (cid,) in payment_customer_ids:
            self._ensure_customer_exists(conn, cid)

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
              AND p.customer_id IS NOT NULL
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
            self._ensure_customer_exists(conn, cid)

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
            if row and row[0] is not None and row[0] != customer_id:
                return

            # Attempt to resolve from customer_risk_profile
            profile_row = conn.execute(
                r"""
                SELECT company_name FROM customer_risk_profile
                WHERE customer_id = ?
                  AND company_name IS NOT NULL
                  AND company_name NOT LIKE 'cust\_%' ESCAPE '\'
                LIMIT 1
                """,
                (customer_id,),
            ).fetchone()

            if profile_row and profile_row[0]:
                sanitized = self._sanitize_company_name(profile_row[0])
                if sanitized:
                    conn.execute(
                        "UPDATE customers SET name = ?, updated_at = ? "
                        "WHERE customer_id = ? AND (name IS NULL OR name = ?)",
                        (sanitized, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), customer_id, customer_id),
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
                    (sanitized, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), customer_id),
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
                    (sanitized, datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), customer_id),
                )
                cleaned_risk_profiles += 1

        if cleaned_customers or cleaned_risk_profiles:
            # No conn.commit() here — transaction is owned by the caller
            # (_init_database commits once after _repair_payment_integrity returns).
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
        """Route incoming events to the appropriate handler module."""
        event_type = event.event_type

        if event_type.startswith("invoice."):
            handle_invoice_upsert(self, event)
        elif event_type in {"payment.received", "payment.partial"}:
            handle_payment_received(self, event)
        elif event_type.startswith("collection."):
            handle_collection_action(self, event)
        elif event_type == "customer.profile.updated":
            handle_customer_profile(self, event)
        elif event_type == "external.litigation.updated":
            handle_litigation_event(self, event)
        elif event_type == "risk.profile.updated":
            handle_customer_risk_profile(self, event)
        elif event_type == "risk.scored":
            handle_risk_scored(self, event)
        elif event_type == "customer.metrics.updated":
            handle_metrics_updated(self, event)

    # ── Event log housekeeping ─────────────────────────────────────────────

    def _maybe_prune_event_log(self, conn: "sqlite3.Connection") -> None:
        """Delete event_log rows older than 7 days, at most once per hour."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if (now - self._last_event_log_cleanup).total_seconds() < 3600:
            return
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM event_log WHERE processed_at < datetime('now', '-7 days')"
            )
            deleted = cursor.rowcount
            conn.commit()
            self._last_event_log_cleanup = now
            if deleted:
                logger.info("[DBAgent] Pruned %d stale event_log rows", deleted)
        except Exception as exc:
            logger.warning("[DBAgent] event_log prune failed: %s", exc)

    # ── Backward Compatibility Delegate Methods ────────────────────────────
    # Preserves backwards compatibility for existing unit tests calling private handlers directly

    def _handle_invoice_upsert(self, event: Event) -> None:
        handle_invoice_upsert(self, event)

    def _handle_payment_received(self, event: Event) -> None:
        handle_payment_received(self, event)

    def _handle_collection_action(self, event: Event) -> None:
        handle_collection_action(self, event)

    def _handle_customer_profile(self, event: Event) -> None:
        handle_customer_profile(self, event)

    def _handle_litigation_event(self, event: Event) -> None:
        handle_litigation_event(self, event)

    def _handle_customer_risk_profile(self, event: Event) -> None:
        handle_customer_risk_profile(self, event)

    def _handle_risk_scored(self, event: Event) -> None:
        handle_risk_scored(self, event)

    def _handle_metrics_updated(self, event: Event) -> None:
        handle_metrics_updated(self, event)


