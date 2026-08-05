"""Invoice event handler for DBAgent."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event
from utils.query_client import QueryClient

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


def handle_invoice_upsert(agent: "DBAgent", event: Event) -> None:
    """Handle all invoice.* events using UPSERT logic.

    Supports: invoice.created, invoice.overdue, invoice.disputed, invoice.cancelled
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
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

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

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return

            agent._ensure_customer_exists(
                conn,
                customer_id,
                data.get("customer_name") or data.get("company_name") or data.get("name"),
            )

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

            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()
            logger.info(
                f"[DBAgent] Upserted invoice: {invoice_id} status={status} "
                f"total_amount={resolved_total_amount}"
            )

            # Pre-populate cache instead of just invalidating
            QueryClient.query("update_invoice_cache", {"invoice_id": invoice_id})
            if customer_id:
                QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)
