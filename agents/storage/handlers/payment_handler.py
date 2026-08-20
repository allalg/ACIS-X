"""Payment event handler for DBAgent."""

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event
from utils.query_client import QueryClient

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


def handle_payment_received(agent: "DBAgent", event: Event) -> None:
    """Handle payment.received event — insert payment and update invoice paid_amount and status."""
    data = event.payload or {}
    payment_id = data.get("payment_id")
    invoice_id = data.get("invoice_id")
    raw_amount = data.get("amount")
    payment_date = data.get("payment_date") or datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    if not payment_id:
        logger.warning("payment.received event missing payment_id, skipping")
        return

    try:
        amount = float(raw_amount or 0.0)
    except (TypeError, ValueError):
        logger.warning(
            "[DBAgent] Invalid payment amount for payment_id=%s invoice_id=%s: %r",
            payment_id, invoice_id, raw_amount,
        )
        return

    if amount <= 0:
        logger.warning(
            "[DBAgent] Payment amount must be positive, got %r for payment_id=%s invoice_id=%s — rejected",
            raw_amount, payment_id, invoice_id,
        )
        return

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return

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
            agent._ensure_customer_exists(
                conn,
                customer_id,
                data.get("customer_name") or data.get("company_name") or data.get("name"),
            )

            if invoice_id:
                cursor.execute("""
                    INSERT INTO invoices (
                        invoice_id, customer_id, total_amount, paid_amount,
                        issued_date, due_date, status, created_at, updated_at
                    ) VALUES (?, ?, 0.0, 0.0, ?, ?, 'pending', ?, ?)
                    ON CONFLICT(invoice_id) DO NOTHING
                """, (invoice_id, customer_id, now, now, now, now))
                if cursor.rowcount > 0:
                    logger.info(
                        f"[DBAgent] Created placeholder invoice {invoice_id} for payment "
                        f"(will be amended when invoice.created arrives)"
                    )

            # Insert payment (idempotent)
            cursor.execute("""
                INSERT INTO payments (
                    payment_id, invoice_id, customer_id, amount, payment_date, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(payment_id) DO NOTHING
            """, (payment_id, invoice_id, customer_id, amount, payment_date, now))

            if cursor.rowcount > 0:
                logger.info(f"[DBAgent] Inserted payment: {payment_id} for invoice: {invoice_id}, amount={amount}")

                # Update invoice paid_amount and determine status
                if invoice_id:
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

            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()

            # Invalidate caches
            if invoice_id:
                QueryClient.query("invalidate_invoice_cache", {"invoice_id": invoice_id})
            if customer_id:
                QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)
