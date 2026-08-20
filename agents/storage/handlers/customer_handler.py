"""Customer profile event handler for DBAgent."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event
from utils.query_client import QueryClient

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


def handle_customer_profile(agent: "DBAgent", event: Event) -> None:
    """Handle customer.profile.updated event — upsert customer profile data.

    Strategy:
      Step 1 — INSERT OR IGNORE: create the row if it does not yet exist.
      Step 2 — Targeted UPDATE: dynamically set only the fields present in payload.
    """
    data = event.payload or {}
    customer_id = data.get("customer_id")
    if not customer_id:
        logger.warning("customer.profile.updated event missing customer_id, skipping")
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # Only extract fields that are genuinely present in this event's payload
    name = agent._sanitize_company_name(data.get("customer_name") or data.get("name"))
    if name is None:
        name = customer_id
    has_risk = "risk_score" in data
    has_limit = "credit_limit" in data
    has_status = "status" in data

    risk_score = float(data["risk_score"]) if has_risk else None
    credit_limit = float(data["credit_limit"]) if has_limit else None
    status = data["status"] if has_status else "active"

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return

            # Step 1: Ensure the customer row exists.
            if name is not None:
                cursor.execute("""
                    INSERT INTO customers
                        (customer_id, name, risk_score, credit_limit, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO NOTHING
                """, (
                    customer_id,
                    name,
                    risk_score if risk_score is not None else 0.0,
                    credit_limit if credit_limit is not None else 0.0,
                    status,
                    now, now,
                ))
            else:
                cursor.execute("""
                    INSERT INTO customers
                        (customer_id, risk_score, credit_limit, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(customer_id) DO NOTHING
                """, (
                    customer_id,
                    risk_score if risk_score is not None else 0.0,
                    credit_limit if credit_limit is not None else 0.0,
                    status,
                    now, now,
                ))
                # Immediately attempt backfill in case profile arrived earlier
                agent._backfill_customer_name(conn, customer_id)

            # Step 2: Unconditionally update name when a real name is present.
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
            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()
            logger.info(
                f"[DBAgent] Upserted customer: {customer_id} "
                f"name={name!r} risk={risk_score} limit={credit_limit} status={status}"
            )

            # Update query-agent cache
            QueryClient.query("update_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)
