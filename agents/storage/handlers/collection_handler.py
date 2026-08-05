"""Collection event handler for DBAgent."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event
from utils.query_client import QueryClient

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


def handle_collection_action(agent: "DBAgent", event: Event) -> None:
    """Handle all collection.* events — insert into collections_log."""
    data = event.payload or {}
    collection_id = data.get("id") or data.get("collection_id") or event.event_id
    customer_id = data.get("customer_id")
    invoice_id = data.get("invoice_id")

    # Map action_type or action field
    action = data.get("action") or data.get("action_type") or event.event_type

    # Map stage from event type if not in payload
    stage = data.get("stage") or event.event_type

    # Analytics fields
    priority = data.get("priority")
    reason = data.get("reason")
    timestamp = data.get("timestamp") or datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    if not collection_id:
        logger.warning(f"{event.event_type} event missing id, skipping")
        return

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return
            cursor.execute("""
                INSERT OR IGNORE INTO collections_log (
                    id, customer_id, invoice_id, action, stage, priority, reason, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (collection_id, customer_id, invoice_id, action, stage, priority, reason, timestamp))
            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()

            if cursor.rowcount > 0:
                logger.info(
                    f"[DBAgent] Inserted collection log: {collection_id} for customer: {customer_id}, "
                    f"action: {action}, priority: {priority}, reason: {reason}"
                )
            else:
                logger.debug(f"[DBAgent] Collection log {collection_id} already exists, skipped")

            # Invalidate caches
            if customer_id:
                QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)
