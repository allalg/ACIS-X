"""Metrics event handler for DBAgent."""

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


def handle_metrics_updated(agent: "DBAgent", event: Event) -> None:
    """Handle customer.metrics.updated event and update customer_metrics table."""
    data = event.payload or {}
    customer_id = data.get("customer_id")
    if not customer_id:
        logger.warning("customer.metrics.updated event missing customer_id, skipping")
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
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
                data.get("company_name") or data.get("customer_name") or data.get("name"),
            )

            aging_buckets = data.get("aging_buckets", {})

            cursor.execute(
                """
                INSERT OR REPLACE INTO customer_metrics
                (customer_id, total_outstanding, avg_delay, on_time_ratio,
                 aging_current, aging_1_30, aging_31_60, aging_61_90, aging_90_plus,
                 last_payment_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    customer_id,
                    float(data.get("total_outstanding", 0.0)),
                    float(data.get("avg_delay", 0.0)),
                    float(data.get("on_time_ratio", 0.0)),
                    float(aging_buckets.get("current", 0.0)),
                    float(aging_buckets.get("1_30_days", 0.0)),
                    float(aging_buckets.get("31_60_days", 0.0)),
                    float(aging_buckets.get("61_90_days", 0.0)),
                    float(aging_buckets.get("90_plus_days", 0.0)),
                    data.get("last_payment_date"),
                    now,
                ),
            )

            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, now),
            )
            conn.commit()
            logger.info(f"[DBAgent] Upserted metrics for customer {customer_id}")
        except Exception as e:
            logger.error(f"[DBAgent] Error handling metrics update: {e}")
        finally:
            agent._finalize_handler_connection(conn)
