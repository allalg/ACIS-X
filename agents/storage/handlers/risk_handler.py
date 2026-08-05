"""Risk-related event handlers for DBAgent.

Handles:
- external.litigation.updated → external_litigation table
- risk.profile.updated → customer_risk_profile table
- risk.scored → customers.risk_score + risk_explanations table
"""

import json
import logging
import re as _re
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from schemas.event_schema import Event
from utils.query_client import QueryClient

if TYPE_CHECKING:
    from agents.storage.db_agent import DBAgent

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# external.litigation.updated
# ─────────────────────────────────────────────────────────────────────────────

def handle_litigation_event(agent: "DBAgent", event: Event) -> None:
    """Handle LitigationRiskUpdated event — insert litigation risk data."""
    data = event.payload or {}
    customer_id = data.get("customer_id")
    company_name = data.get("company_name")
    company_name = agent._sanitize_company_name(company_name)
    litigation_risk = data.get("litigation_risk", 0.0)
    severity = data.get("severity")
    case_count = data.get("case_count") or data.get("nclt_case_count", 0)
    case_types = data.get("case_types", [])
    cases = data.get("cases") or data.get("nclt_cases", [])
    evidence = data.get("evidence", "")
    source = data.get("source")
    confidence = data.get("confidence", 0.0)
    created_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    if not customer_id:
        logger.warning("LitigationRiskUpdated event missing customer_id, skipping")
        return

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return

            # Backfill company_name on customers table if still NULL
            if company_name:
                conn.execute(
                    "UPDATE customers SET name = ?, updated_at = ? "
                    "WHERE customer_id = ? AND name IS NULL",
                    (company_name, created_at, customer_id),
                )

            cursor.execute("""
                INSERT OR REPLACE INTO external_litigation (
                    id, customer_id, company_name, litigation_risk, severity,
                    case_count, case_types, cases, evidence, source, confidence, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                created_at,
            ))
            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()

            if cursor.rowcount > 0:
                logger.info(
                    f"[DBAgent] Stored litigation: customer={customer_id}, "
                    f"risk={litigation_risk}, cases={case_count}"
                )
            else:
                logger.debug(f"[DBAgent] Litigation record {event.event_id} already exists, skipped")

            # Invalidate cache
            QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)


# ─────────────────────────────────────────────────────────────────────────────
# risk.profile.updated
# ─────────────────────────────────────────────────────────────────────────────

def handle_customer_risk_profile(agent: "DBAgent", event: Event) -> None:
    """Handle risk.profile.updated event — upsert aggregated risk data.

    KEY FIXES:
    1. IDEMPOTENCY: skip if this event_id was already processed.
    2. NULL-SAFE UPSERT: financial_risk, financial_source and company_name
       are only overwritten when the incoming value is NOT NULL.
    3. FRESHNESS CHECK: incoming event's generated_at must be >= the stored
       updated_at, otherwise the write is silently skipped.
    """
    data = event.payload or {}

    customer_id = data.get("customer_id")
    company_name = data.get("company_name")
    company_name = agent._sanitize_company_name(company_name)

    if not customer_id:
        logger.warning("CustomerRiskProfileUpdated missing customer_id, skipping")
        return

    # --- IDEMPOTENCY GUARD ---
    event_id = event.event_id
    if event_id and event_id in agent._processed_risk_events:
        logger.debug(f"[DBAgent] Skipping duplicate risk profile event_id={event_id}")
        return

    # Guard: if company_name looks like a customer_id fallback (e.g. "cust_00003"),
    # try to resolve it from the customers table before persisting.
    if not company_name or _re.match(r'^cust_\d+$', company_name):
        conn_check = agent._get_connection()
        try:
            row = conn_check.execute(
                "SELECT name FROM customers WHERE customer_id = ?", (customer_id,)
            ).fetchone()
            if row and row[0]:
                company_name = agent._sanitize_company_name(row[0])
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
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # Parse the event's generated_at for the freshness guard
    incoming_generated_at = data.get("generated_at")

    with agent._db_lock:
        conn = agent._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM event_log WHERE event_id = ?", (event.event_id,))
            if cursor.fetchone():
                logger.debug(f"Skipping duplicate event_id={event.event_id}")
                return

            agent._ensure_customer_exists(conn, customer_id, company_name)

            # --- FRESHNESS GUARD ---
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
            cursor.execute("""
                INSERT INTO customer_risk_profile (
                    customer_id, id, company_name, financial_risk, litigation_risk,
                    combined_risk, severity, financial_source, litigation_source,
                    confidence, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            )
            conn.commit()

            logger.info(
                f"[DBAgent] Stored aggregated risk: customer={customer_id}, combined={combined_risk}, "
                f"financial={financial_risk if financial_risk is not None else '(preserved)'}"
            )

            # --- MARK EVENT AS PROCESSED (bounded eviction) ---
            if event_id:
                if len(agent._processed_risk_events) >= agent.MAX_PROCESSED_IDS:
                    agent._processed_risk_events.popitem(last=False)
                agent._processed_risk_events[event_id] = True

            # Invalidate cache
            QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})

        finally:
            agent._finalize_handler_connection(conn)


# ─────────────────────────────────────────────────────────────────────────────
# risk.scored
# ─────────────────────────────────────────────────────────────────────────────

def handle_risk_scored(agent: "DBAgent", event: Event) -> None:
    """Persist invoice-level risk scores and SHAP explanations.

    Writes to two tables:
    1. customers.risk_score  — scalar for downstream use.
    2. risk_explanations     — full SHAP attribution for regulatory audit.
    """
    data = event.payload or {}
    customer_id = data.get("customer_id")
    invoice_id = data.get("invoice_id")
    if not customer_id:
        logger.warning("risk.scored event missing customer_id, skipping")
        return

    try:
        risk_score = float(data.get("risk_score") or 0.0)
    except (TypeError, ValueError):
        logger.warning("[DBAgent] Invalid risk_score for %s: %r", customer_id, data.get("risk_score"))
        return

    # ── SHAP fields (present when emitted by PaymentPredictionAgent v1.1+) ──
    shap_values = data.get("shap_values")   # dict or None
    shap_top_driver = data.get("shap_top_driver")
    shap_sum = data.get("shap_sum")
    shap_baseline = float(data.get("shap_baseline") or 0.0)
    shap_rating_adj = float(data.get("shap_rating_adjustment") or 0.0)
    shap_litig_adj = float(data.get("shap_litigation_adjustment") or 0.0)
    risk_level = data.get("risk_level")
    reasons = data.get("reasons")  # list or None

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
                data.get("customer_name") or data.get("company_name") or data.get("name"),
            )

            # 1. Update scalar risk_score on the customer row
            cursor.execute(
                "UPDATE customers SET risk_score = ?, updated_at = ? WHERE customer_id = ?",
                (risk_score, now, customer_id),
            )

            # 2. Upsert SHAP explanation into risk_explanations (if invoice_id present)
            if invoice_id:
                cursor.execute("""
                    INSERT INTO risk_explanations (
                        invoice_id, customer_id, risk_score, risk_level,
                        shap_top_driver, shap_values, shap_sum,
                        shap_baseline, shap_rating_adjustment, shap_litigation_adjustment,
                        reasons, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(invoice_id) DO UPDATE SET
                        risk_score              = excluded.risk_score,
                        risk_level              = excluded.risk_level,
                        shap_top_driver         = excluded.shap_top_driver,
                        shap_values             = excluded.shap_values,
                        shap_sum                = excluded.shap_sum,
                        shap_baseline           = excluded.shap_baseline,
                        shap_rating_adjustment  = excluded.shap_rating_adjustment,
                        shap_litigation_adjustment = excluded.shap_litigation_adjustment,
                        reasons                 = excluded.reasons,
                        updated_at              = excluded.updated_at
                """, (
                    invoice_id,
                    customer_id,
                    risk_score,
                    risk_level,
                    shap_top_driver,
                    json.dumps(shap_values) if shap_values is not None else None,
                    shap_sum,
                    shap_baseline,
                    shap_rating_adj,
                    shap_litig_adj,
                    json.dumps(reasons) if reasons is not None else None,
                    now,
                    now,
                ))
                logger.info(
                    "[DBAgent] SHAP explanation stored: invoice=%s customer=%s "
                    "risk=%.4f top_driver=%s",
                    invoice_id, customer_id, risk_score, shap_top_driver,
                )

            cursor.execute(
                "INSERT INTO event_log (event_id, event_type, processed_at) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, now),
            )
            conn.commit()
            QueryClient.query("invalidate_customer_cache", {"customer_id": customer_id})
        finally:
            agent._finalize_handler_connection(conn)
