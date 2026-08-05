from __future__ import annotations
from datetime import timezone

import sqlite3
from contextlib import contextmanager
from datetime import datetime

from .config import load_settings


@contextmanager
def get_connection():
    import os
    settings = load_settings()
    raw_path = settings.db_path or "acis.db"
    abs_path = os.path.abspath(raw_path)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    
    conn = sqlite3.connect(abs_path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict]:
    return [dict(row) for row in rows]


def get_dashboard_summary() -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + 'Z'
    try:
        with get_connection() as conn:
            cursor = conn.cursor()

            total_customers = cursor.execute('SELECT COUNT(*) FROM customers').fetchone()[0]
            total_invoices = cursor.execute('SELECT COUNT(*) FROM invoices').fetchone()[0]
            open_invoices = cursor.execute("SELECT COUNT(*) FROM invoices WHERE status != 'paid'").fetchone()[0]
            overdue_invoices = cursor.execute("SELECT COUNT(*) FROM invoices WHERE status = 'overdue'").fetchone()[0]
            total_outstanding = cursor.execute(
                'SELECT COALESCE(SUM(total_amount - paid_amount), 0) FROM invoices WHERE status != \'paid\''
            ).fetchone()[0]

            metrics = cursor.execute(
                'SELECT COALESCE(AVG(avg_delay), 0), COALESCE(AVG(on_time_ratio), 0) FROM customer_metrics'
            ).fetchone()

            db_on_time_ratio = float(metrics[1] or 0)
            if db_on_time_ratio <= 0 and total_invoices > 0:
                on_time_ratio = float((total_invoices - overdue_invoices) / total_invoices)
            elif total_invoices == 0:
                on_time_ratio = 1.0
            else:
                on_time_ratio = db_on_time_ratio

            risk_counts = cursor.execute(
                """
                SELECT
                  COALESCE(SUM(CASE WHEN severity IN ('high', 'critical') THEN 1 ELSE 0 END), 0) AS high_risk_count,
                  COALESCE(SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END), 0) AS medium_risk_count,
                  COALESCE(SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END), 0) AS low_risk_count
                FROM customer_risk_profile
                """
            ).fetchone()

        return {
            'timestamp': now,
            'total_customers': total_customers,
            'total_invoices': total_invoices,
            'open_invoices': open_invoices,
            'overdue_invoices': overdue_invoices,
            'total_outstanding': float(total_outstanding or 0),
            'avg_delay': float(metrics[0] or 0),
            'on_time_ratio': on_time_ratio,
            'high_risk_count': int(risk_counts[0] or 0),
            'medium_risk_count': int(risk_counts[1] or 0),
            'low_risk_count': int(risk_counts[2] or 0),
        }
    except Exception:
        return {
            'timestamp': now,
            'total_customers': 0,
            'total_invoices': 0,
            'open_invoices': 0,
            'overdue_invoices': 0,
            'total_outstanding': 0.0,
            'avg_delay': 0.0,
            'on_time_ratio': 1.0,
            'high_risk_count': 0,
            'medium_risk_count': 0,
            'low_risk_count': 0,
        }


def get_customers(search: str | None = None) -> list[dict]:
    query = """
        SELECT
            c.customer_id,
            c.name,
            c.credit_limit,
            c.risk_score,
            c.status,
            COALESCE(rp.combined_risk, c.risk_score, 0) AS combined_risk,
            COALESCE(rp.severity, 'low') AS severity,
            COALESCE(cm.total_outstanding, 0) AS total_outstanding,
            COALESCE(cm.avg_delay, 0) AS avg_delay,
            COALESCE(cm.on_time_ratio, 0) AS on_time_ratio,
            COALESCE(rp.updated_at, c.updated_at, c.created_at) AS updated_at
        FROM customers c
        LEFT JOIN customer_risk_profile rp ON rp.customer_id = c.customer_id
        LEFT JOIN customer_metrics cm ON cm.customer_id = c.customer_id
    """
    params: list[str] = []

    if search:
        query += ' WHERE c.name LIKE ? OR c.customer_id LIKE ?'
        params.extend([f'%{search}%', f'%{search}%'])

    query += ' ORDER BY combined_risk DESC'

    try:
        with get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return rows_to_dicts(rows)
    except Exception:
        return []


def get_customer_by_id(customer_id: str) -> dict | None:
    query = """
        SELECT
            c.customer_id,
            c.name,
            c.credit_limit,
            c.risk_score,
            COALESCE(cm.total_outstanding, 0) AS total_outstanding,
            COALESCE(cm.avg_delay, 0) AS avg_delay,
            COALESCE(cm.on_time_ratio, 0) AS on_time_ratio,
            COALESCE(cm.aging_current, 0) AS aging_current,
            COALESCE(cm.aging_1_30, 0) AS aging_1_30,
            COALESCE(cm.aging_31_60, 0) AS aging_31_60,
            COALESCE(cm.aging_61_90, 0) AS aging_61_90,
            COALESCE(cm.aging_90_plus, 0) AS aging_90_plus,
            COALESCE((SELECT COUNT(*) FROM invoices i WHERE i.customer_id = c.customer_id AND i.status = 'overdue'), 0) AS overdue_count,
            COALESCE(cm.last_payment_date, c.updated_at) AS last_payment_date,
            COALESCE(rp.financial_risk, 0) AS financial_risk,
            COALESCE(rp.litigation_risk, 0) AS litigation_risk,
            COALESCE(rp.combined_risk, c.risk_score, 0) AS combined_risk,
            COALESCE(rp.severity, 'low') AS severity,
            COALESCE(rp.confidence, 0.5) AS confidence,
            COALESCE(rp.updated_at, c.updated_at, c.created_at) AS updated_at
        FROM customers c
        LEFT JOIN customer_metrics cm ON cm.customer_id = c.customer_id
        LEFT JOIN customer_risk_profile rp ON rp.customer_id = c.customer_id
        WHERE c.customer_id = ?
        LIMIT 1
    """

    with get_connection() as conn:
        row = conn.execute(query, (customer_id,)).fetchone()
        if not row:
            return None
        return dict(row)


def get_invoices(customer_id: str | None, status: str | None, page: int, limit: int) -> tuple[list[dict], int]:
    where_clauses = []
    params: list[str | int] = []

    if customer_id:
        where_clauses.append('i.customer_id = ?')
        params.append(customer_id)
    if status:
        where_clauses.append('i.status = ?')
        params.append(status)

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ''

    query = f"""
        SELECT
            i.invoice_id,
            i.customer_id,
            COALESCE(c.name, 'Unknown') AS customer_name,
            COALESCE(i.total_amount, 0) AS total_amount,
            COALESCE(i.paid_amount, 0) AS paid_amount,
            'INR' AS currency,
            i.issued_date,
            i.due_date,
            i.status,
            CASE
              WHEN i.status = 'overdue' THEN CAST(julianday('now') - julianday(i.due_date) AS INTEGER)
              ELSE 0
            END AS days_overdue,
            i.created_at,
            i.updated_at
        FROM invoices i
        LEFT JOIN customers c ON c.customer_id = i.customer_id
        {where}
        ORDER BY i.due_date ASC
        LIMIT ? OFFSET ?
    """

    count_query = f'SELECT COUNT(*) FROM invoices i {where}'
    offset = (page - 1) * limit

    with get_connection() as conn:
        total = conn.execute(count_query, params).fetchone()[0]
        rows = conn.execute(query, [*params, limit, offset]).fetchall()
        return rows_to_dicts(rows), int(total)


def get_payments(
    customer_id: str | None,
    invoice_id: str | None,
    page: int,
    limit: int,
) -> tuple[list[dict], int]:
    where_clauses = []
    params: list[str | int] = []

    if customer_id:
        where_clauses.append('p.customer_id = ?')
        params.append(customer_id)
    if invoice_id:
        where_clauses.append('p.invoice_id = ?')
        params.append(invoice_id)

    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ''

    query = f"""
        SELECT
            p.payment_id,
            p.invoice_id,
            p.customer_id,
            COALESCE(c.name, 'Unknown') AS customer_name,
            COALESCE(p.amount, 0) AS amount,
            'INR' AS currency,
            p.payment_date,
            'bank_transfer' AS payment_method,
            'applied' AS status,
            p.payment_id AS reference,
            p.created_at
        FROM payments p
        LEFT JOIN customers c ON c.customer_id = p.customer_id
        {where}
        ORDER BY p.payment_date DESC
        LIMIT ? OFFSET ?
    """

    count_query = f'SELECT COUNT(*) FROM payments p {where}'
    offset = (page - 1) * limit

    with get_connection() as conn:
        total = conn.execute(count_query, params).fetchone()[0]
        rows = conn.execute(query, [*params, limit, offset]).fetchall()
        return rows_to_dicts(rows), int(total)


def get_risk_profiles() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                customer_id,
                COALESCE(company_name, customer_id) AS company_name,
                COALESCE(financial_risk, 0) AS financial_risk,
                COALESCE(litigation_risk, 0) AS litigation_risk,
                COALESCE(combined_risk, 0) AS combined_risk,
                COALESCE(severity, 'low') AS severity,
                COALESCE(confidence, 0.5) AS confidence,
                COALESCE(updated_at, created_at) AS updated_at
            FROM customer_risk_profile
            ORDER BY combined_risk DESC
            """
        ).fetchall()
        return rows_to_dicts(rows)


def get_customer_metrics() -> list[dict]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                cm.customer_id,
                COALESCE(c.name, cm.customer_id) AS company_name,
                COALESCE(cm.total_outstanding, 0) AS total_outstanding,
                COALESCE(cm.avg_delay, 0) AS avg_delay,
                COALESCE(cm.on_time_ratio, 0) AS on_time_ratio,
                COALESCE(cm.aging_current, 0) AS aging_current,
                COALESCE(cm.aging_1_30, 0) AS aging_1_30,
                COALESCE(cm.aging_31_60, 0) AS aging_31_60,
                COALESCE(cm.aging_61_90, 0) AS aging_61_90,
                COALESCE(cm.aging_90_plus, 0) AS aging_90_plus,
                COALESCE(cm.last_payment_date, cm.updated_at) AS last_payment_date,
                cm.updated_at
            FROM customer_metrics cm
            LEFT JOIN customers c ON c.customer_id = cm.customer_id
            ORDER BY cm.total_outstanding DESC
            """
        ).fetchall()
        return rows_to_dicts(rows)


def get_customer_collections(customer_id: str) -> list[dict]:
    query = """
        SELECT id, invoice_id, action, stage, priority, reason, timestamp
        FROM collections_log
        WHERE customer_id = ?
        ORDER BY timestamp DESC
    """
    with get_connection() as conn:
        rows = conn.execute(query, (customer_id,)).fetchall()
        return rows_to_dicts(rows)


def get_customer_risk_explanation(customer_id: str) -> dict | None:
    query = """
        SELECT invoice_id, risk_score, risk_level, shap_top_driver, shap_values, reasons, updated_at
        FROM risk_explanations
        WHERE customer_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
    """
    with get_connection() as conn:
        row = conn.execute(query, (customer_id,)).fetchone()
        if not row:
            return None
        return dict(row)


def get_customer_external_intelligence(customer_id: str) -> dict | None:
    query = """
        SELECT 
            l.litigation_risk, l.severity, l.case_count, l.case_types, l.cases, l.evidence, l.source as litigation_source, l.confidence,
            f.pe, f.roe, f.roce, f.operating_margin, f.interest_coverage, f.source as financial_source,
            f."debt (₹ Cr.)" as debt, f."market_cap (₹ Cr.)" as market_cap, f.sales_growth, f.profit_growth
        FROM external_litigation l
        LEFT JOIN customers c ON c.customer_id = l.customer_id
        LEFT JOIN external_financials f ON f.company_name = c.name
        WHERE l.customer_id = ?
        LIMIT 1
    """
    with get_connection() as conn:
        row = conn.execute(query, (customer_id,)).fetchone()
        if not row:
            return None
        return dict(row)
