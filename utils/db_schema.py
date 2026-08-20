"""
Apply ACIS schema for the active dialect (Postgres or SQLite).
"""

from __future__ import annotations

import logging
from typing import Any

from utils.db_connection import is_postgres

logger = logging.getLogger(__name__)

# Postgres DDL — same logical schema as SQLite without PRAGMA / SQLite-only types.
POSTGRES_SCHEMA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS event_log (
        event_id TEXT PRIMARY KEY,
        event_type TEXT,
        processed_at TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_event_log_time
    ON event_log (processed_at)
    """,
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        name TEXT,
        credit_limit DOUBLE PRECISION DEFAULT 0,
        risk_score DOUBLE PRECISION DEFAULT 0,
        status TEXT DEFAULT 'active',
        created_at TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_name_unique
    ON customers (name)
    WHERE name IS NOT NULL
    """,
    """
    CREATE TABLE IF NOT EXISTS invoices (
        invoice_id TEXT PRIMARY KEY,
        customer_id TEXT REFERENCES customers(customer_id),
        total_amount DOUBLE PRECISION,
        paid_amount DOUBLE PRECISION DEFAULT 0,
        issued_date TEXT,
        due_date TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_invoice_id ON invoices(invoice_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_invoice_customer ON invoices(customer_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
        payment_id TEXT PRIMARY KEY,
        invoice_id TEXT REFERENCES invoices(invoice_id),
        customer_id TEXT REFERENCES customers(customer_id),
        amount DOUBLE PRECISION,
        payment_date TEXT,
        created_at TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_payments_invoice ON payments(invoice_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS collections_log (
        id TEXT PRIMARY KEY,
        customer_id TEXT REFERENCES customers(customer_id),
        invoice_id TEXT,
        action TEXT,
        stage TEXT,
        priority TEXT,
        reason TEXT,
        timestamp TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS customer_metrics (
        customer_id TEXT PRIMARY KEY REFERENCES customers(customer_id),
        total_outstanding DOUBLE PRECISION DEFAULT 0,
        avg_delay DOUBLE PRECISION DEFAULT 0,
        on_time_ratio DOUBLE PRECISION DEFAULT 0,
        aging_current DOUBLE PRECISION DEFAULT 0,
        aging_1_30 DOUBLE PRECISION DEFAULT 0,
        aging_31_60 DOUBLE PRECISION DEFAULT 0,
        aging_61_90 DOUBLE PRECISION DEFAULT 0,
        aging_90_plus DOUBLE PRECISION DEFAULT 0,
        last_payment_date TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS external_financials (
        company_name TEXT PRIMARY KEY,
        ticker TEXT,
        pe DOUBLE PRECISION,
        roe DOUBLE PRECISION,
        roce DOUBLE PRECISION,
        "debt (₹ Cr.)" DOUBLE PRECISION,
        "market_cap (₹ Cr.)" DOUBLE PRECISION,
        sales_growth DOUBLE PRECISION,
        profit_growth DOUBLE PRECISION,
        operating_margin DOUBLE PRECISION,
        interest_coverage DOUBLE PRECISION,
        risk DOUBLE PRECISION,
        source TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS external_litigation (
        customer_id TEXT PRIMARY KEY,
        id TEXT UNIQUE,
        company_name TEXT,
        litigation_risk DOUBLE PRECISION,
        severity TEXT,
        case_count INTEGER,
        case_types TEXT,
        cases TEXT,
        evidence TEXT,
        source TEXT,
        confidence DOUBLE PRECISION,
        created_at TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_litigation_customer
    ON external_litigation(customer_id)
    """,
    """
    CREATE TABLE IF NOT EXISTS customer_risk_profile (
        customer_id TEXT PRIMARY KEY,
        id TEXT UNIQUE,
        company_name TEXT,
        financial_risk DOUBLE PRECISION,
        litigation_risk DOUBLE PRECISION,
        combined_risk DOUBLE PRECISION,
        severity TEXT,
        financial_source TEXT,
        litigation_source TEXT,
        confidence DOUBLE PRECISION,
        created_at TEXT,
        updated_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS risk_explanations (
        invoice_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL REFERENCES customers(customer_id),
        risk_score DOUBLE PRECISION NOT NULL,
        risk_level TEXT,
        shap_top_driver TEXT,
        shap_values TEXT,
        shap_sum DOUBLE PRECISION,
        shap_baseline DOUBLE PRECISION DEFAULT 0.0,
        shap_rating_adjustment DOUBLE PRECISION DEFAULT 0.0,
        shap_litigation_adjustment DOUBLE PRECISION DEFAULT 0.0,
        reasons TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_risk_exp_customer
    ON risk_explanations (customer_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_risk_exp_score
    ON risk_explanations (risk_score DESC)
    """,
]


def init_schema(conn: Any) -> None:
    """Create tables/indexes on an open connection."""
    # Lazy import avoids circular import via agents.storage.__init__ → DBAgent.
    from agents.storage.handlers.schema import (
        SCHEMA_DDL,
        SCHEMA_MIGRATIONS,
        SCHEMA_VERSION,
    )

    if is_postgres():
        for stmt in POSTGRES_SCHEMA_DDL:
            conn.execute(stmt)
        conn.commit()
        logger.info("Postgres schema applied (%s)", SCHEMA_VERSION)
        return

    for stmt in SCHEMA_DDL:
        conn.execute(stmt)
    for mig in SCHEMA_MIGRATIONS:
        try:
            conn.execute(mig)
        except Exception:
            pass
    conn.commit()
    logger.info("SQLite schema applied (%s)", SCHEMA_VERSION)
