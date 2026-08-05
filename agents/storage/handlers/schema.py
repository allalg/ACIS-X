"""
Database schema definitions for ACIS-X.

Centralizes all CREATE TABLE and CREATE INDEX statements that were previously
inline in DBAgent._init_database(). This makes the schema auditable, versionable,
and reusable (e.g. by migration tools).
"""

SCHEMA_VERSION = "v6"

# All DDL statements in execution order.
# Each entry is a single SQL statement.
SCHEMA_DDL = [
    # ── PRAGMA settings ────────────────────────────────────────────────────
    "PRAGMA journal_mode = DELETE",
    "PRAGMA synchronous = FULL",
    "PRAGMA cache_size=-32000",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA foreign_keys = ON",

    # ── event_log: idempotency tracking ────────────────────────────────────
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

    # ── customers ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        name TEXT,
        credit_limit REAL DEFAULT 0,
        risk_score REAL DEFAULT 0,
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

    # ── invoices ───────────────────────────────────────────────────────────
    """
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_invoice_id
    ON invoices(invoice_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_invoice_customer
    ON invoices(customer_id)
    """,

    # ── payments ───────────────────────────────────────────────────────────
    """
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_payments_invoice
    ON payments(invoice_id)
    """,

    # ── collections_log ────────────────────────────────────────────────────
    """
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
    """,

    # ── customer_metrics ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS customer_metrics (
        customer_id TEXT PRIMARY KEY,
        total_outstanding REAL DEFAULT 0,
        avg_delay REAL DEFAULT 0,
        on_time_ratio REAL DEFAULT 0,
        aging_current REAL DEFAULT 0,
        aging_1_30 REAL DEFAULT 0,
        aging_31_60 REAL DEFAULT 0,
        aging_61_90 REAL DEFAULT 0,
        aging_90_plus REAL DEFAULT 0,
        last_payment_date TEXT,
        updated_at TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )
    """,

    # ── external_financials ────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS external_financials (
        company_name TEXT PRIMARY KEY,
        pe REAL,
        roe REAL,
        roce REAL,
        "debt (₹ Cr.)" REAL,
        "market_cap (₹ Cr.)" REAL,
        sales_growth REAL,
        profit_growth REAL,
        operating_margin REAL,
        interest_coverage REAL,
        risk REAL,
        source TEXT,
        updated_at TEXT
    )
    """,

    # ── external_litigation ────────────────────────────────────────────────
    """
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
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_litigation_customer
    ON external_litigation(customer_id)
    """,

    # ── customer_risk_profile ──────────────────────────────────────────────
    """
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
    """,

    # ── risk_explanations (SHAP audit trail) ───────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS risk_explanations (
        invoice_id              TEXT PRIMARY KEY,
        customer_id             TEXT NOT NULL,
        risk_score              REAL NOT NULL,
        risk_level              TEXT,
        shap_top_driver         TEXT,
        shap_values             TEXT,
        shap_sum                REAL,
        shap_baseline           REAL DEFAULT 0.0,
        shap_rating_adjustment  REAL DEFAULT 0.0,
        shap_litigation_adjustment REAL DEFAULT 0.0,
        reasons                 TEXT,
        created_at              TEXT NOT NULL,
        updated_at              TEXT NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
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

# Schema migrations for existing databases — add columns if they don't exist.
SCHEMA_MIGRATIONS = [
    "ALTER TABLE customer_metrics ADD COLUMN aging_current REAL DEFAULT 0",
    "ALTER TABLE customer_metrics ADD COLUMN aging_1_30 REAL DEFAULT 0",
    "ALTER TABLE customer_metrics ADD COLUMN aging_31_60 REAL DEFAULT 0",
    "ALTER TABLE customer_metrics ADD COLUMN aging_61_90 REAL DEFAULT 0",
    "ALTER TABLE customer_metrics ADD COLUMN aging_90_plus REAL DEFAULT 0",
]
