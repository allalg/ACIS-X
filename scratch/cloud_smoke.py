"""One-shot cloud smoke test: Postgres schema + Kafka connectivity."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")
load_dotenv(ROOT / ".env.cloud", override=True)

from utils.db_connection import connect, db_dialect
from utils.db_schema import init_schema, POSTGRES_SCHEMA_DDL


def main() -> int:
    print("dialect:", db_dialect())
    if db_dialect() != "postgres":
        print("ERROR: DATABASE_URL not set or not postgres")
        return 1

    conn = connect()
    try:
        init_schema(conn)
        cur = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' ORDER BY table_name"
        )
        tables = [r[0] for r in cur.fetchall()]
        print("tables:", len(tables), tables[:5], "...")
    finally:
        conn.close()

    from kafka.admin import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=os.environ["ACIS_KAFKA_BOOTSTRAP_SERVERS"],
        security_protocol=os.environ["ACIS_KAFKA_SECURITY_PROTOCOL"],
        sasl_mechanism=os.environ["ACIS_KAFKA_SASL_MECHANISM"],
        sasl_plain_username=os.environ["ACIS_KAFKA_SASL_USERNAME"],
        sasl_plain_password=os.environ["ACIS_KAFKA_SASL_PASSWORD"],
        request_timeout_ms=30000,
    )
    topics = sorted(admin.list_topics())
    print("kafka topics:", len(topics), topics[:8], ("..." if len(topics) > 8 else ""))
    admin.close()
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
