from dotenv import load_dotenv
load_dotenv(".env")
load_dotenv(".env.cloud", override=True)

from utils.db_connection import connect
from runtime.topic_manager import TopicAdmin
from config.settings import get_settings

get_settings.cache_clear()
s = get_settings()

conn = connect()
try:
    cur = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' ORDER BY table_name"
    )
    tables = [r[0] for r in cur.fetchall()]
    print("tables", len(tables))
    for t in tables:
        print(" -", t)
finally:
    conn.close()

admin = TopicAdmin(
    bootstrap_servers=s.kafka_bootstrap_servers,
    backend=s.kafka_backend,
    security_protocol=s.kafka_security_protocol or "PLAINTEXT",
    sasl_mechanism=s.kafka_sasl_mechanism,
    sasl_username=s.kafka_sasl_username,
    sasl_password=s.kafka_sasl_password,
)
results = admin.create_all_acis_topics()
created = sum(1 for v in results.values() if v)
print("kafka topics created/ok:", created, "/", len(results))
for name, ok in sorted(results.items()):
    if not ok:
        print(" FAIL", name)
