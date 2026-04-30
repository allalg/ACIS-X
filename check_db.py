import sqlite3
import sys
import os

db_path = 'acis.db'
if not os.path.exists(db_path):
    print("acis.db does not exist yet.")
    sys.exit(0)

try:
    db = sqlite3.connect(db_path)
    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for t in tables:
        table_name = t[0]
        cursor.execute(f"SELECT count(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Table {table_name}: {count} rows")
except Exception as e:
    print(f"Error reading DB: {e}")
