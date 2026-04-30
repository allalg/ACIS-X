import sqlite3
db_path = 'acis.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT company_name, evidence FROM external_litigation")
rows = cursor.fetchall()
print("company_name | evidence")
print("-" * 80)
for row in rows:
    print(f"{row[0]} | {row[1]}")
conn.close()
