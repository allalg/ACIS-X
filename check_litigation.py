import sqlite3
db_path = 'acis.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT id, customer_id, company_name, source, created_at FROM external_litigation")
rows = cursor.fetchall()
print("id | customer_id | company_name | source | created_at")
print("-" * 80)
for row in rows:
    print(f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]}")
conn.close()
