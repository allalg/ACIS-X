import sqlite3
db_path = 'acis.db'
db = sqlite3.connect(db_path)
cursor = db.cursor()
print("--- METRICS ---")
cursor.execute("SELECT customer_id, total_outstanding, avg_delay FROM customer_metrics")
for row in cursor.fetchall():
    print(row)
db.close()
