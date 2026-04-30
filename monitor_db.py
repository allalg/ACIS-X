import sqlite3
import time

db_path = 'acis.db'

for _ in range(10):
    try:
        db = sqlite3.connect(db_path)
        cursor = db.cursor()
        cursor.execute("SELECT count(*) FROM customers")
        cust = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM invoices")
        inv = cursor.fetchone()[0]
        print(f"Time: {time.strftime('%X')} | Customers: {cust} | Invoices: {inv}")
        db.close()
    except Exception as e:
        print(e)
    time.sleep(2)
