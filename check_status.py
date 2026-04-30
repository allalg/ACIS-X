import sqlite3
db_path = 'acis.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT customer_id, company_name, analysis_status FROM external_litigation")
rows = cursor.fetchall()
# Print the results
for row in rows:
    print(f"{row[0]} | {row[1]} | {row[2]}")
conn.close()
