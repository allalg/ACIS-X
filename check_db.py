import sqlite3

try:
    conn = sqlite3.connect('acis.db')
    c = conn.cursor()
    c.execute("SELECT * FROM external_litigation")
    for row in c.fetchall():
        print(row)
except Exception as e:
    print(e)
finally:
    if 'conn' in locals():
        conn.close()
