import sqlite3
conn = sqlite3.connect('acis.db')
c = conn.cursor()
c.execute('SELECT sql FROM sqlite_master WHERE type="table" AND name="external_financials"')
print(c.fetchone()[0])
c.execute('SELECT * FROM external_financials LIMIT 5')
data = c.fetchall()
print([desc[0] for desc in c.description])
for row in data:
    print(row)
conn.close()
