import sqlite3
conn = sqlite3.connect('acis.db')
conn.row_factory = sqlite3.Row
c = conn.cursor()
c.execute("SELECT company_name, litigation_risk, evidence FROM external_litigation WHERE company_name LIKE '%Dream%'")
print([dict(r) for r in c.fetchall()])
