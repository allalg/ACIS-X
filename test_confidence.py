import sqlite3
conn = sqlite3.connect('acis.db')
c = conn.cursor()
c.execute('SELECT company_name, confidence, financial_source, litigation_source FROM customer_risk_profile LIMIT 5')
data = c.fetchall()
print([desc[0] for desc in c.description])
for row in data:
    print(row)
conn.close()
