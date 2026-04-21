import sqlite3
conn = sqlite3.connect('acis.db')
c = conn.cursor()
c.execute('SELECT * FROM external_financials WHERE company_name="Infosys Ltd"')
data = c.fetchall()
print([desc[0] for desc in c.description])
for row in data:
    print(row)
conn.close()
