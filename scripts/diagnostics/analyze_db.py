import sqlite3

conn = sqlite3.connect('acis.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print('=== CUSTOMERS TABLE SCHEMA ===')
cur.execute('PRAGMA table_info(customers)')
for r in cur.fetchall(): print(dict(r))

print()
print('=== CUSTOMERS COUNT ===')
cur.execute('SELECT COUNT(*) FROM customers')
print(cur.fetchone()[0])

print()
print('=== CUSTOMERS (first 40) ===')
cur.execute('SELECT customer_id, name, credit_limit, status FROM customers LIMIT 40')
for r in cur.fetchall(): print(dict(r))

print()
print('=== NULL or empty names ===')
cur.execute("SELECT COUNT(*) FROM customers WHERE name IS NULL OR TRIM(name) = ''")
print(cur.fetchone()[0])
cur.execute("SELECT customer_id, name FROM customers WHERE name IS NULL OR TRIM(name) = '' LIMIT 10")
for r in cur.fetchall(): print(dict(r))

print()
print('=== Names like cust_ (ID used as name) ===')
cur.execute("SELECT COUNT(*) FROM customers WHERE name LIKE 'cust_%'")
print(cur.fetchone()[0])
cur.execute("SELECT customer_id, name FROM customers WHERE name LIKE 'cust_%' LIMIT 15")
for r in cur.fetchall(): print(dict(r))

print()
print('=== Duplicate names ===')
cur.execute('SELECT name, COUNT(*) as cnt FROM customers GROUP BY name HAVING cnt > 1 ORDER BY cnt DESC LIMIT 20')
for r in cur.fetchall(): print(dict(r))

print()
print('=== invoices count ===')
cur.execute('SELECT COUNT(*) FROM invoices')
print(cur.fetchone()[0])

print()
print('=== customer_risk_profile count and sample ===')
cur.execute('SELECT COUNT(*) FROM customer_risk_profile')
print(cur.fetchone()[0])
cur.execute('SELECT * FROM customer_risk_profile LIMIT 10')
for r in cur.fetchall(): print(dict(r))

print()
print('=== customer_metrics count ===')
cur.execute('SELECT COUNT(*) FROM customer_metrics')
print(cur.fetchone()[0])

print()
print('=== external_financials count ===')
cur.execute('SELECT COUNT(*) FROM external_financials')
print(cur.fetchone()[0])

print()
print('=== external_litigation count and sample ===')
cur.execute('SELECT COUNT(*) FROM external_litigation')
print(cur.fetchone()[0])
cur.execute('SELECT customer_id, company_name, litigation_risk, case_count, evidence FROM external_litigation LIMIT 5')
for r in cur.fetchall(): print(dict(r))

print()
print('=== collections_log count ===')
cur.execute('SELECT COUNT(*) FROM collections_log')
print(cur.fetchone()[0])

print()
print('=== Unique customers in invoices vs customers table ===')
cur.execute('SELECT COUNT(DISTINCT customer_id) FROM invoices')
print("Unique customer_ids in invoices:", cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM customers')
print("Total rows in customers table:", cur.fetchone()[0])

conn.close()
