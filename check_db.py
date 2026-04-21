import sqlite3
conn = sqlite3.connect('acis.db')
c = conn.cursor()

c.execute('SELECT count(*) as total, count(name) as named, count(*)-count(name) as null_names FROM customers')
r = c.fetchone()
print(f'CUSTOMERS: total={r[0]} named={r[1]} null_names={r[2]}')

c.execute('SELECT count(*) FROM invoices')
print(f'INVOICES: {c.fetchone()[0]}')

c.execute('SELECT count(*) FROM payments')
print(f'PAYMENTS: {c.fetchone()[0]}')

c.execute('SELECT count(*) FROM collections_log')
print(f'COLLECTIONS_LOG: {c.fetchone()[0]}')

print()
print('--- Named customers (first 10) ---')
c.execute('SELECT customer_id, name, risk_score FROM customers WHERE name IS NOT NULL ORDER BY customer_id LIMIT 10')
for row in c.fetchall():
    print(f'  {row[0]}: {row[1]} (risk={row[2]})')

print()
print('--- NULL-named customer_ids ---')
c.execute('SELECT customer_id FROM customers WHERE name IS NULL LIMIT 5')
rows = c.fetchall()
if rows:
    for row in rows:
        print(f'  NULL NAME: {row[0]}')
else:
    print('  *** ZERO NULL NAMES - PERFECT! ***')

conn.close()
