import sqlite3

conn = sqlite3.connect('acis.db')
c = conn.cursor()

print("--- CUSTOMER RISK PROFILE (External & Combined) ---")
c.execute('''
SELECT company_name, financial_risk, litigation_risk, combined_risk, severity, confidence 
FROM customer_risk_profile 
WHERE company_name LIKE '%Pvt Ltd%' OR company_name IN ('Infosys Ltd', 'Bajaj Auto Ltd', 'Tech Mahindra Ltd')
LIMIT 10
''')
columns = [desc[0] for desc in c.description]
print(f"{columns[0]:<30} {columns[1]:<15} {columns[2]:<15} {columns[3]:<15} {columns[4]:<10} {columns[5]:<10}")
for row in c.fetchall():
    print(f"{str(row[0]):<30} {str(row[1]):<15} {str(row[2]):<15} {str(row[3]):<15} {str(row[4]):<10} {str(row[5]):<10}")

print("\n--- CUSTOMERS TABLE (Overall Risk) ---")
c.execute('''
SELECT name, risk_score, credit_limit, status
FROM customers 
WHERE name LIKE '%Pvt Ltd%' OR name IN ('Infosys Ltd', 'Bajaj Auto Ltd', 'Tech Mahindra Ltd')
LIMIT 10
''')
columns2 = [desc[0] for desc in c.description]
print(f"{columns2[0]:<30} {columns2[1]:<15} {columns2[2]:<15} {columns2[3]:<10}")
for row in c.fetchall():
    print(f"{str(row[0]):<30} {str(row[1]):<15} {str(row[2]):<15} {str(row[3]):<10}")

conn.close()
