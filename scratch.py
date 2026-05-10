import sqlite3
from datetime import datetime

conn = sqlite3.connect('acis.db')
conn.row_factory = sqlite3.Row

customer_id = 'cust_00005'
invoices = list(conn.execute('SELECT invoice_id, due_date, status FROM invoices WHERE customer_id = ?', (customer_id,)))
invoices = [dict(i) for i in invoices]

invoice_ids = [i['invoice_id'] for i in invoices]
placeholders = ','.join('?' * len(invoice_ids))
payments = list(conn.execute(f'SELECT payment_id, invoice_id, payment_date, amount FROM payments WHERE invoice_id IN ({placeholders})', invoice_ids))
payments = [dict(p) for p in payments]

payment_map = {}
for payment in payments:
    invoice_id = payment.get('invoice_id')
    if invoice_id:
        payment_map.setdefault(invoice_id, []).append(payment)

paid_invoices = [inv for inv in invoices if inv.get('status') in ('paid', 'completed')]

delays = []
on_time_count = 0

for invoice in paid_invoices:
    invoice_id = invoice.get('invoice_id')
    due_date_str = invoice.get('due_date')
    payment_list = payment_map.get(invoice_id, [])

    print(f'Checking {invoice_id}: status={invoice.get("status")}, payments={len(payment_list)}')

    if not payment_list or not due_date_str:
        print('Skipped due to missing payment or due_date')
        continue

    latest_payment = max(payment_list, key=lambda x: x.get('payment_date', ''))
    payment_date_str = latest_payment.get('payment_date')

    due_date = datetime.fromisoformat(due_date_str.replace('Z', '+00:00')).replace(tzinfo=None)
    payment_date = datetime.fromisoformat(payment_date_str.replace('Z', '+00:00')).replace(tzinfo=None)

    delay_days = max((payment_date - due_date).days, 0)
    delays.append(delay_days)

    if (payment_date - due_date).days <= 0:
        on_time_count += 1
        
    print(f'  due={due_date}, payment={payment_date}, delay={delay_days}')

avg_delay = sum(delays) / len(delays) if delays else 0.0
on_time_ratio = on_time_count / len(paid_invoices) if paid_invoices else 0.0

print(f'Final: avg_delay={avg_delay}, on_time_ratio={on_time_ratio}')
