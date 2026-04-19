import sqlite3, os, re
from collections import Counter

print('=== DB DIAGNOSTICS ===')
db = 'acis.db'
if not os.path.exists(db):
    print('acis.db NOT FOUND')
else:
    try:
        conn = sqlite3.connect(db, timeout=5)
        c = conn.cursor()
        
        tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
        print('Table Row Counts:')
        for t in tables:
            try:
                n = c.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
                print(f'  {t}: {n}')
            except Exception as e:
                print(f'  {t}: ERROR {e}')
                
        print('\nIntegrity Checks:')
        checks = [
            ("SELECT COUNT(*) FROM customers WHERE name IS NULL", 'customers with NULL name'),
            ("SELECT COUNT(*) FROM invoices WHERE total_amount IS NULL", 'invoices with NULL total_amount'),
            ("SELECT COUNT(*) FROM invoices WHERE status IS NULL", 'invoices with NULL status'),
            ("SELECT COUNT(*) FROM payments WHERE amount IS NULL OR amount <= 0", 'payments with NULL/zero/negative amount'),
            ("SELECT COUNT(*) FROM customer_metrics WHERE on_time_ratio IS NULL", 'metrics with NULL on_time_ratio'),
            ("SELECT COUNT(*) FROM collections_log WHERE customer_id IS NULL", 'collections_log with NULL customer_id')
        ]
        for q, label in checks:
            try:
                n = c.execute(q).fetchone()[0]
                print(f'  {label}: {n}')
            except Exception as e:
                print(f'  {label}: ERROR {e}')
        conn.close()
    except Exception as e:
        print(f'DB Connection Error: {e}')

print('\n=== LOG DIAGNOSTICS ===')
try:
    if os.path.exists('acis.log'):
        lines = open('acis.log', encoding='utf-8', errors='replace').readlines()
        print(f'Total log lines: {len(lines)}')
        
        errors = Counter()
        warnings = Counter()
        
        for l in lines:
            if 'ERROR' in l or 'FOREIGN KEY' in l or 'Traceback' in l:
                # Extract a summarized key for the error
                key = l.split(':', 3)[-1].strip()[:100] if ':' in l else l.strip()[:100]
                errors[key] += 1
            elif 'WARNING' in l:
                key = l.split(']', 1)[-1].strip()[:100] if ']' in l else l.split(':', 3)[-1].strip()[:100]
                warnings[key] += 1
                
        print('\nTop Errors:')
        for msg, cnt in errors.most_common(15):
            print(f'  {cnt:4d}x | {msg}')
            
        print('\nTop Warnings:')
        for msg, cnt in warnings.most_common(15):
            print(f'  {cnt:4d}x | {msg}')
            
        # Find the first few full error tracebacks/messages for the top errors
        print('\nSample Error Contexts:')
        top_error_keys = [k for k, _ in errors.most_common(3)]
        shown = set()
        for i, l in enumerate(lines):
            for k in top_error_keys:
                if k in l and k not in shown:
                    print(f'\n--- Context for: {k} ---')
                    start = max(0, i - 5)
                    end = min(len(lines), i + 5)
                    for j in range(start, end):
                        marker = '>>>' if j == i else '   '
                        print(f'{marker} L{j+1}: {lines[j].rstrip()}')
                    shown.add(k)
                    break
    else:
        print('acis.log NOT FOUND')
except Exception as e:
    print(f'Log Parsing Error: {e}')
