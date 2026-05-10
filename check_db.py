import sqlite3
import json

conn = sqlite3.connect('acis.db')
conn.row_factory = sqlite3.Row

# ── 1. risk_explanations overview ──────────────────────────────────────
total = conn.execute('SELECT COUNT(*) FROM risk_explanations').fetchone()[0]
print('risk_explanations rows:', total)

if total > 0:
    text_cols    = ['shap_values', 'shap_top_driver', 'risk_level', 'reasons']
    numeric_cols = ['shap_sum', 'shap_baseline', 'shap_rating_adjustment', 'shap_litigation_adjustment']

    print('\nColumn null/zero audit:')
    for col in text_cols:
        n = conn.execute(f'SELECT COUNT(*) FROM risk_explanations WHERE {col} IS NULL').fetchone()[0]
        print(f'  {col:<40} nulls={n}')
    for col in numeric_cols:
        n = conn.execute(f'SELECT COUNT(*) FROM risk_explanations WHERE {col} IS NULL').fetchone()[0]
        z = conn.execute(f'SELECT COUNT(*) FROM risk_explanations WHERE {col} = 0').fetchone()[0]
        print(f'  {col:<40} nulls={n}  zeros={z}')

    print('\nSample rows (top 5 by risk_score):')
    rows = conn.execute('SELECT * FROM risk_explanations ORDER BY risk_score DESC LIMIT 5').fetchall()
    for r in rows:
        sv = json.loads(r['shap_values']) if r['shap_values'] else None
        print(f"  invoice={r['invoice_id']}  cust={r['customer_id']}  risk={r['risk_score']:.4f}  level={r['risk_level']}  top_driver={r['shap_top_driver']}")
        if sv:
            print(f'    shap={sv}')
        print(f"    reasons={r['reasons']}")
else:
    print('TABLE EMPTY - diagnosing...')
    scored    = conn.execute("SELECT COUNT(*) FROM event_log WHERE event_type='risk.scored'").fetchone()[0]
    predicted = conn.execute("SELECT COUNT(*) FROM event_log WHERE event_type='payment.risk.predicted'").fetchone()[0]
    any_risk  = conn.execute("SELECT COUNT(*) FROM event_log WHERE event_type LIKE '%risk%'").fetchone()[0]
    print(f'  event_log risk.scored events:            {scored}')
    print(f'  event_log payment.risk.predicted events: {predicted}')
    print(f'  event_log any-risk events:               {any_risk}')
    # Show what event types are present
    types = conn.execute("SELECT event_type, COUNT(*) as n FROM event_log GROUP BY event_type ORDER BY n DESC LIMIT 15").fetchall()
    print('\n  All event types in event_log:')
    for t in types:
        print(f'    {t[0]:<45} count={t[1]}')

# ── 2. customers overview ───────────────────────────────────────────────
print()
cust_total = conn.execute('SELECT COUNT(*) FROM customers').fetchone()[0]
cust_zero  = conn.execute('SELECT COUNT(*) FROM customers WHERE risk_score IS NULL OR risk_score = 0').fetchone()[0]
print(f'customers: total={cust_total}  risk_score null/zero={cust_zero}')

# ── 3. customer_risk_profile overview ──────────────────────────────────
crp = conn.execute('SELECT COUNT(*) FROM customer_risk_profile').fetchone()[0]
crp_null_fin = conn.execute('SELECT COUNT(*) FROM customer_risk_profile WHERE financial_risk IS NULL').fetchone()[0]
crp_null_lit = conn.execute('SELECT COUNT(*) FROM customer_risk_profile WHERE litigation_risk IS NULL').fetchone()[0]
print(f'customer_risk_profile: total={crp}  financial_risk_null={crp_null_fin}  litigation_risk_null={crp_null_lit}')

conn.close()
