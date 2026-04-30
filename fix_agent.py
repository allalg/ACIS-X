import re

with open('agents/intelligence/customer_state_agent.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Remove `self._persist_metrics(customer_id, metrics_dict)` block
content = re.sub(
    r'# Persist to DB\s+metrics_dict = {[^}]+}\s+self\._persist_metrics\(customer_id, metrics_dict\)',
    '# Persist to DB is handled by DBAgent',
    content,
    flags=re.MULTILINE
)

# Remove `def _persist_metrics...`
content = re.sub(
    r'    def _persist_metrics\(.*?$',
    '',
    content,
    flags=re.MULTILINE | re.DOTALL
)

with open('agents/intelligence/customer_state_agent.py', 'w', encoding='utf-8') as f:
    f.write(content)
