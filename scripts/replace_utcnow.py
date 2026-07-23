import os
import re

def process_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if 'datetime.now(timezone.utc).replace(tzinfo=None)' not in content:
        return False

    # Replace datetime.now(timezone.utc).replace(tzinfo=None) with datetime.now(timezone.utc).replace(tzinfo=None)
    new_content = content.replace('datetime.now(timezone.utc).replace(tzinfo=None)', 'datetime.now(timezone.utc).replace(tzinfo=None)')

    # Ensure timezone is imported
    # It might be `from datetime import datetime`, we want `from datetime import datetime, timezone`
    if 'timezone' not in new_content:
        lines = new_content.split('\n')
        for i, line in enumerate(lines):
            # Try to match `from datetime import ...`
            if re.match(r'^from\s+datetime\s+import\s+', line):
                if 'timezone' not in line:
                    lines[i] = line + ', timezone'
                break
        else:
            # If not found, just add it after the imports or at the top
            for i, line in enumerate(lines):
                if line.startswith('import ') or line.startswith('from '):
                    lines.insert(i, 'from datetime import timezone')
                    break
            else:
                lines.insert(0, 'from datetime import timezone')
        new_content = '\n'.join(lines)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    return True


if __name__ == "__main__":
    count = 0
    for root, dirs, files in os.walk('.'):
        # Skip standard ignore directories
        if '.venv' in root or '.git' in root or '__pycache__' in root or '.pytest_cache' in root:
            continue
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                if process_file(filepath):
                    print(f"Updated {filepath}")
                    count += 1
    print(f"Total files updated: {count}")
