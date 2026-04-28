import psutil
import os

current = os.getpid()
for p in psutil.process_iter(['pid', 'name', 'cmdline']):
    name = p.info.get('name') or ''
    if 'python' in name.lower() and p.pid != current:
        cmdline = p.info.get('cmdline') or []
        cmd_str = ' '.join(cmdline).lower()
        if "acis" in cmd_str or "multiprocessing" in cmd_str:
            print(f"PID {p.pid}: {cmd_str}")
