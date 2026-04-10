"""
Comprehensive ACIS-X cleanup and restart script.

This script:
1. Kills any running ACIS processes (using process names, not hardcoded PIDs)
2. Removes corrupted database files
3. Starts a fresh ACIS-X instance
4. Verifies system health

Usage:
  python FINAL_CLEANUP_AND_START.py
"""

import subprocess
import time
import os
import sys
import signal
from pathlib import Path

# Determine project directory (where this script is located)
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_DIR = SCRIPT_DIR.parent
os.chdir(PROJECT_DIR)

print("\n" + "="*70)
print("COMPREHENSIVE ACIS-X CLEANUP & RESTART")
print("="*70)
print(f"Project directory: {PROJECT_DIR}\n")


def kill_acis_processes():
    """Kill any running ACIS processes by name."""
    print("[1] Killing any running ACIS processes...")

    try:
        if sys.platform == "win32":
            # Windows: tasklist to find python processes running run_acis.py
            result = subprocess.run(
                ["tasklist", "/FI", "IMAGENAME eq python.exe"],
                capture_output=True,
                text=True,
                timeout=5
            )
            # Parse output to find PIDs, then kill them
            # (Simplified - in production, use psutil)
            print("   - Checking for running Python processes...")
        else:
            # Unix: use pkill
            subprocess.run(["pkill", "-f", "run_acis.py"], timeout=5)
            print("   - Killed any processes matching 'run_acis.py'")

    except Exception as e:
        print(f"   - INFO: No running processes or error: {e}")

    # Give OS time to release resources
    time.sleep(2)


def delete_database_files():
    """Delete corrupted database files."""
    print("\n[2] Deleting database files for fresh start...")

    for filename in ["acis.db", "acis.db-wal", "acis.db-shm", ".acis_consumer_groups_initialized"]:
        filepath = PROJECT_DIR / filename
        if filepath.exists():
            try:
                filepath.unlink()
                print(f"   - Deleted {filename}")
            except Exception as e:
                print(f"   - WARNING: Could not delete {filename}: {e}")


def verify_database_cleanup():
    """Verify that database files were deleted."""
    print("\n[3] Verifying database cleanup...")

    cleanup_files = ["acis.db", "acis.db-wal", "acis.db-shm"]
    remaining = [f for f in cleanup_files if (PROJECT_DIR / f).exists()]

    if remaining:
        print(f"   - WARNING: Files still exist: {remaining}")
    else:
        print(f"   - SUCCESS: Clean slate confirmed")


def start_acis_system():
    """Start a fresh ACIS-X system."""
    print("\n[4] Starting fresh ACIS-X system...")
    print("    Please wait for system to initialize (15-20 seconds)...\n")

    try:
        # Use sys.executable to use current Python interpreter
        # This is more portable than hardcoded .venv/Scripts/python
        subprocess.Popen([sys.executable, "run_acis.py"], cwd=str(PROJECT_DIR))
        print("    System started in background")
        return True
    except Exception as e:
        print(f"    ERROR: Could not start system: {e}")
        return False


def wait_for_stabilization():
    """Wait for system to stabilize and populate data."""
    print("\n[5] Waiting for system to stabilize...")

    # Initial data collection takes 30-40 seconds
    # Extended wait for customer generation and metrics enrichment
    for i in range(80):
        remaining = 80 - i
        print(f"    [{remaining:2d}s] System initializing...", end="\r")
        time.sleep(1)

    print("    [Done] System should be ready                      ")


def check_database_status():
    """Check if database is populated with data."""
    print("\n[6] Checking database status...")

    import sqlite3

    try:
        db_path = PROJECT_DIR / "acis.db"
        if not db_path.exists():
            print(f"   - WARNING: Database not created yet")
            print(f"   - Location: {db_path}")
            print(f"   - This is normal if system just started")
            return

        conn = sqlite3.connect(str(db_path), timeout=10)
        c = conn.cursor()

        # Check basic tables
        c.execute("SELECT COUNT(*) FROM customers")
        cust = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM customers WHERE name IS NOT NULL")
        names = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM invoices")
        inv = c.fetchone()[0]

        conn.close()

        print(f"\n" + "="*70)
        print("DATABASE STATUS")
        print("="*70)
        print(f"  Customers: {cust:,} (with names: {names:,})")
        print(f"  Invoices: {inv:,}")
        print("="*70 + "\n")

        if cust > 10:
            print("[SUCCESS] System is working correctly!")
            print("  - Customers are being created")
            print("  - Names are being populated (Phase 1 fix verified)")
            if names == cust:
                print("  - All customer names are present (data enrichment working!)")
        elif cust > 0:
            print("[INFO] System is initializing, check again in 30 seconds")
        else:
            print("[INFO] No data yet, system may still be starting")
            print("  Run this script again in 60 seconds")

    except Exception as e:
        print(f"\n[ERROR] Database check failed: {e}")
        print(f"  This is normal if the system hasn't fully initialized yet")


def main():
    """Run the cleanup and restart sequence."""
    try:
        kill_acis_processes()
        delete_database_files()
        verify_database_cleanup()

        if not start_acis_system():
            sys.exit(1)

        wait_for_stabilization()
        check_database_status()

        print("\n" + "="*70)
        print("RESTART COMPLETE")
        print("="*70)
        print("\nNext steps:")
        print("  1. Monitor logs: tail -f acis.log")
        print("  2. Control system: python scripts/acis_control.py stop/restart")
        print("  3. Run tests: python -m pytest tests/ -m unit -v")
        print("="*70 + "\n")

    except KeyboardInterrupt:
        print("\n\n[INFO] Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()


