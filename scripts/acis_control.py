#!/usr/bin/env python
"""
ACIS-X Control Script - Easy start/stop/status management

Usage:
  python acis_control.py start     - Start the system
  python acis_control.py stop      - Stop the system gracefully
  python acis_control.py status    - Check if system is running
  python acis_control.py restart   - Stop then start
"""

import os
import sys
import time
import subprocess
import signal
from pathlib import Path

class ACISController:
    def __init__(self):
        self.project_dir = Path(__file__).parent
        self.pid_file = self.project_dir / ".acis_pid"
        self.log_file = self.project_dir / "acis.log"
        self.python_exe = sys.executable

    def get_running_pid(self) -> int:
        """Get the PID of running ACIS process, if any."""
        if self.pid_file.exists():
            try:
                pid = int(self.pid_file.read_text().strip())
                # Check if process is actually running
                if self._process_exists(pid):
                    return pid
                else:
                    self.pid_file.unlink()  # Remove stale PID file
            except (ValueError, OSError):
                self.pid_file.unlink()
        return None

    def _process_exists(self, pid: int) -> bool:
        """Check if process with given PID is running."""
        try:
            # On Windows, os.kill with signal 0 checks if process exists
            # On Unix, same approach
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False

    def start(self) -> int:
        """Start the ACIS-X system."""
        running_pid = self.get_running_pid()
        if running_pid:
            print(f"ACIS-X is already running (PID: {running_pid})")
            return 1

        print("\n" + "=" * 70)
        print("STARTING ACIS-X RUNTIME SYSTEM")
        print("=" * 70)
        print(f"Python: {self.python_exe}")
        print(f"Project: {self.project_dir}")
        print(f"Log file: {self.log_file}")
        print("\nTo stop the system: Press Ctrl+C or run 'python acis_control.py stop'")
        print("=" * 70 + "\n")

        try:
            # Start process
            process = subprocess.Popen(
                [self.python_exe, "run_acis.py"],
                cwd=self.project_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            # Save PID
            self.pid_file.write_text(str(process.pid))
            print(f"[OK] ACIS-X started (PID: {process.pid})")

            # Bring it to foreground and wait
            process.wait()

            # Clean up PID file when process exits
            self.pid_file.unlink(missing_ok=True)
            print("\n[OK] ACIS-X stopped")
            return 0

        except KeyboardInterrupt:
            # User pressed Ctrl+C
            print("\n[INFO] Ctrl+C received, stopping...")
            self.stop()
            return 0
        except Exception as e:
            print(f"[ERROR] Failed to start: {e}")
            return 1

    def stop(self) -> int:
        """Stop the ACIS-X system gracefully."""
        pid = self.get_running_pid()
        if not pid:
            print("ACIS-X is not running")
            return 0

        print(f"\n[INFO] Stopping ACIS-X (PID: {pid})...")

        try:
            # Send SIGTERM for graceful shutdown
            os.kill(pid, signal.SIGTERM)

            # Wait up to 15 seconds for process to exit
            for i in range(15):
                if not self._process_exists(pid):
                    print(f"[OK] ACIS-X stopped gracefully")
                    self.pid_file.unlink(missing_ok=True)
                    return 0
                time.sleep(1)
                print(f"  [waiting {i+1}/15]", end="\r")

            # Force kill if still running
            print("\n[WARNING] Process did not stop gracefully, force killing...")
            os.kill(pid, signal.SIGKILL)
            self.pid_file.unlink(missing_ok=True)
            print(f"[OK] ACIS-X force killed")
            return 0

        except (OSError, ProcessLookupError):
            self.pid_file.unlink(missing_ok=True)
            print("[OK] ACIS-X is no longer running")
            return 0
        except Exception as e:
            print(f"[ERROR] Failed to stop: {e}")
            return 1

    def status(self) -> int:
        """Check if ACIS-X is running."""
        pid = self.get_running_pid()
        if pid:
            print(f"ACIS-X is RUNNING (PID: {pid})")
            return 0
        else:
            print("ACIS-X is NOT RUNNING")
            return 1

    def restart(self) -> int:
        """Restart ACIS-X."""
        print("Restarting ACIS-X...")
        self.stop()
        time.sleep(2)
        return self.start()

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1

    controller = ACISController()
    command = sys.argv[1].lower()

    if command == "start":
        return controller.start()
    elif command == "stop":
        return controller.stop()
    elif command == "status":
        return controller.status()
    elif command == "restart":
        return controller.restart()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        return 1

if __name__ == "__main__":
    sys.exit(main())
