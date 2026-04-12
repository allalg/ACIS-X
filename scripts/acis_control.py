#!/usr/bin/env python
"""
ACIS-X Control Script - Easy start/stop/status management

Usage:
  python acis_control.py start     - Start the system
  python acis_control.py stop      - Stop the system gracefully
  python acis_control.py status    - Check if system is running
  python acis_control.py restart   - Stop then start
    python acis_control.py flush-cache - Stop and clear DB + offset marker cache files
"""

import os
import sys
import time
import subprocess
import signal
from pathlib import Path

class ACISController:
    def __init__(self):
        self.project_dir = Path(__file__).resolve().parent.parent
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

    def _discover_run_acis_pids(self) -> list[int]:
        """Find run_acis.py Python processes even when no PID file exists."""
        pids: list[int] = []

        try:
            if os.name == "nt":
                cmd = (
                    "Get-CimInstance Win32_Process | "
                    "Where-Object { $_.Name -match 'python' -and $_.CommandLine -match 'run_acis.py' } | "
                    "Select-Object -ExpandProperty ProcessId"
                )
                result = subprocess.run(
                    ["powershell", "-NoProfile", "-Command", cmd],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in result.stdout.splitlines():
                    line = line.strip()
                    if line.isdigit():
                        pids.append(int(line))
            else:
                result = subprocess.run(
                    ["ps", "-eo", "pid,args"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                for line in result.stdout.splitlines():
                    line = line.strip()
                    if "python" in line and "run_acis.py" in line:
                        parts = line.split(maxsplit=1)
                        if parts and parts[0].isdigit():
                            pids.append(int(parts[0]))
        except Exception:
            return []

        # Deduplicate while preserving order
        seen = set()
        ordered = []
        for pid in pids:
            if pid not in seen:
                seen.add(pid)
                ordered.append(pid)
        return ordered

    def _terminate_pid(self, pid: int, force: bool = False) -> None:
        """Terminate a process with cross-platform handling."""
        if os.name == "nt":
            # Use taskkill on Windows for reliable termination of Python process trees.
            cmd = ["taskkill", "/PID", str(pid), "/T"]
            if force:
                cmd.append("/F")
            subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            return

        sig = signal.SIGKILL if force else signal.SIGTERM
        os.kill(pid, sig)

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
        discovered = self._discover_run_acis_pids()
        pids_to_stop = []
        if pid:
            pids_to_stop.append(pid)
        for discovered_pid in discovered:
            if discovered_pid not in pids_to_stop:
                pids_to_stop.append(discovered_pid)

        if not pids_to_stop:
            print("ACIS-X is not running")
            return 0

        print(f"\n[INFO] Stopping ACIS-X (PIDs: {pids_to_stop})...")

        try:
            # Send SIGTERM for graceful shutdown
            for running_pid in pids_to_stop:
                try:
                    self._terminate_pid(running_pid, force=False)
                except Exception:
                    pass

            # Wait up to 15 seconds for process to exit
            for i in range(15):
                alive = [running_pid for running_pid in pids_to_stop if self._process_exists(running_pid)]
                if not alive:
                    print("[OK] ACIS-X stopped gracefully")
                    self.pid_file.unlink(missing_ok=True)
                    return 0
                time.sleep(1)
                print(f"  [waiting {i+1}/15] still alive: {alive}", end="\r")

            # Force kill if still running
            print("\n[WARNING] Process did not stop gracefully, force killing...")
            for running_pid in pids_to_stop:
                try:
                    self._terminate_pid(running_pid, force=True)
                except Exception:
                    pass
            self.pid_file.unlink(missing_ok=True)
            print("[OK] ACIS-X force killed")
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
        discovered = self._discover_run_acis_pids()
        active = []
        if pid:
            active.append(pid)
        for discovered_pid in discovered:
            if discovered_pid not in active:
                active.append(discovered_pid)

        if active:
            print(f"ACIS-X is RUNNING (PIDs: {active})")
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

    def flush_cache(self) -> int:
        """Stop ACIS-X and remove local cache/state files."""
        print("\n[INFO] Flushing ACIS-X cache/state...")

        stop_code = self.stop()
        if stop_code != 0:
            print("[WARNING] Stop reported an issue; continuing with cache cleanup")

        cache_files = [
            "acis.db",
            "acis.db-journal",
            "acis.db-wal",
            "acis.db-shm",
            ".acis_consumer_groups_initialized",
        ]

        removed = 0
        for filename in cache_files:
            path = self.project_dir / filename
            if path.exists():
                try:
                    path.unlink()
                    print(f"[OK] Removed {filename}")
                    removed += 1
                except Exception as e:
                    print(f"[WARNING] Could not remove {filename}: {e}")

        if removed == 0:
            print("[INFO] No cache files found to remove")

        print("[OK] Cache flush complete")
        return 0

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
    elif command == "flush-cache":
        return controller.flush_cache()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        return 1

if __name__ == "__main__":
    sys.exit(main())
