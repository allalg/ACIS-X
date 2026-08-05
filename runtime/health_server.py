"""
Lightweight health-check HTTP server for the ACIS-X Core Engine.

Runs as a daemon thread alongside run_acis.py and exposes:
    GET /healthz  → 200 with agent process statuses

Usage (inside run_acis.py main()):
    from runtime.health_server import start_health_server
    start_health_server(port=9090, process_registry=process_registry)
"""

import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Module-level reference to the process registry dict.
# Set by start_health_server(); read by _HealthHandler.
_process_registry: Optional[Dict[str, Any]] = None
_started = False


class _HealthHandler(BaseHTTPRequestHandler):
    """Minimal request handler for /healthz."""

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._respond_healthz()
        else:
            self.send_error(404)

    def _respond_healthz(self) -> None:
        status: Dict[str, Any] = {"status": "ok", "agents": {}}
        overall_healthy = True

        if _process_registry:
            for name, proc in _process_registry.items():
                alive = proc.is_alive() if hasattr(proc, "is_alive") else False
                status["agents"][name] = {
                    "alive": alive,
                    "pid": proc.pid if hasattr(proc, "pid") else None,
                }
                if not alive:
                    overall_healthy = False

        status["status"] = "ok" if overall_healthy else "degraded"
        code = 200 if overall_healthy else 503

        body = json.dumps(status, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stderr logging to avoid noise."""
        pass


def start_health_server(
    port: int = 9090,
    process_registry: Optional[Dict[str, Any]] = None,
) -> None:
    """Start the health-check HTTP server on a daemon thread.

    Args:
        port: TCP port to bind (default 9090).
        process_registry: dict mapping agent names → multiprocessing.Process.
    """
    global _process_registry, _started
    if _started:
        return
    _process_registry = process_registry

    def _serve() -> None:
        server = HTTPServer(("0.0.0.0", port), _HealthHandler)
        logger.info("Health server listening on http://0.0.0.0:%d/healthz", port)
        server.serve_forever()

    t = threading.Thread(target=_serve, daemon=True, name="health-server")
    t.start()
    _started = True
