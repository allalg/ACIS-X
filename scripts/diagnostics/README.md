# scripts/diagnostics/

Read-only diagnostic tools for inspecting ACIS-X system state.

## ⚠️ Important Safety Warning

**NEVER run these scripts while `acis.db` is locked by a live ACIS-X system.**

SQLite allows concurrent reads when WAL mode is active, but:
- `analyze_db.py` and `query_dream.py` open direct SQLite connections
- Running them during a live system write burst may return stale data
- If the system is in the middle of a schema migration or bulk insert,
  reads can briefly fail with `database is locked` errors

**Safe usage pattern:**
1. Stop the system: `python scripts/acis_control.py stop`
2. Run the diagnostic: `python scripts/diagnostics/analyze_db.py`
3. Restart: `python scripts/acis_control.py start`

Or use `reset_acis.py` for a full clean slate before restarting.

## Scripts

| Script | Purpose |
|---|---|
| `analyze_db.py` | Comprehensive DB audit — row counts, sample data, schema checks, NULL analysis across all tables |
| `query_dream.py` | Ad-hoc SQL query helper for interactive inspection |

## Not for Production

These are **read-only diagnostic** tools only. They do not write to the
database, publish Kafka events, or modify any system state. They are
intended for development and post-mortem analysis, not operational monitoring.

For live operational monitoring, use the `MonitoringAgent` events on
`acis.monitoring` or the `DLQMonitorAgent` stats on `acis.monitoring`.
