"""
ACIS-X Full System Reset Script
================================
Run this BEFORE starting run_acis.py for a truly clean fresh start.

What it does:
1. Deletes acis.db (the SQLite database)
2. Deletes acis.log
3. Deletes acis.log.1 and other rotated logs
4. Deletes .acis_consumer_groups_initialized marker file
5. Deletes + recreates ALL Kafka topics (purges all old messages)
6. Deletes all consumer group offsets

Usage:
    python reset_acis.py
    python run_acis.py
"""

import logging
import os
import pathlib
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("reset_acis")


def _bootstrap_servers():
    servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [s.strip() for s in servers.split(",") if s.strip()]


try:
    from runtime.topic_manager import DEFAULT_TOPICS
    ACIS_TOPICS = list(DEFAULT_TOPICS.keys())
except ImportError:
    # Fallback if PYTHONPATH is not set.  Must mirror the keys in
    # runtime/topic_manager.py::ACIS_TOPIC_CONFIGS exactly so that reset_acis
    # purges the same set of topics as the running system creates.
    ACIS_TOPICS = [
        # Business event topics
        "acis.invoices", "acis.payments", "acis.customers",
        "acis.risk", "acis.policy", "acis.commands",
        "acis.metrics", "acis.collections", "acis.predictions",
        # System topics
        "acis.system", "acis.time", "acis.agent.health", "acis.registry",
        # Monitoring / alerts
        "acis.alerts",
        # Placement
        "acis.placement.requests", "acis.placement.assignments",
        # Query bus
        "acis.query.request", "acis.query.response",
        # DLQ topics
        "acis.invoices.dlq", "acis.payments.dlq", "acis.risk.dlq",
        "acis.system.dlq", "acis.dlq",
    ]

def step0_kill_zombie_processes():
    """Step 0: Ensure no orphaned ACIS-X agents are running."""
    logger.info("=" * 60)
    logger.info("STEP 0: Stopping ACIS-X Processes...")
    logger.info("=" * 60)
    
    try:
        import psutil
        current_pid = os.getpid()
        killed = 0
        for p in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = p.info.get('name') or ''
                if 'python' in name.lower() and p.pid != current_pid:
                    cmdline = p.info.get('cmdline') or []
                    cmd_str = ' '.join(cmdline).lower()
                    
                    # Kill anything running run_acis, reset_acis, or any ACIS multiprocessing child
                    if "acis" in cmd_str or "multiprocessing" in cmd_str:
                        # Use kill() for forceful termination on Windows instead of terminate()
                        p.kill()
                        killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
                
        if killed > 0:
            logger.info(f"  [OK] Force-killed {killed} orphaned ACIS-X background processes.")
            time.sleep(3)  # Give OS time to release file locks and clean up PIDs
        else:
            logger.info("  [OK] No orphaned processes found.")
    except ImportError:
        logger.warning("  psutil not installed. Cannot auto-kill zombies.")
        logger.warning("  If reset fails with PermissionError on acis.db, manually run: taskkill /IM python.exe /F")
    
    logger.info("Step 0 complete.\n")


def step1_delete_local_files():
    """Step 1: Delete local DB and log files."""
    logger.info("=" * 60)
    logger.info("STEP 1: Deleting local files...")
    logger.info("=" * 60)

    files_to_delete = [
        "acis.db",
        "acis.db-wal",
        "acis.db-shm",
        "acis.log",
        "acis.log.1",
        ".acis_consumer_groups_initialized",
    ]

    for f in files_to_delete:
        p = pathlib.Path(f)
        if p.exists():
            p.unlink()
            logger.info(f"  [OK] Deleted: {f}")
        else:
            logger.info(f"  [--] Not found (OK): {f}")

    logger.info("Step 1 complete.\n")


def step2_purge_kafka_topics():
    """Step 2: Delete and recreate all Kafka topics to purge old messages."""
    logger.info("=" * 60)
    logger.info("STEP 2: Purging Kafka topics...")
    logger.info("=" * 60)

    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import UnknownTopicOrPartitionError

        bootstrap = _bootstrap_servers()
        logger.info(f"  Connecting to Kafka: {bootstrap}")

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap,
            client_id="acis-reset-client",
            request_timeout_ms=10000,
        )

        # --- Delete existing topics ---
        logger.info("  Deleting topics...")
        existing_topics = []
        try:
            cluster_topics = admin.list_topics()
            existing_topics = [t for t in cluster_topics if t.startswith("acis.")]
            logger.info(f"  Found {len(existing_topics)} existing ACIS topics to delete.")
        except Exception as e:
            logger.warning(f"  Could not list topics: {e}")

        if existing_topics:
            try:
                admin.delete_topics(existing_topics, timeout_ms=15000)
                logger.info(f"  [OK] Deleted {len(existing_topics)} topics. Waiting for cleanup...")
                time.sleep(5)  # Give Kafka time to finish deleting
            except Exception as e:
                logger.warning(f"  Topic deletion warning (may be OK): {e}")
                time.sleep(3)

        # --- Delete consumer group offsets ---
        logger.info("  Deleting consumer group offsets...")
        try:
            groups = admin.list_consumer_groups()
            group_ids = [g[0] for g in groups]
            
            # Delete any group containing these keywords
            to_delete = [
                gid for gid in group_ids 
                if "-group" in gid or "acis" in gid or "agent" in gid or "query-client" in gid
            ]
            
            if to_delete:
                admin.delete_consumer_groups(to_delete)
                logger.info(f"  [OK] Deleted {len(to_delete)} consumer groups.")
            else:
                logger.info("  [OK] No matching consumer groups found to delete.")
        except Exception as e:
            logger.warning(f"  Could not cleanly delete consumer groups: {e}")

        # --- Recreate topics ---
        logger.info("  Recreating topics...")
        topic_configs = {
            "acis.customers":      {"partitions": 6,  "retention_ms": 86400000},  # 1 day
            "acis.invoices":       {"partitions": 6,  "retention_ms": 86400000},
            "acis.payments":       {"partitions": 6,  "retention_ms": 86400000},
            "acis.metrics":        {"partitions": 4,  "retention_ms": 86400000},
            "acis.risk":           {"partitions": 4,  "retention_ms": 86400000},
            "acis.commands":       {"partitions": 3,  "retention_ms": 3600000},   # 1 hour
            "acis.events":         {"partitions": 3,  "retention_ms": 86400000},
            "acis.alerts":         {"partitions": 3,  "retention_ms": 86400000},
            "acis.system":         {"partitions": 3,  "retention_ms": 3600000},
            "acis.heartbeat":      {"partitions": 1,  "retention_ms": 300000},    # 5 min
            "acis.registry":       {"partitions": 3,  "retention_ms": 86400000},
            "acis.audit":          {"partitions": 3,  "retention_ms": 86400000},
            "acis.external.data":  {"partitions": 4,  "retention_ms": 86400000},
            "acis.external.scraping": {"partitions": 3, "retention_ms": 86400000},
            "acis.customers.dlq":  {"partitions": 1,  "retention_ms": 86400000},
            "acis.invoices.dlq":   {"partitions": 1,  "retention_ms": 86400000},
            "acis.payments.dlq":   {"partitions": 1,  "retention_ms": 86400000},
            "acis.metrics.dlq":    {"partitions": 1,  "retention_ms": 86400000},
            "acis.risk.dlq":       {"partitions": 1,  "retention_ms": 86400000},
        }

        new_topics = []
        for topic_name, cfg in topic_configs.items():
            new_topics.append(NewTopic(
                name=topic_name,
                num_partitions=cfg["partitions"],
                replication_factor=1,
                topic_configs={"retention.ms": str(cfg["retention_ms"]), "compression.type": "gzip"},
            ))

        try:
            admin.create_topics(new_topics, timeout_ms=15000)
            logger.info(f"  [OK] Created {len(new_topics)} topics fresh.")
        except Exception as e:
            logger.warning(f"  Topic creation warning: {e}")

        admin.close()
        logger.info("Step 2 complete.\n")

    except ImportError:
        logger.error("  kafka-python not installed. Cannot purge Kafka topics.")
        logger.error("  Install: pip install kafka-python")
    except Exception as e:
        logger.error(f"  Kafka connection failed: {e}")
        logger.error("  Is Kafka running? Check bootstrap servers.")


def step3_verify():
    """Step 3: Verify the reset was successful."""
    logger.info("=" * 60)
    logger.info("STEP 3: Verifying reset...")
    logger.info("=" * 60)

    checks = {
        "acis.db": not pathlib.Path("acis.db").exists(),
        "acis.db-wal": not pathlib.Path("acis.db-wal").exists(),
        "acis.db-shm": not pathlib.Path("acis.db-shm").exists(),
        ".acis_consumer_groups_initialized": not pathlib.Path(".acis_consumer_groups_initialized").exists(),
    }

    all_ok = True
    for name, ok in checks.items():
        status = "[OK]" if ok else "[FAIL]"
        logger.info(f"  {status} {name}: {'absent (good)' if ok else 'STILL EXISTS (problem!)'}")
        if not ok:
            all_ok = False

    logger.info("")
    if all_ok:
        logger.info("=" * 60)
        logger.info("[OK] RESET COMPLETE - System is in a clean state.")
        logger.info("  You can now run: python run_acis.py")
        logger.info("=" * 60)
    else:
        logger.warning("Some files were not cleaned up. Check above.")


if __name__ == "__main__":
    logger.info("")
    logger.info("+----------------------------------------------------------+")
    logger.info("|          ACIS-X Full System Reset                        |")
    logger.info("+----------------------------------------------------------+")
    logger.info("")

    step0_kill_zombie_processes()
    step1_delete_local_files()
    step2_purge_kafka_topics()
    step3_verify()
