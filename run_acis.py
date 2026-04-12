"""
Bootstrap the ACIS-X runtime stack.

Starts the core runtime components:
- RegistryService
- MonitoringAgent
- SelfHealingAgent
- RuntimeManager
- PlacementEngine
- TimeTickAgent (publishes time ticks every 5s for overdue detection)
- ScenarioGeneratorAgent
- PaymentPredictionAgent
- RiskScoringAgent
- CreditPolicyAgent

Creates Kafka topics up front and runs each component in its own thread.
"""

import logging
import os
import signal
import sys
import threading
from typing import Any, Dict, List, Tuple
import pathlib

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

from agents.intelligence.customer_state_agent import CustomerStateAgent
from agents.intelligence.external_data_agent import ExternalDataAgent
from agents.intelligence.external_scrapping_agent import ExternalScrapingAgent
from agents.intelligence.aggregator_agent import AggregatorAgent
from agents.invoice.overdue_detection_agent import OverdueDetectionAgent
from agents.prediction.payment_prediction_agent import PaymentPredictionAgent
from agents.risk.risk_scoring_agent import RiskScoringAgent
from agents.customer.customer_profile_agent import CustomerProfileAgent
from agents.collections.collections_agent import CollectionsAgent
from agents.scenario_generator.scenario_generator_agent import ScenarioGeneratorAgent
from agents.storage.db_agent import DBAgent
from agents.storage.memory_agent import MemoryAgent
from agents.storage.query_agent import QueryAgent
from agents.system.time_tick_agent import TimeTickAgent
from monitoring.monitoring_agent import MonitoringAgent
from registry.registry_service import RegistryService
from runtime.kafka_client import KafkaClient, KafkaConfig
from runtime.placement_engine import PlacementEngine
from runtime.runtime_manager import RuntimeManager
from runtime.topic_manager import TopicAdmin
from self_healing.core.self_healing_agent import SelfHealingAgent


def _configure_console_streams() -> None:
    """
    Make console logging resilient on Windows terminals with legacy encodings.

    We keep the existing console encoding, but switch error handling away from
    "strict" so one non-ASCII log line cannot crash the runtime.
    """
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue

        try:
            stream.reconfigure(errors="backslashreplace")
        except Exception:
            # Some wrapped streams do not support reconfigure(); keep going.
            pass


_configure_console_streams()

logging.basicConfig(
    level=os.getenv("ACIS_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("acis.log", mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("run_acis")

if not os.environ.get("INDIAN_KANOON_API_KEY"):
    logger.warning("INDIAN_KANOON_API_KEY not set - ExternalScrapingAgent will run in limited mode")


def _bootstrap_servers() -> List[str]:
    servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [server.strip() for server in servers.split(",") if server.strip()]


def _kafka_backend() -> str:
    return os.getenv("ACIS_KAFKA_BACKEND", "kafka-python")


def _reset_consumer_group_offsets_on_first_run(bootstrap_servers: List[str]) -> None:
    """
    Delete committed consumer groups on first run only.

    This forces Kafka to recreate offsets using the current client defaults on
    the next subscribe call. It is used to keep "fresh start" behavior honest
    instead of relying on a marker plus a comment that never changed offsets.

    Marker file: .acis_consumer_groups_initialized
    """
    marker_file = pathlib.Path(".acis_consumer_groups_initialized")

    if marker_file.exists():
        logger.info("[ConsumerGroup] Marker file exists - skipping offset reset (not first run)")
        return

    logger.info("[ConsumerGroup] First run detected - deleting committed consumer groups")

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="acis-consumer-group-init"
        )

        # Stable consumer groups that should be recreated on a fresh bootstrap.
        consumer_groups_to_reset = [
            "db-agent-group",
            "memory-agent-group",
            "customer-state-group",
            "overdue-detection-group",
            "collections-group",
            "external-data-group",
            "litigation-agent-group",
            "risk-scoring-group",
            "customer-profile-group",
            "aggregator-agent-group",
            "payment-prediction-group",
        ]

        reset_count = 0
        for group_id in consumer_groups_to_reset:
            try:
                admin.delete_consumer_groups([group_id])
                logger.info("[ConsumerGroup] Deleted consumer group: %s", group_id)
                reset_count += 1
            except Exception as e:
                # Group may not exist yet - this is OK.
                logger.debug("[ConsumerGroup] Skipped group %s: %s", group_id, str(e)[:80])

        admin.close()

        # Write marker file to indicate consumer groups have been initialized
        marker_file.write_text(
            "ACIS-X Consumer Groups Initialized\n"
            f"Timestamp: {__import__('datetime').datetime.now().isoformat()}\n"
            f"Marker: This file prevents repeated consumer group offset resets.\n"
            f"Delete this file to force a reset on next startup.\n"
        )

        logger.info(f"[ConsumerGroup] Marker file created: {marker_file}")
        logger.info("[ConsumerGroup] Consumer group cleanup complete (%s groups deleted)", reset_count)

    except Exception as e:
        logger.warning(f"[ConsumerGroup] Failed to clean consumer groups: {e}")
        logger.warning("[ConsumerGroup] System will continue with existing committed offsets")


def _reset_control_plane_consumer_groups(bootstrap_servers: List[str]) -> None:
    """Always recreate control-plane consumer groups so orchestration starts clean."""
    control_plane_groups = [
        "runtime-manager-group",
        "placement-engine-group",
        "monitoring-group",
        "self-healing-group",
        "acis-registry-service",
    ]

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="acis-control-plane-reset"
        )
        try:
            deleted = 0
            for group_id in control_plane_groups:
                try:
                    admin.delete_consumer_groups([group_id])
                    logger.info("[ConsumerGroup] Reset control-plane group: %s", group_id)
                    deleted += 1
                except Exception as exc:
                    logger.debug("[ConsumerGroup] Control-plane group %s unchanged: %s", group_id, str(exc)[:80])

            logger.info("[ConsumerGroup] Control-plane reset complete (%s groups deleted)", deleted)
        finally:
            admin.close()
    except Exception as exc:
        logger.warning("[ConsumerGroup] Failed to reset control-plane groups: %s", exc)


def _build_kafka_client(auto_offset_reset: str = "earliest") -> KafkaClient:
    config = KafkaConfig(
        bootstrap_servers=_bootstrap_servers(),
        consumer_auto_offset_reset=auto_offset_reset,
    )
    return KafkaClient(config=config, backend=_kafka_backend())



def _create_topics() -> Dict[str, bool]:
    try:
        admin = TopicAdmin(bootstrap_servers=_bootstrap_servers(), backend=_kafka_backend())
        try:
            results = admin.create_all_acis_topics()
            failed = [topic for topic, ok in results.items() if not ok]
            if failed:
                logger.warning("Some topics failed to create: %s", ", ".join(failed))
            else:
                logger.info("ACIS-X topics are ready")
            return results
        finally:
            admin.close()
    except Exception as e:
        logger.warning("Topic creation skipped: %s", e)
        return {}


def _run_registry_service(service: RegistryService, shutdown_event: threading.Event) -> None:
    service.start()
    shutdown_event.wait()


def _run_agent_service(agent: Any, shutdown_event: threading.Event) -> None:
    try:
        agent.start()
    except Exception as e:
        logger.error(f"Error in agent {agent.agent_name}: {e}")

    shutdown_event.wait()


def _build_components() -> Tuple[RegistryService, List[Any]]:
    # FIX 5 - Architecture Design (CORRECTED):
    # - ISSUE 2 FIX: Create ONE shared Kafka client for PRODUCER only
    # - Each agent gets its OWN Kafka client for CONSUMER (prevents subscription overwrites)
    # - Shared producer = 1 connection
    # - Separate consumers per agent = isolated subscriptions and group IDs

    shared_kafka_client = _build_kafka_client(auto_offset_reset="latest")
    logger.info("[Bootstrap] Created shared Kafka producer client")

    registry_service = RegistryService(kafka_client=shared_kafka_client)

    # Create QueryAgent FIRST (DB source of truth)
    query_agent = QueryAgent(kafka_client=_build_kafka_client())

    # Create MemoryAgent with QueryAgent dependency
    memory_agent = MemoryAgent(
        kafka_client=_build_kafka_client(),
        query_agent=query_agent
    )

    customer_state_agent = CustomerStateAgent(
        kafka_client=_build_kafka_client(),
        query_agent=query_agent
    )

    overdue_detection_agent = OverdueDetectionAgent(
        kafka_client=_build_kafka_client(),
        query_agent=query_agent
    )

    collections_agent = CollectionsAgent(
        kafka_client=_build_kafka_client(),
        query_agent=query_agent
    )

    # Create DBAgent and link with QueryAgent for cache invalidation
    db_agent = DBAgent(kafka_client=_build_kafka_client())
    db_agent.set_query_agent(query_agent)

    # Create AggregatorAgent and link with QueryAgent for company name resolution (FIX #12)
    aggregator_agent = AggregatorAgent(kafka_client=_build_kafka_client())
    aggregator_agent.set_query_agent(query_agent)

    agents: List[Any] = [
        MonitoringAgent(kafka_client=_build_kafka_client(auto_offset_reset="latest")),
        SelfHealingAgent(
            kafka_client=_build_kafka_client(auto_offset_reset="latest"),
            registry=registry_service,
        ),
        RuntimeManager(kafka_client=_build_kafka_client(auto_offset_reset="latest")),
        PlacementEngine(
            kafka_client=_build_kafka_client(auto_offset_reset="latest"),
            registry=registry_service,
        ),
        TimeTickAgent(kafka_client=_build_kafka_client()),  # Time infrastructure - required for overdue detection
        ScenarioGeneratorAgent(
            kafka_client=_build_kafka_client(),
            query_agent=query_agent  # FIX #1: Pass QueryAgent for DB-backed customer count check
        ),
        db_agent,
        memory_agent,
        query_agent,
        customer_state_agent,
        overdue_detection_agent,
        ExternalDataAgent(
            kafka_client=_build_kafka_client(),
            query_agent=query_agent
        ),
        ExternalScrapingAgent(
            kafka_client=_build_kafka_client(),
            query_agent=query_agent
        ),
        aggregator_agent,
        PaymentPredictionAgent(
            kafka_client=_build_kafka_client(),
            query_agent=query_agent
        ),
        RiskScoringAgent(
            kafka_client=_build_kafka_client(),
            query_agent=query_agent,
            memory_agent=memory_agent  # For temporal trend detection
        ),
        CustomerProfileAgent(kafka_client=_build_kafka_client()),
        collections_agent,
        # REMOVED: CreditPolicyAgent - CollectionsAgent is now the sole decision engine
    ]

    return registry_service, agents


def main() -> None:
    shutdown_event = threading.Event()
    _create_topics()
    _reset_control_plane_consumer_groups(_bootstrap_servers())

    # FIX 8: Reset consumer group offsets on first run to prevent skipping historical messages
    _reset_consumer_group_offsets_on_first_run(_bootstrap_servers())

    registry_service, agents = _build_components()
    threads: List[threading.Thread] = []

    def _request_shutdown(signum: int, frame: Any) -> None:
        """Handle Ctrl+C (SIGINT) and SIGTERM for graceful shutdown."""
        logger.info("\n>>> Received signal %s, initiating graceful shutdown...", signum)
        shutdown_event.set()

    # Register signal handlers for graceful shutdown
    # SIGINT = Ctrl+C on all platforms, SIGTERM = termination signal
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    registry_thread = threading.Thread(
        target=_run_registry_service,
        args=(registry_service, shutdown_event),
        daemon=True,
        name="registry-service-runner",
    )
    threads.append(registry_thread)
    registry_thread.start()

    for agent in agents:
        thread = threading.Thread(
            target=_run_agent_service,
            args=(agent, shutdown_event),
            daemon=True,
            name=agent.agent_name,
        )
        threads.append(thread)
        thread.start()

    logger.info("ACIS-X runtime bootstrap complete")
    logger.info(">>> Press Ctrl+C to stop the system")

    try:
        # Wait for shutdown signal (Ctrl+C or SIGTERM)
        shutdown_event.wait()
    except KeyboardInterrupt:
        # Fallback for KeyboardInterrupt (should be caught by signal handler)
        logger.info("\n>>> Ctrl+C received, initiating shutdown...")
        shutdown_event.set()
    finally:
        logger.info("\n>>> Stopping ACIS-X components...")

        # Stop agents in reverse order of startup
        for i, agent in enumerate(reversed(agents), 1):
            agent_name = getattr(agent, "agent_name", type(agent).__name__)
            try:
                logger.info(f"  [{i}/{len(agents)}] Stopping {agent_name}...")
                agent.stop()
                logger.debug(f"  [{i}/{len(agents)}] Stopped {agent_name}")
            except Exception as exc:
                logger.warning(f"  [{i}/{len(agents)}] Failed stopping {agent_name}: {exc}")

        # Stop registry service
        try:
            logger.info(f"  [{len(agents)+1}/{len(agents)+1}] Stopping RegistryService...")
            registry_service.stop()
            logger.debug(f"  [{len(agents)+1}/{len(agents)+1}] Stopped RegistryService")
        except Exception as exc:
            logger.warning(f"Failed stopping RegistryService: {exc}")

        # Wait for threads to finish (up to 10 seconds total)
        logger.info("\n>>> Waiting for threads to finish (up to 10 seconds)...")
        for i, thread in enumerate(threads, 1):
            if thread.is_alive():
                logger.debug(f"  Joining thread {i}/{len(threads)}: {thread.name}...")
                thread.join(timeout=2)
                if thread.is_alive():
                    logger.warning(f"  Thread {thread.name} did not exit cleanly (timeout)")

        logger.info(">>> ACIS-X shutdown complete")


if __name__ == "__main__":
    main()
