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


logging.basicConfig(
    level=os.getenv("ACIS_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("acis.log", mode="w"),
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


def _build_kafka_client() -> KafkaClient:
    config = KafkaConfig(bootstrap_servers=_bootstrap_servers())
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

    shared_kafka_client = _build_kafka_client()
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

    agents: List[Any] = [
        MonitoringAgent(kafka_client=_build_kafka_client()),
        SelfHealingAgent(kafka_client=_build_kafka_client()),
        RuntimeManager(kafka_client=_build_kafka_client()),
        PlacementEngine(kafka_client=_build_kafka_client()),
        TimeTickAgent(kafka_client=_build_kafka_client()),  # Time infrastructure - required for overdue detection
        ScenarioGeneratorAgent(kafka_client=_build_kafka_client()),
        db_agent,
        memory_agent,
        query_agent,
        customer_state_agent,
        overdue_detection_agent,
        ExternalDataAgent(kafka_client=_build_kafka_client()),
        ExternalScrapingAgent(kafka_client=_build_kafka_client()),
        AggregatorAgent(kafka_client=_build_kafka_client()),
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

    registry_service, agents = _build_components()
    threads: List[threading.Thread] = []

    def _request_shutdown(signum: int, frame: Any) -> None:
        logger.info("Received signal %s, shutting down ACIS-X runtime", signum)
        shutdown_event.set()

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

    try:
        shutdown_event.wait()
    finally:
        logger.info("Stopping ACIS-X components")

        for agent in reversed(agents):
            try:
                agent.stop()
            except Exception as exc:
                logger.warning("Failed stopping %s: %s", getattr(agent, "agent_name", type(agent).__name__), exc)

        try:
            registry_service.stop()
        except Exception as exc:
            logger.warning("Failed stopping RegistryService: %s", exc)

        for thread in threads:
            thread.join(timeout=5)

        logger.info("ACIS-X shutdown complete")


if __name__ == "__main__":
    main()
