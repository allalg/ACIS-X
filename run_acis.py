"""
Bootstrap the ACIS-X runtime stack.

Starts the core runtime components:
- RegistryService
- MonitoringAgent
- SelfHealingAgent
- RuntimeManager
- PlacementEngine
- TimeTickAgent
- ScenarioGeneratorAgent
- PaymentPredictionAgent
- RiskScoringAgent
- CreditPolicyAgent

Creates Kafka topics up front and runs each component in its own independent OS process.
"""

import logging
import os
import signal
import sys
import time
import multiprocessing
from typing import Any, Dict, List, Tuple
import pathlib
from dotenv import load_dotenv

load_dotenv()

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
from agents.system.dlq_monitor_agent import DLQMonitorAgent
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
    """
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(errors="backslashreplace")
        except Exception:
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



def _bootstrap_servers() -> List[str]:
    servers = os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return [server.strip() for server in servers.split(",") if server.strip()]


def _kafka_backend() -> str:
    return os.getenv("ACIS_KAFKA_BACKEND", "kafka-python")


def _reset_consumer_group_offsets_on_first_run(bootstrap_servers: List[str]) -> None:
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
            # acis.agent.health consumers — must be reset together so stale
            # degraded/critical events from a previous run are not replayed.
            "monitoring-group",
            "self-healing-group",
        ]

        reset_count = 0
        for group_id in consumer_groups_to_reset:
            try:
                admin.delete_consumer_groups([group_id])
                logger.info("[ConsumerGroup] Deleted consumer group: %s", group_id)
                reset_count += 1
            except Exception as e:
                logger.debug("[ConsumerGroup] Skipped group %s: %s", group_id, str(e)[:80])

        admin.close()

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


def launch_agent(agent_class: type, kwargs: Dict[str, Any]) -> None:
    """
    Entry point for child processes.
    Initializes Kafka clients inside the child process and runs the agent.
    """
    try:
        # Each agent class may declare an OFFSET_RESET class attribute to control
        # consumer replay behaviour.  Stateful agents (DBAgent, MemoryAgent,
        # QueryAgent) use "earliest" so they fully replay on restart.  All other
        # agents default to "latest" to avoid reprocessing old events.
        offset_reset = getattr(agent_class, "OFFSET_RESET", "latest")
        kafka_client = _build_kafka_client(auto_offset_reset=offset_reset)
        kwargs["kafka_client"] = kafka_client

        agent = agent_class(**kwargs)
        agent_name = getattr(agent, "agent_name", agent_class.__name__)

        # Wire QueryAgent → MemoryAgent in the multi-process case.
        # A real object reference cannot cross OS-process boundaries, so we set
        # a sentinel (True) to activate the QueryClient-based Kafka lookup path.
        # In a single-process deployment, replace True with the actual MemoryAgent
        # instance via query_agent.set_memory_agent(memory_agent).
        if isinstance(agent, QueryAgent) and hasattr(agent, "set_memory_agent"):
            agent.set_memory_agent(True)  # sentinel: enables QueryClient path

        # Handle graceful shutdown in child
        def _handle_sigterm(signum: int, frame: Any) -> None:
            logger.info(f"[{agent_name}] Received SIGTERM, stopping...")
            agent.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)
        # Ignore SIGINT in child to let supervisor handle it
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        agent.start()

        # Keep process alive while agent is running
        while getattr(agent, "_running", True):
            time.sleep(1)

        agent.stop()
    except Exception as e:
        logger.error(f"Fatal error in agent process {agent_class.__name__}: {e}", exc_info=True)
        sys.exit(1)


def _build_components(supervisor_client=None) -> List[Tuple[type, Dict[str, Any]]]:
    """
    Returns the specifications for each agent to be launched in a separate process.

    NOTE — QueryAgent.set_memory_agent() wiring:
        In a single-process deployment you would wire the agents like this:

            kafka_client = _build_kafka_client()
            query_agent  = QueryAgent(kafka_client=kafka_client)
            memory_agent = MemoryAgent(kafka_client=kafka_client)
            query_agent.set_memory_agent(memory_agent)

        In this multi-process deployment each agent runs in its own OS process,
        so Python object references cannot cross process boundaries.
        QueryAgent already reaches MemoryAgent state via QueryClient (Kafka) when
        self._memory_agent is not None.  The _memory_agent flag is set to a
        non-None sentinel inside launch_agent() for QueryAgent so that the
        QueryClient-based path is always attempted; see launch_agent() below.

    NOTE — Agent kwargs convention:
        Each tuple is (AgentClass, kwargs).  kafka_client is injected by
        launch_agent() and must NOT appear here.  All other constructor
        parameters that have non-default values are listed explicitly so that:
          * required params  → documented and always present
          * optional params  → documented with their default for transparency
    """
    # ── Tunable environment variables ─────────────────────────────────────────
    # How often ScenarioGeneratorAgent creates new customers/invoices (seconds).
    # Lower values increase event throughput for stress-testing; higher values
    # reduce load in production-like deployments.
    # Default: 20.0 s  |  Env var: ACIS_GENERATION_INTERVAL
    # Raised from 5 s: at 5 s the generator outpaced DBAgent's SQLite write
    # throughput, causing consumer lag of 15k+ and systematic query timeouts.
    generation_interval = float(os.getenv("ACIS_GENERATION_INTERVAL", "20.0"))

    specs = [
        # ── Control-plane services ────────────────────────────────────────────
        # RegistryService: no extra kwargs; kafka_client injected by launch_agent
        (RegistryService, {}),

        # ── Storage agents ────────────────────────────────────────────────────
        # QueryAgent: no extra kwargs; memory_agent sentinel set inside launch_agent
        (QueryAgent, {}),
        # MemoryAgent: no extra kwargs; subscribes to acis.customers + acis.invoices
        (MemoryAgent, {}),
        # DBAgent: no extra kwargs; db_path defaults to DBAgent.DB_PATH ("acis.db")
        (DBAgent, {}),

        # ── Intelligence / analytical agents ──────────────────────────────────
        # CustomerStateAgent: no extra kwargs; per-customer lock granularity
        #   is configured internally via MAX_CONCURRENT_REBUILDS
        (CustomerStateAgent, {}),
        # OverdueDetectionAgent: no extra kwargs
        (OverdueDetectionAgent, {}),
        # CollectionsAgent: no extra kwargs
        (CollectionsAgent, {}),
        # AggregatorAgent: no extra kwargs; merges internal + external risk signals
        (AggregatorAgent, {}),

        # ── Monitoring / self-healing ─────────────────────────────────────────
        # MonitoringAgent: no extra kwargs; evaluation interval set via HEARTBEAT_TIMEOUT
        (MonitoringAgent, {}),
        # SelfHealingAgent: optional supervisor handle for cross-process restarts.
        #   supervisor (AgentSupervisorClient | None): enables restart requests
        (SelfHealingAgent, {"supervisor": supervisor_client} if supervisor_client else {}),

        # ── Runtime infrastructure ────────────────────────────────────────────
        # RuntimeManager: no extra kwargs
        (RuntimeManager, {}),
        # PlacementEngine: no extra kwargs; polling thread started in .start()
        (PlacementEngine, {}),

        # ── System utility agents ─────────────────────────────────────────────
        # TimeTickAgent: no extra kwargs; tick interval is hardcoded (60 s)
        (TimeTickAgent, {}),
        # DLQMonitorAgent: no extra kwargs; polls acis.dlq at a fixed interval
        (DLQMonitorAgent, {}),

        # ── External data enrichment ──────────────────────────────────────────
        # ExternalDataAgent: no extra kwargs; thread-pool size = 4 workers
        (ExternalDataAgent, {}),
        # ExternalScrapingAgent: no extra kwargs; thread-pool size = 2 workers
        (ExternalScrapingAgent, {}),

        # ── Analytical / prediction agents ────────────────────────────────────
        # PaymentPredictionAgent: no extra kwargs
        (PaymentPredictionAgent, {}),
        # RiskScoringAgent: no extra kwargs; context enrichment via QueryClient
        (RiskScoringAgent, {}),
        # CustomerProfileAgent: no extra kwargs
        (CustomerProfileAgent, {}),

        # ── Simulation / load generation ─────────────────────────────────────
        # ScenarioGeneratorAgent:
        #   generation_interval_seconds (float, required): seconds between
        #     customer/invoice creation cycles.
        #     Controlled by env var ACIS_GENERATION_INTERVAL (default 5.0 s).
        (ScenarioGeneratorAgent, {"generation_interval_seconds": generation_interval}),
    ]
    return specs



MAX_RESTART_ATTEMPTS = 5
RESTART_WINDOW_SECONDS = 60


def main() -> None:
    _create_topics()
    _reset_control_plane_consumer_groups(_bootstrap_servers())
    _reset_consumer_group_offsets_on_first_run(_bootstrap_servers())

    manager = multiprocessing.Manager()
    shared_dict = manager.dict({"restart_requests": []})
    
    from runtime.agent_supervisor import AgentSupervisor, SupervisorClient
    supervisor_client = SupervisorClient(shared_dict)
    supervisor = AgentSupervisor(launch_fn=launch_agent)

    agent_specs = _build_components(supervisor_client)
    shutdown_requested = False

    def _request_shutdown(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        logger.info("\n>>> Received signal %s, initiating graceful shutdown...", signum)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    # Start all processes
    for agent_class, kwargs in agent_specs:
        agent_name = agent_class.__name__
        p = multiprocessing.Process(
            target=launch_agent,
            args=(agent_class, kwargs),
            name=agent_name
        )
        p.start()
        supervisor.register(agent_name, p, agent_class, kwargs)
        logger.info(f"Started process for {agent_name} (PID: {p.pid})")

    logger.info("ACIS-X runtime bootstrap complete (Multi-Process with Supervisor)")
    logger.info(">>> Press Ctrl+C to stop the system")

    try:
        # Supervisor loop
        while not shutdown_requested:
            # Process restart requests from SelfHealingAgent
            requests = shared_dict.get("restart_requests", [])
            if requests:
                shared_dict["restart_requests"] = []
                for agent_name in requests:
                    logger.info(f"Received restart request for {agent_name} from SelfHealingAgent")
                    supervisor.restart_agent(agent_name)

            # Check for unexpectedly exited processes
            for p in supervisor.all_processes():
                if p is not None and not p.is_alive():
                    exitcode = p.exitcode
                    if exitcode is not None and exitcode != 0:
                        agent_name = p.name
                        logger.warning(f"Process {agent_name} exited unexpectedly with code {exitcode}.")
                        supervisor.restart_agent(agent_name)

            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n>>> Ctrl+C received in main thread, shutting down...")
        shutdown_requested = True

    # Graceful shutdown
    logger.info("\n>>> Stopping ACIS-X multi-process components...")
    
    # Send SIGTERM to all child processes
    for p in supervisor.all_processes():
        if p and p.is_alive():
            logger.info(f"Terminating {p.name} (PID: {p.pid})...")
            p.terminate()

    # Wait up to 10s for clean exit
    end_time = time.time() + 10
    for p in supervisor.all_processes():
        if p and p.is_alive():
            timeout = max(0, end_time - time.time())
            p.join(timeout=timeout)
            if p.is_alive():
                logger.warning(f"Process {p.name} did not terminate gracefully, killing it...")
                p.kill()

    logger.info(">>> ACIS-X shutdown complete")


if __name__ == "__main__":
    main()
