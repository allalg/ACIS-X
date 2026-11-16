class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        import json
        entry = {"ts": self.formatTime(record), "level": record.levelname, "logger": record.name, "msg": record.getMessage()}
        if record.exc_info and record.exc_info[0]:
            entry["exc"] = self.formatException(record.exc_info)
        for k in ("agent_name", "event_type", "customer_id"):
            if hasattr(record, k):
                entry[k] = getattr(record, k)
        return json.dumps(entry, default=str)

_setup_logging()
logger = logging.getLogger("run_acis")

def _bootstrap_servers() -> List[str]:
    return [s.strip() for s in os.getenv("ACIS_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",") if s.strip()]

def _build_kafka_client(auto_offset_reset: str = "earliest") -> KafkaClient:
    from config.settings import get_settings
    s = get_settings()
    config = KafkaConfig(
        bootstrap_servers=_bootstrap_servers(),
        consumer_auto_offset_reset=auto_offset_reset,
        security_protocol=s.kafka_security_protocol or "PLAINTEXT",
        sasl_mechanism=s.kafka_sasl_mechanism,
        sasl_username=s.kafka_sasl_username,
        sasl_password=s.kafka_sasl_password,
    )
    return KafkaClient(config=config, backend=os.getenv("ACIS_KAFKA_BACKEND", "kafka-python"))

def _create_topics() -> Dict[str, bool]:
    try:
        from config.settings import get_settings
        s = get_settings()
        admin = TopicAdmin(
            bootstrap_servers=_bootstrap_servers(), backend=os.getenv("ACIS_KAFKA_BACKEND", "kafka-python"),
            security_protocol=s.kafka_security_protocol or "PLAINTEXT", sasl_mechanism=s.kafka_sasl_mechanism,
            sasl_username=s.kafka_sasl_username, sasl_password=s.kafka_sasl_password,
        )
        try:
            return admin.create_all_acis_topics()
        finally:
            admin.close()
    except Exception as e:
        logger.warning("Topic initialization skipped: %s", e)
        return {}

def launch_agent(agent_class: type, kwargs: Dict[str, Any]) -> None:
    """Child process entry point initializing isolated Kafka client & agent runtime."""
    try:
        offset_reset = getattr(agent_class, "OFFSET_RESET", "latest")
        kwargs["kafka_client"] = _build_kafka_client(auto_offset_reset=offset_reset)
        agent = agent_class(**kwargs)
        agent_name = getattr(agent, "agent_name", agent_class.__name__)

        if isinstance(agent, QueryAgent) and hasattr(agent, "set_memory_agent"):
            agent.set_memory_agent(True)  

        def _handle_sigterm(signum: int, frame: Any) -> None:
            logger.info("[%s] Received SIGTERM, stopping...", agent_name)
            agent.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        agent.start()
        while getattr(agent, "_running", True):
            time.sleep(1)
        agent.stop()
    except Exception as e:
        logger.error("Fatal error in agent process %s: %s", agent_class.__name__, e, exc_info=True)
        sys.exit(1)

def _build_components(supervisor_client: Any = None) -> List[Tuple[type, Dict[str, Any]]]:
    gen_interval = float(os.getenv("ACIS_GENERATION_INTERVAL", "20.0"))
    return [
        (RegistryService, {}),
        (QueryAgent, {}), (MemoryAgent, {}), (DBAgent, {}),
        (CustomerStateAgent, {}), (OverdueDetectionAgent, {}), (CollectionsAgent, {}), (AggregatorAgent, {}),
        (MonitoringAgent, {}), (SelfHealingAgent, {"supervisor": supervisor_client} if supervisor_client else {}),
        (RuntimeManager, {}), (PlacementEngine, {}), (TimeTickAgent, {}), (DLQMonitorAgent, {}),
        (ExternalDataAgent, {}), (ExternalScrapingAgent, {}),
        (PaymentPredictionAgent, {}), (RiskScoringAgent, {}), (CustomerProfileAgent, {}), (ModelGovernanceAgent, {}),
        (ScenarioGeneratorAgent, {"generation_interval_seconds": gen_interval}),
    ]

def main() -> None:
    _create_topics()
    _reset_consumer_groups(_bootstrap_servers())

    manager = multiprocessing.Manager()
    shared_dict = manager.dict({"restart_requests": []})
    supervisor_client = SupervisorClient(shared_dict)
    supervisor = AgentSupervisor(launch_fn=launch_agent)

    agent_specs = _build_components(supervisor_client)
    shutdown_requested = False

    def _request_shutdown(signum: int, frame: Any) -> None:
        nonlocal shutdown_requested
        logger.info("\n>>> Received shutdown signal (%s), terminating components...", signum)
        shutdown_requested = True

    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    for agent_class, kwargs in agent_specs:
        name = agent_class.__name__
        p = multiprocessing.Process(target=launch_agent, args=(agent_class, kwargs), name=name)
        p.start()
        supervisor.register(name, p, agent_class, kwargs)
        logger.info("Started process for %s (PID: %d)", name, p.pid)

    logger.info("ACIS-X Runtime Bootstrap Complete. System operational.")
    process_reg = {p.name: p for p in supervisor.all_processes() if p is not None}
    start_health_server(port=int(os.getenv("ACIS_HEALTH_PORT", "9090")), process_registry=process_reg)

    try:
        while not shutdown_requested:
            requests = shared_dict.get("restart_requests", [])
            if requests:
                shared_dict["restart_requests"] = []
                for agent_name in requests:
                    logger.info("Restart request received for %s from SelfHealingAgent", agent_name)
                    supervisor.restart_agent(agent_name)

            for p in supervisor.all_processes():
                if p and not p.is_alive() and p.exitcode not in (0, None):
                    logger.warning("Process %s died unexpectedly (code %d). Restarting...", p.name, p.exitcode)
                    supervisor.restart_agent(p.name)
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_requested = True

    logger.info(">>> Shutting down ACIS-X multi-process agent pipeline...")
    for p in supervisor.all_processes():
        if p and p.is_alive(): p.terminate()
    end_time = time.time() + 10
    for p in supervisor.all_processes():
        if p and p.is_alive():
            p.join(timeout=max(0, end_time - time.time()))
            if p.is_alive(): p.kill()
    logger.info(">>> ACIS-X shutdown complete.")

if __name__ == "__main__":
    main()
