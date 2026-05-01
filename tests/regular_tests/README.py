"""
tests/regular_tests/README.md

ACIS-X Regular Test Suite
=========================

PURPOSE
-------
These are functional smoke tests that answer:
    "Does this agent/component work correctly?"

They run on every commit, are fast (< 20 s total), and do not require
live Kafka or a real database.

WHO RUNS THEM
-------------
• Developers — run after every local change:
      pytest tests/regular_tests/ -q

• CI pipeline — run on every push/PR.

WHAT IS TESTED
--------------

Infrastructure / Primitives
    test_unit_kafka_client.py           KafkaClient commit offset logic
    test_unit_circuit_breaker.py        CircuitBreaker open/half-open/closed
    test_unit_datetime_tz.py            Event timezone normalisation
    test_unit_event_validation.py       EventEnvelope schema validation + DLQ
    test_unit_schema.py                 SQLite schema creation + basic queries
    test_unit_query_client.py           QueryClient thread-local consumer isolation
    test_query_client_thread_isolation.py  QueryClient correlation_id filtering

Agent Contracts
    test_unit_architecture_fixes.py     Broad agent contract smoke tests:
                                          invoice amount preservation, payment
                                          string handling, null invoice, placement
                                          hints, spawn requests, etc. (22 tests)
    test_unit_placement.py              PlacementEngine round-robin routing
    test_unit_external_agent.py         ExternalDataAgent non-blocking I/O
    test_aggregator_null_safety.py      AggregatorAgent null-safe risk fusion
    test_canonical_consumer_group_scaling.py  Kafka group_id load-balance semantics
    test_unit_self_healing.py           SelfHealingAgent lock safety + grace period
    test_unit_self_healing_health_routing.py  Health topic routing + stale event drop
    test_self_healing_concurrent.py     RLock no-deadlock (10×10 threads)

Recovery / Resilience
    test_recovery_action_correctness.py SelfHealingAgent score→action decision matrix
    test_recovery_storm_prevention.py   Cooldown suppresses duplicate restart events

DB / Storage
    test_overpayment_clamping.py        DBAgent clamps paid_amount, QueryAgent guard
    test_exactly_once_processing.py     DBAgent 10k duplicate events → 1 row
    test_integration_pipeline.py        Risk profile persistence, supervisor restart

Intelligence
    test_enrichment_ablation.py         AggregatorAgent enrichment rank correlation
    test_risk_score_monotonicity.py     RiskScoringAgent monotonicity sweeps + chart

Throughput
    test_throughput_scaling.py          Horizontal scaling with multiprocessing
                                        (NOTE: slow, ~30 s; run separately:
                                         pytest tests/regular_tests/test_throughput_scaling.py)

HOW TO RUN
----------
All regular tests (excluding slow throughput):
    pytest tests/regular_tests/ --ignore=tests/regular_tests/test_throughput_scaling.py -v

Throughput test (slow, run separately):
    pytest tests/regular_tests/test_throughput_scaling.py -v -s

By component:
    pytest tests/regular_tests/ -k "kafka" -v
    pytest tests/regular_tests/ -k "self_healing" -v
    pytest tests/regular_tests/ -k "db_agent or overpayment" -v

RELATIONSHIP TO tests/suite/
-----------------------------
Some tests here (overpayment_clamping, exactly_once_processing,
risk_score_monotonicity, enrichment_ablation) are ALSO represented in
tests/suite/ as more structured, report-grade versions.

The regular_tests/ versions are kept as developer-friendly smoke tests
because they run fast, have clear error messages, and don't produce
artifacts (charts, tables).

The suite/ versions add:
  • Statistical aggregates (P95, mean, Spearman ρ)
  • Chart outputs (tests/outputs/)
  • Honest comparison baselines
  • Formal assertions suitable for academic reporting
"""
