# ACIS-X Test Results

> **Generated:** 2026-07-23  
> **Python:** 3.14.6 · **pytest:** 9.1.1 · **Platform:** macOS (darwin, arm64)  
> **Commands run:** `python -m pytest tests/ -v` and `python -m pytest tests/suite/ -v`

---

## Summary

| Command | Scope | Collected | Passed | Failed | Duration |
|---|---|---|---|---|---|
| `python -m pytest tests/ -v` | Full suite (regular + suite) | 162 | **162** | 0 | 46.86 s |
| `python -m pytest tests/suite/ -v` | Integration/suite only | 45 | **45** | 0 | 33.23 s |

**Overall result: ✅ All tests pass. Zero failures.**

---

## Command 1 — `python -m pytest tests/ -v`

### Environment

```
platform darwin -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: /Users/johangeorge/ACIS-X
configfile: pytest.ini
plugins: anyio-4.14.2
collected 162 items
```

### Results by File

#### `tests/regular_tests/` — 117 tests

| File | Tests | Result |
|---|---|---|
| `test_aggregator_null_safety.py` | 5 | ✅ All passed |
| `test_canonical_consumer_group_scaling.py` | 4 | ✅ All passed |
| `test_enrichment_ablation.py` | 2 | ✅ All passed |
| `test_exactly_once_processing.py` | 2 | ✅ All passed |
| `test_integration_pipeline.py` | 6 | ✅ All passed |
| `test_ml_payment_prediction.py` | 3 | ✅ All passed |
| `test_overpayment_clamping.py` | 2 | ✅ All passed |
| `test_query_client_thread_isolation.py` | 3 | ✅ All passed |
| `test_recovery_action_correctness.py` | 11 | ✅ All passed |
| `test_recovery_storm_prevention.py` | 1 | ✅ All passed |
| `test_risk_score_monotonicity.py` | 4 | ✅ All passed |
| `test_self_healing_concurrent.py` | 1 | ✅ All passed |
| `test_throughput_scaling.py` | 1 | ✅ All passed |
| `test_unit_architecture_fixes.py` | 19 | ✅ All passed |
| `test_unit_circuit_breaker.py` | 4 | ✅ All passed |
| `test_unit_datetime_tz.py` | 7 | ✅ All passed |
| `test_unit_event_validation.py` | 4 | ✅ All passed |
| `test_unit_external_agent.py` | 4 | ✅ All passed |
| `test_unit_kafka_client.py` | 7 | ✅ All passed |
| `test_unit_placement.py` | 3 | ✅ All passed |
| `test_unit_query_client.py` | 4 | ✅ All passed |
| `test_unit_schema.py` | 4 | ✅ All passed |
| `test_unit_self_healing.py` | 5 | ✅ All passed |
| `test_unit_self_healing_health_routing.py` | 8 | ✅ All passed |

#### `tests/suite/` — 45 tests

| File | Tests | Result |
|---|---|---|
| `test_dynamic_discovery.py` | 2 | ✅ All passed |
| `test_integration_contracts.py` | 6 | ✅ All passed |
| `test_intelligence_comparison.py` | 3 | ✅ All passed |
| `test_lifecycle_trace.py` | 4 | ✅ All passed |
| `test_performance.py` | 3 | ✅ All passed |
| `test_self_healing_proof.py` | 5 | ✅ All passed |
| `test_unit_core.py` | 22 | ✅ All passed |

### Individual Test List (Regular Tests)

<details>
<summary>Click to expand all 117 regular test names</summary>

```
tests/regular_tests/test_aggregator_null_safety.py::TestAggregatorNullSafety::test_null_safe_risk_fusion[financial=0.6_litigation=0.3]      PASSED
tests/regular_tests/test_aggregator_null_safety.py::TestAggregatorNullSafety::test_null_safe_risk_fusion[financial=None_litigation=0.5]      PASSED
tests/regular_tests/test_aggregator_null_safety.py::TestAggregatorNullSafety::test_null_safe_risk_fusion[financial=0.6_litigation=None]       PASSED
tests/regular_tests/test_aggregator_null_safety.py::TestAggregatorNullSafety::test_null_safe_risk_fusion[financial=None_litigation=None]      PASSED
tests/regular_tests/test_aggregator_null_safety.py::TestAggregatorNullSafety::test_none_financial_does_not_overwrite_valid_score              PASSED
tests/regular_tests/test_canonical_consumer_group_scaling.py::TestCanonicalConsumerGroupScaling::test_same_group_id_load_balances             PASSED
tests/regular_tests/test_canonical_consumer_group_scaling.py::TestCanonicalConsumerGroupScaling::test_unique_group_ids_broadcast              PASSED
tests/regular_tests/test_canonical_consumer_group_scaling.py::TestCanonicalConsumerGroupScaling::test_acis_agents_use_canonical_group_ids     PASSED
tests/regular_tests/test_canonical_consumer_group_scaling.py::TestCanonicalConsumerGroupScaling::test_multiple_replicas_share_canonical_group_id PASSED
tests/regular_tests/test_enrichment_ablation.py::TestEnrichmentAblation::test_enrichment_reduces_variance_for_public_companies               PASSED
tests/regular_tests/test_enrichment_ablation.py::TestEnrichmentAblation::test_enrichment_improves_rank_correlation                           PASSED
tests/regular_tests/test_exactly_once_processing.py::TestExactlyOnceProcessing::test_10k_duplicates_produce_single_row                       PASSED
tests/regular_tests/test_exactly_once_processing.py::TestExactlyOnceProcessing::test_process_event_level_idempotency                         PASSED
tests/regular_tests/test_integration_pipeline.py::test_db_agent_schema_creates_customer_risk_profile_table                                   PASSED
tests/regular_tests/test_integration_pipeline.py::test_handle_customer_risk_profile_persists_correct_values                                  PASSED
tests/regular_tests/test_integration_pipeline.py::test_handle_customer_risk_profile_is_idempotent                                            PASSED
tests/regular_tests/test_integration_pipeline.py::test_event_type_contract_aggregator_to_db_agent                                            PASSED
tests/regular_tests/test_integration_pipeline.py::test_agent_supervisor_stores_launch_fn                                                     PASSED
tests/regular_tests/test_integration_pipeline.py::test_agent_supervisor_restart_without_launch_fn_returns_false                              PASSED
tests/regular_tests/test_ml_payment_prediction.py::TestPaymentPredictionML::test_model_warmup                                                PASSED
tests/regular_tests/test_ml_payment_prediction.py::TestPaymentPredictionML::test_process_event_2_stage_policy                                PASSED
tests/regular_tests/test_ml_payment_prediction.py::TestPaymentPredictionML::test_shap_values_integrity                                       PASSED
tests/regular_tests/test_overpayment_clamping.py::TestOverpaymentClamping::test_overpayment_clamps_to_total                                  PASSED
tests/regular_tests/test_overpayment_clamping.py::TestOverpaymentClamping::test_query_agent_never_returns_negative_remaining                  PASSED
tests/regular_tests/test_query_client_thread_isolation.py::TestQueryClientThreadIsolation::test_concurrent_queries_no_contamination           PASSED
tests/regular_tests/test_query_client_thread_isolation.py::TestQueryClientThreadIsolation::test_threading_local_provides_isolation            PASSED
tests/regular_tests/test_query_client_thread_isolation.py::TestQueryClientThreadIsolation::test_wrong_correlation_id_is_rejected              PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_decision_matrix_mapping[DEGRADED_high_lag]       PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_decision_matrix_mapping[CRITICAL_no_lag]         PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_decision_matrix_mapping[ERROR_low_cpu]           PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_decision_matrix_mapping[OVERLOADED_high_cpu]     PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_decision_matrix_mapping[TIMEOUT]                 PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_boundary_at_degraded_threshold             PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_boundary_at_scale_threshold                PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_boundary_at_critical_threshold             PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_below_degraded_threshold                   PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_at_maximum                                 PASSED
tests/regular_tests/test_recovery_action_correctness.py::TestRecoveryActionCorrectness::test_score_at_zero                                    PASSED
tests/regular_tests/test_recovery_storm_prevention.py::TestRecoveryStormPrevention::test_duplicate_degraded_events_suppressed                 PASSED
tests/regular_tests/test_risk_score_monotonicity.py::TestRiskScoreMonotonicity::test_avg_delay_monotonic                                     PASSED
tests/regular_tests/test_risk_score_monotonicity.py::TestRiskScoreMonotonicity::test_on_time_ratio_monotonic                                  PASSED
tests/regular_tests/test_risk_score_monotonicity.py::TestRiskScoreMonotonicity::test_overdue_count_monotonic                                  PASSED
tests/regular_tests/test_risk_score_monotonicity.py::TestRiskScoreMonotonicity::test_generate_monotonicity_chart                              PASSED
tests/regular_tests/test_self_healing_concurrent.py::TestSelfHealingConcurrent::test_rlock_no_deadlock_10x10                                 PASSED
tests/regular_tests/test_throughput_scaling.py::TestThroughputScaling::test_throughput_scales_with_replicas                                  PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_customer_identity_contract                                                         PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_metrics_enrichment_with_company_name                                               PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_consumer_group_scaling_canonical_group_id                                          PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_external_agent_fallback_chain                                                      PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_producer_only_agent_lifecycle                                                      PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_base_agent_start_skips_kafka_subscription_for_producer_only_agent                  PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_lazy_kafka_producer_init                                                           PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_runtime_manager_spawn_request_requests_single_placement_with_incremented_replica_count PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_placement_engine_preserves_restart_context                                         PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_self_healing_emits_single_spawn_request_with_placement_hints                       PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_db_agent_preserves_existing_invoice_total_when_status_update_omits_amount           PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_memory_agent_recompute_state_tolerates_null_invoice_amounts                        PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_customer_state_metrics_tolerate_null_invoice_amounts                                PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_query_agent_clamps_negative_remaining_amounts                                      PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_lock_contention_optimization                                                       PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_self_healing_agent_bug_fix                                                         PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_db_agent_handles_payment_partial_with_string_amount                                 PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_db_agent_rejects_non_numeric_payment_amount                                        PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_kafka_client_init_consumer_rejects_unknown_backend                                 PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_memory_agent_persist_metrics_creates_missing_customer                               PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_db_agent_repair_payment_integrity_backfills_orphans_and_clamps_paid                 PASSED
tests/regular_tests/test_unit_architecture_fixes.py::test_external_scraping_news_analysis_uses_description_keywords                          PASSED
tests/regular_tests/test_unit_circuit_breaker.py::test_circuit_breaker_failures_open_circuit                                                 PASSED
tests/regular_tests/test_unit_circuit_breaker.py::test_circuit_breaker_open_raises_immediately                                               PASSED
tests/regular_tests/test_unit_circuit_breaker.py::test_circuit_breaker_recovery_to_half_open                                                 PASSED
tests/regular_tests/test_unit_circuit_breaker.py::test_circuit_breaker_half_open_to_closed                                                   PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_tz_aware_string_parsed_to_naive_datetime                  PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_tz_aware_string_is_a_datetime_instance                    PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_utc_string_no_offset_parsed_correctly                     PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_naive_utcnow_roundtrip                                    PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_timezone_aware_utc_stripped                               PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_positive_offset_normalised                                PASSED
tests/regular_tests/test_unit_datetime_tz.py::TestEventTimezoneNormalisation::test_negative_offset_normalised                                PASSED
tests/regular_tests/test_unit_event_validation.py::test_valid_event_passes_validation                                                        PASSED
tests/regular_tests/test_unit_event_validation.py::test_wrong_schema_version_goes_to_dlq                                                     PASSED
tests/regular_tests/test_unit_event_validation.py::test_invalid_event_type_goes_to_dlq                                                       PASSED
tests/regular_tests/test_unit_event_validation.py::test_missing_required_field_goes_to_dlq                                                   PASSED
tests/regular_tests/test_unit_external_agent.py::TestExternalDataAgentNonBlocking::test_handle_event_returns_before_http_completes            PASSED
tests/regular_tests/test_unit_external_agent.py::TestExternalDataAgentNonBlocking::test_handle_event_submits_to_executor_not_inline           PASSED
tests/regular_tests/test_unit_external_agent.py::TestExternalDataAgentNonBlocking::test_handle_event_missing_customer_id_returns_early        PASSED
tests/regular_tests/test_unit_external_agent.py::TestExternalDataAgentNonBlocking::test_handle_event_does_not_block_consumer_thread           PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_with_message_commits_offset_plus_one                 PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_with_message_does_not_call_bare_commit               PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_without_message_uses_bare_commit                     PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_offset_is_exactly_message_offset_plus_one            PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_only_touches_correct_partition                       PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_when_no_consumer_is_a_no_op                          PASSED
tests/regular_tests/test_unit_kafka_client.py::TestKafkaClientCommitOffset::test_commit_propagates_consumer_exceptions                       PASSED
tests/regular_tests/test_unit_placement.py::test_placement_routing_round_robin                                                               PASSED
tests/regular_tests/test_unit_placement.py::test_placement_routing_no_capable_agent                                                          PASSED
tests/regular_tests/test_unit_placement.py::test_placement_routing_registry_update                                                           PASSED
tests/regular_tests/test_unit_query_client.py::TestQueryClientThreadIsolation::test_same_thread_reuses_consumer                              PASSED
tests/regular_tests/test_unit_query_client.py::TestQueryClientThreadIsolation::test_different_threads_get_different_consumers                 PASSED
tests/regular_tests/test_unit_query_client.py::TestQueryClientThreadIsolation::test_concurrent_query_calls_timeout_independently              PASSED
tests/regular_tests/test_unit_query_client.py::TestQueryClientThreadIsolation::test_no_cross_contamination_of_correlation_ids                 PASSED
tests/regular_tests/test_unit_schema.py::test_schema_creation                                                                                PASSED
tests/regular_tests/test_unit_schema.py::test_customer_insertion                                                                             PASSED
tests/regular_tests/test_unit_schema.py::test_invoice_queries                                                                                PASSED
tests/regular_tests/test_unit_schema.py::test_get_customer_with_enrichment_fields                                                            PASSED
tests/regular_tests/test_unit_self_healing.py::TestSelfHealingLockSafety::test_handle_degraded_completes_without_deadlock                    PASSED
tests/regular_tests/test_unit_self_healing.py::TestSelfHealingLockSafety::test_publish_restart_reacquires_rlock_while_held                   PASSED
tests/regular_tests/test_unit_self_healing.py::TestSelfHealingLockSafety::test_evaluate_state_sets_last_restart_requested                    PASSED
tests/regular_tests/test_unit_self_healing.py::TestSelfHealingLockSafety::test_handle_degraded_within_grace_period_skips_restart             PASSED
tests/regular_tests/test_unit_self_healing.py::TestSelfHealingLockSafety::test_concurrent_handle_degraded_does_not_deadlock                  PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_monitoring_agent_health_topic_is_acis_agent_health                        PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_monitoring_agent_publishes_degraded_to_health_topic                       PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_self_healing_agent_subscribes_to_health_topic                             PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_self_healing_agent_subscribed_topics_init_matches_subscribe               PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_self_healing_process_event_routes_degraded_to_handle_degraded             PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_self_healing_process_event_routes_critical_to_handle_critical             PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_self_healing_drops_stale_health_events                                    PASSED
tests/regular_tests/test_unit_self_healing_health_routing.py::test_handle_degraded_sets_last_degraded_at                                     PASSED
```

</details>

---

## Command 2 — `python -m pytest tests/suite/ -v`

### Environment

```
platform darwin -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: /Users/johangeorge/ACIS-X
configfile: pytest.ini
plugins: anyio-4.14.2
collected 45 items
```

### Individual Test List

```
tests/suite/test_dynamic_discovery.py::TestDynamicDiscovery::test_agent_dynamic_registration                                                 PASSED [  4%]
tests/suite/test_dynamic_discovery.py::TestDynamicDiscovery::test_multi_agent_concurrent_discovery                                           PASSED [  8%]
tests/suite/test_integration_contracts.py::TestRiskProfilePersistence::test_persists_correct_values                                          PASSED [ 11%]
tests/suite/test_integration_contracts.py::TestRiskProfilePersistence::test_idempotent_double_write                                          PASSED [ 13%]
tests/suite/test_integration_contracts.py::TestRiskProfilePersistence::test_event_type_contract_aggregator_to_db_agent                       PASSED [ 15%]
tests/suite/test_integration_contracts.py::TestCSAtoPPAContract::test_metrics_event_contains_required_fields                                 PASSED [ 17%]
tests/suite/test_integration_contracts.py::TestRSAtoCollectionsContract::test_risk_scored_contains_required_fields                           PASSED [ 20%]
tests/suite/test_integration_contracts.py::TestCorrelationIdPropagation::test_correlation_id_flows_through_csa_ppa_rsa                       PASSED [ 22%]
tests/suite/test_intelligence_comparison.py::TestACISvsNaiveBaseline::test_acis_f1_exceeds_naive_f1                                          PASSED [ 24%]
tests/suite/test_intelligence_comparison.py::TestSignalFusionVsSingleSignals::test_acis_rank_correlation_dominates_single_signals             PASSED [ 26%]
tests/suite/test_intelligence_comparison.py::TestExternalEnrichmentAblation::test_enrichment_improves_rank_correlation                       PASSED [ 28%]
tests/suite/test_lifecycle_trace.py::TestInvoiceFullLifecycle::test_single_invoice_lifecycle_completes                                       PASSED [ 31%]
tests/suite/test_lifecycle_trace.py::TestInvoiceFullLifecycle::test_correlation_id_propagates_through_pipeline                               PASSED [ 33%]
tests/suite/test_lifecycle_trace.py::TestInvoiceFullLifecycle::test_stage_latency_breakdown_N50                                              PASSED [ 35%]
tests/suite/test_lifecycle_trace.py::TestPaymentRescoreLifecycle::test_payment_triggers_risk_rescore                                         PASSED [ 37%]
tests/suite/test_performance.py::TestPipelineP95Latency::test_p95_latency_under_500ms                                                        PASSED [ 40%]
tests/suite/test_performance.py::TestSelfHealingMTTR::test_mttr_50_iterations                                                                PASSED [ 42%]
tests/suite/test_performance.py::TestCollectionsAgentDuplicatePrevention::test_no_duplicate_actions_for_repeated_risk_scored                  PASSED [ 44%]
tests/suite/test_self_healing_proof.py::TestAgentHardFailure::test_hard_failure_triggers_restart                                             PASSED [ 46%]
tests/suite/test_self_healing_proof.py::TestLatencySpike::test_latency_spike_triggers_recovery                                               PASSED [ 48%]
tests/suite/test_self_healing_proof.py::TestCascadingFailureStormPrevention::test_each_failed_agent_recovers_exactly_once                    PASSED [ 51%]
tests/suite/test_self_healing_proof.py::TestRecoverySuccessRate::test_100_cycles_success_rate                                                PASSED [ 53%]
tests/suite/test_self_healing_proof.py::TestRecoveryTimeDistribution::test_recovery_time_distribution                                        PASSED [ 55%]
tests/suite/test_unit_core.py::TestDBAgentContracts::test_invoice_amount_preserved_on_status_update                                          PASSED [ 57%]
tests/suite/test_unit_core.py::TestDBAgentContracts::test_overpayment_clamps_paid_amount                                                     PASSED [ 60%]
tests/suite/test_unit_core.py::TestDBAgentContracts::test_exactly_once_db_level_guard                                                        PASSED [ 62%]
tests/suite/test_unit_core.py::TestDBAgentContracts::test_query_agent_clamps_negative_remaining                                              PASSED [ 64%]
tests/suite/test_unit_core.py::TestRiskScoreMonotonicity::test_avg_delay_monotonic                                                           PASSED [ 66%]
tests/suite/test_unit_core.py::TestRiskScoreMonotonicity::test_on_time_ratio_monotonic                                                       PASSED [ 68%]
tests/suite/test_unit_core.py::TestRiskScoreMonotonicity::test_overdue_count_monotonic                                                       PASSED [ 71%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.1-None]                                PASSED [ 73%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.29-None]                               PASSED [ 75%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.3-send_reminder]                       PASSED [ 77%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.5-send_reminder]                       PASSED [ 79%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.6-escalate_invoice]                    PASSED [ 81%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.75-escalate_invoice]                   PASSED [ 82%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.8-hold_credit]                         PASSED [ 84%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.89-hold_credit]                        PASSED [ 86%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[0.9-legal_escalation]                    PASSED [ 88%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_base_severity_to_action[1.0-legal_escalation]                    PASSED [ 90%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_overdue_count_escalates_action                                   PASSED [ 93%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_chronic_delay_escalates_action                                   PASSED [ 95%]
tests/suite/test_unit_core.py::TestCollectionsAgentDecisionThresholds::test_90_day_overdue_invoice_forces_critical_priority                  PASSED [ 97%]
tests/suite/test_unit_core.py::TestRiskScoringContextTTL::test_cleanup_evicts_expired_entries                                                PASSED [ 99%]
tests/suite/test_unit_core.py::TestRiskScoringContextTTL::test_cleanup_enforces_max_size                                                     PASSED [100%]
```

---

## Warnings

Both runs emit `DeprecationWarning: datetime.datetime.utcnow() is deprecated` across a large number of tests (~50 000+ warning instances total). These are **non-blocking** — all tests pass regardless. The warnings originate from calls to `datetime.utcnow()` in both production code and test helpers. The recommended migration is to replace these with `datetime.now(datetime.UTC)` for Python 3.11+.

**Affected source files (production code):**

| File | Approximate warning count |
|---|---|
| `agents/storage/db_agent.py` | ~10 000+ |
| `agents/intelligence/aggregator_agent.py` | ~500 |
| `agents/base/base_agent.py` | ~50 |
| `self_healing/core/self_healing_agent.py` | ~50 |

**Affected test files:**

| File | Approximate warning count |
|---|---|
| `tests/regular_tests/test_canonical_consumer_group_scaling.py` | ~600 |
| `tests/regular_tests/test_throughput_scaling.py` | ~1 000 |
| `tests/regular_tests/test_self_healing_concurrent.py` | ~100 |
| `tests/suite/test_self_healing_proof.py` | ~500 |
| Various others | ~50 each |

---

## Historical Context — Recent Fixes Reflected in These Results

The following bugs were diagnosed and fixed prior to this clean test run:

| Fix | File(s) Changed | Symptom Before Fix |
|---|---|---|
| `OffsetAndMetadata` 3-arg constructor incompatible with `kafka-python 2.2.3` on macOS | `runtime/kafka_client.py`, `tests/regular_tests/test_unit_kafka_client.py` | Flood of `OffsetAndMetadata.__new__() takes 3 positional arguments but 4 were given` errors; offset commits failing for every agent |
| Kafka broker rebalance storm at startup | `docker-compose.yml` | Continuous `Heartbeat failed for group … because it is rebalancing`; caused by `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` with 20+ agents joining simultaneously |
| `KafkaClient` defaulting to `confluent` backend (not installed on this machine) | `tests/suite/test_dynamic_discovery.py` | `ModuleNotFoundError: No module named 'confluent_kafka'` inside daemon threads; `_consumer` remained `None`; assertion failure in dynamic discovery tests |
| `agent.agent_id` attribute does not exist on `BaseAgent` | `tests/suite/test_dynamic_discovery.py` | `AttributeError: 'PaymentPredictionAgent' object has no attribute 'agent_id'`; corrected to `agent.instance_id` |

---

## Notes

- Tests marked `pytest.mark.integration` (in `test_dynamic_discovery.py`) require a live Kafka broker on `localhost:9092`. They auto-skip with `pytest.skip()` if no broker is reachable, and were run here with the Docker Compose cluster active.
- The `tests/suite/` run is a **strict subset** of `tests/` — all 45 suite tests appear in both run outputs.
- No tests are currently marked `xfail` or permanently skipped.
