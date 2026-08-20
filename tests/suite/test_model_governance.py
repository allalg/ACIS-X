"""
Test suite for Layer 6 Autonomous Model Risk Governance (ModelGovernanceAgent).

Tests:
1. Population Stability Index (PSI) calculation under stable vs drifted distributions.
2. Kolmogorov-Smirnov (KS) two-sample feature drift detection.
3. Online Challenger model training and champion-vs-challenger evaluation.
4. ModelGovernanceAgent end-to-end event handling and audit report generation.
"""

import pytest
import numpy as np
from datetime import datetime, timezone
from unittest.mock import MagicMock

from agents.governance.model_governance_agent import ModelGovernanceAgent
from schemas.event_schema import Event


@pytest.fixture
def mock_kafka():
    client = MagicMock()
    client.publish = MagicMock()
    return client


class TestModelGovernanceMetrics:
    """Unit tests for statistical drift formulas (PSI and KS-test)."""

    def test_psi_stable_distribution(self, mock_kafka):
        """PSI on identical or slightly perturbed distributions should be < 0.10 (stable)."""
        agent = ModelGovernanceAgent(kafka_client=mock_kafka)
        np.random.seed(42)
        baseline = list(np.random.normal(0.5, 0.1, size=500))
        target = list(np.random.normal(0.5, 0.1, size=500))

        psi = agent.compute_psi(baseline, target)
        assert psi < 0.10, f"Expected stable PSI (<0.10), got {psi}"

    def test_psi_significant_drift(self, mock_kafka):
        """PSI on heavily shifted distributions should be >= 0.25 (significant drift)."""
        agent = ModelGovernanceAgent(kafka_client=mock_kafka)
        np.random.seed(42)
        baseline = list(np.random.normal(0.2, 0.05, size=200))
        target = list(np.random.normal(0.8, 0.05, size=200))

        psi = agent.compute_psi(baseline, target)
        assert psi >= 0.25, f"Expected significant drift PSI (>=0.25), got {psi}"

    def test_ks_feature_drift(self, mock_kafka):
        """KS statistic on shifted feature values should detect divergence."""
        agent = ModelGovernanceAgent(kafka_client=mock_kafka)
        np.random.seed(42)
        baseline = list(np.random.uniform(0.0, 0.3, size=100))
        target = list(np.random.uniform(0.6, 1.0, size=100))

        ks_stat = agent.compute_ks_drift(baseline, target)
        assert ks_stat > 0.5, f"Expected high KS distance (>0.5), got {ks_stat}"


class TestModelGovernanceAgentLifecycle:
    """Integration tests for ModelGovernanceAgent event processing."""

    def test_prediction_event_triggers_governance(self, mock_kafka):
        """Streaming prediction events should be ingested and trigger governance reports."""
        agent = ModelGovernanceAgent(
            kafka_client=mock_kafka,
            window_size=50,
            drift_check_interval=20,
        )

        # Feed 25 prediction events
        for i in range(25):
            event = Event(
                event_id=f"pred_evt_{i}",
                event_type="payment.risk.predicted",
                event_source="PaymentPredictionAgent",
                event_time=datetime.now(timezone.utc).replace(tzinfo=None),
                correlation_id=f"corr_{i}",
                entity_id=f"CUST_{i}",
                payload={
                    "customer_id": f"CUST_{i}",
                    "predicted_risk_score": 0.35 + (i % 5) * 0.05,
                    "features": {
                        "payment_delay": 0.1,
                        "credit_utilization": 0.4,
                        "invoice_size_ratio": 0.2,
                        "late_payment_rate": 0.1,
                        "external_risk": 0.2,
                        "financial_weakness": 0.2,
                        "litigation_risk": 0.0,
                        "credit_rating_penalty": 0.0,
                    },
                },
            )
            agent.process_event(event)

        # Verify that publish_event was called with governance report
        assert mock_kafka.publish.called
        published_topics = [call[0][0] for call in mock_kafka.publish.call_args_list]
        assert "acis.system" in published_topics

        metrics = agent.get_latest_metrics()
        assert metrics["eval_count"] == 25
        assert metrics["window_size"] == 25
