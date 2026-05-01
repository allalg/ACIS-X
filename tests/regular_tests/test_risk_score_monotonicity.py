"""
tests/test_risk_score_monotonicity.py

Objective
---------
Prove that ``RiskScoringAgent._refine_risk_with_context()`` behaves
monotonically as credit signals deteriorate.

Methodology
-----------
Hold all variables static except the one under test, then sweep that
variable from "healthy" to "stressed" and assert each successive
risk score is >= the previous one.

Sweeps:
  1. ``avg_delay``:    0 → 90 days (step 10)
  2. ``on_time_ratio``: 1.0 → 0.0 (step −0.1)
  3. ``overdue_count``: 0 → 10  (step 1)

A 3-subplot line chart is saved to ``tests/outputs/monotonicity_proof.png``.

Marker
------
Tagged with ``@pytest.mark.novel``.
"""

import logging
import os
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "outputs")
CHART_PATH = os.path.join(OUTPUT_DIR, "monotonicity_proof.png")

# Baseline values — held constant unless the test explicitly sweeps them.
BASELINE = {
    "customer_id": "cust_mono_001",
    "credit_limit": 500_000.0,
    "total_outstanding": 100_000.0,
    "avg_delay": 10.0,
    "on_time_ratio": 0.80,
    "overdue_count": 1,
}

# Risk velocity (stable)
VELOCITY = {"velocity": 0.0, "trend": "stable", "volatility": 0.02}


def _build_query_handler(overrides: Dict[str, Any]):
    """Return a QueryClient.query side_effect with specific metric overrides."""
    def handler(query_type: str, params: dict = None, **kwargs):
        params = params or {}
        if query_type == "get_customer_metrics":
            result = dict(BASELINE)
            result.update(overrides)
            return result
        if query_type == "get_risk_velocity":
            return VELOCITY
        return None
    return handler


@pytest.mark.novel
class TestRiskScoreMonotonicity:
    """Prove monotonicity of _refine_risk_with_context under factor sweeps."""

    def _sweep(
        self,
        field: str,
        values: list,
    ) -> List[float]:
        """Sweep a single field and return the risk scores.

        For each value in *values*, mock QueryClient to return metrics
        with ``field`` set to that value, then call
        ``_refine_risk_with_context()`` and record the output.
        """
        from agents.risk.risk_scoring_agent import RiskScoringAgent

        kafka = MagicMock()
        kafka.publish.return_value = True
        agent = RiskScoringAgent(kafka_client=kafka)

        scores: List[float] = []
        base_risk = 0.30  # fixed baseline prediction
        confidence = 0.80

        for val in values:
            handler = _build_query_handler({field: val})
            with patch("utils.query_client.QueryClient.query", side_effect=handler):
                reasons: List[str] = []
                score = agent._refine_risk_with_context(
                    customer_id="cust_mono_001",
                    invoice_id="inv_mono_001",
                    base_risk=base_risk,
                    confidence=confidence,
                    reasons=reasons,
                )
                scores.append(score)

        return scores

    def test_avg_delay_monotonic(self) -> None:
        """Risk must increase (or stay flat) as avg_delay grows 0 → 90."""
        delays = list(range(0, 91, 10))
        scores = self._sweep("avg_delay", delays)

        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Monotonicity violation at avg_delay={delays[i]}: "
                f"score={scores[i]:.6f} < prev={scores[i-1]:.6f}"
            )

        logger.info(
            f"avg_delay sweep: {list(zip(delays, [f'{s:.4f}' for s in scores]))}"
        )

    def test_on_time_ratio_monotonic(self) -> None:
        """Risk must increase (or stay flat) as on_time_ratio drops 1.0 → 0.0."""
        ratios = [round(1.0 - i * 0.1, 1) for i in range(11)]  # 1.0, 0.9, ..., 0.0
        scores = self._sweep("on_time_ratio", ratios)

        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Monotonicity violation at on_time_ratio={ratios[i]}: "
                f"score={scores[i]:.6f} < prev={scores[i-1]:.6f}"
            )

        logger.info(
            f"on_time_ratio sweep: {list(zip(ratios, [f'{s:.4f}' for s in scores]))}"
        )

    def test_overdue_count_monotonic(self) -> None:
        """Risk must increase (or stay flat) as overdue_count grows 0 → 10."""
        counts = list(range(0, 11))
        scores = self._sweep("overdue_count", counts)

        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1] - 1e-9, (
                f"Monotonicity violation at overdue_count={counts[i]}: "
                f"score={scores[i]:.6f} < prev={scores[i-1]:.6f}"
            )

        logger.info(
            f"overdue_count sweep: {list(zip(counts, [f'{s:.4f}' for s in scores]))}"
        )

    def test_generate_monotonicity_chart(self) -> None:
        """Generate a 3-subplot chart proving monotonicity."""
        delays = list(range(0, 91, 10))
        delay_scores = self._sweep("avg_delay", delays)

        ratios = [round(1.0 - i * 0.1, 1) for i in range(11)]
        ratio_scores = self._sweep("on_time_ratio", ratios)

        counts = list(range(0, 11))
        count_scores = self._sweep("overdue_count", counts)

        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            os.makedirs(OUTPUT_DIR, exist_ok=True)

            fig, axes = plt.subplots(1, 3, figsize=(18, 5))

            # Subplot 1: avg_delay
            axes[0].plot(delays, delay_scores, "o-", color="#ea4335", linewidth=2)
            axes[0].set_title("avg_delay (days)", fontsize=12, fontweight="bold")
            axes[0].set_xlabel("avg_delay (days)")
            axes[0].set_ylabel("Risk Score")
            axes[0].grid(alpha=0.3)

            # Subplot 2: on_time_ratio (x-axis inverted for deterioration)
            axes[1].plot(ratios, ratio_scores, "s-", color="#4285f4", linewidth=2)
            axes[1].set_title("on_time_ratio", fontsize=12, fontweight="bold")
            axes[1].set_xlabel("on_time_ratio (1.0 = best)")
            axes[1].set_ylabel("Risk Score")
            axes[1].invert_xaxis()
            axes[1].grid(alpha=0.3)

            # Subplot 3: overdue_count
            axes[2].plot(counts, count_scores, "^-", color="#34a853", linewidth=2)
            axes[2].set_title("overdue_count", fontsize=12, fontweight="bold")
            axes[2].set_xlabel("overdue_count")
            axes[2].set_ylabel("Risk Score")
            axes[2].grid(alpha=0.3)

            fig.suptitle(
                "ACIS-X Risk Score Monotonicity Proof\n"
                "(base_risk=0.30, confidence=0.80)",
                fontsize=14,
                fontweight="bold",
            )
            plt.tight_layout()
            plt.savefig(CHART_PATH, dpi=150)
            plt.close(fig)

            logger.info(f"Monotonicity chart saved to {CHART_PATH}")
        except ImportError:
            logger.warning("matplotlib not available — skipping chart")
