"""
Test suite for LFTD-Style Latent Stress Scenario Generator (LFTDStressGenerator).

Tests:
1. Latent jump-diffusion process stability and bounding in [0, 1].
2. Macroeconomic regime classification (CALM, STRESSED, CRISIS).
3. Regime-conditioned fat-tailed payment delay generation.
4. Multi-variate customer state vector sampling.
"""

import pytest
import numpy as np

from agents.scenario_generator.lftd_stress_generator import LFTDStressGenerator, StressRegime


class TestLFTDStressDynamics:
    """Tests for latent stochastic stress generation."""

    def test_latent_stress_bounding(self):
        """Latent stress Z_t should remain strictly within (0, 1) across 100 steps."""
        generator = LFTDStressGenerator(kappa=0.5, mu=0.25, sigma=0.15, seed=42)
        for _ in range(100):
            z = generator.step_latent_stress(dt=1.0)
            assert 0.0 < z < 1.0, f"Latent stress {z} out of bounds"

    def test_regime_transitions(self):
        """Generator should correctly map stress levels to macro regimes."""
        generator = LFTDStressGenerator(seed=42)

        generator._z_t = 0.20
        assert generator.get_current_regime() == StressRegime.CALM

        generator._z_t = 0.50
        assert generator.get_current_regime() == StressRegime.STRESSED

        generator._z_t = 0.80
        assert generator.get_current_regime() == StressRegime.CRISIS

    def test_fat_tailed_delays_under_crisis(self):
        """Under CRISIS regime, sampled payment delays should exhibit severe delays."""
        generator = LFTDStressGenerator(seed=42)
        generator._z_t = 0.85  # Force CRISIS

        delays = [generator.sample_payment_delay(base_delay=10.0) for _ in range(50)]
        mean_delay = float(np.mean(delays))
        max_delay = float(np.max(delays))

        assert mean_delay > 20.0, f"Expected elevated mean delay in crisis, got {mean_delay}"
        assert max_delay > 40.0, f"Expected fat-tail extreme delay in crisis, got {max_delay}"

    def test_customer_risk_profile_structure(self):
        """Sampled customer profiles should contain all required multi-variate fields."""
        generator = LFTDStressGenerator(seed=42)
        profile = generator.sample_customer_risk_profile("CUST_001")

        required_keys = [
            "customer_id",
            "latent_stress_z",
            "macro_regime",
            "credit_utilization",
            "on_time_ratio",
            "avg_delay_days",
            "debt_to_equity",
            "roe",
            "pe_ratio",
            "has_litigation",
            "litigation_risk",
            "credit_limit",
            "outstanding_amount",
        ]
        for key in required_keys:
            assert key in profile, f"Missing required key {key} in profile"
            assert profile[key] is not None
