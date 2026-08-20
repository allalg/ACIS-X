"""
LFTD-Style Latent Stress Scenario Generator for ACIS-X.

Implements a Latent Financial Time-series Diffusion-inspired multi-variate
stochastic jump-diffusion test harness.

PURPOSE:
- Provides a controlled, reproducible test harness for evaluating ACIS-X
  under diverse macroeconomic stress regimes, correlated delinquency bursts,
  and fat-tailed payment delay distributions.

DISCLAIMER:
- This module is strictly designed as an empirical stress-test harness for
  algorithmic evaluation and resilience benchmarking. It does NOT claim
  perfect or complete replication of live macroeconomic dynamics.
"""

import math
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum
import numpy as np


class StressRegime(str, Enum):
    """Macroeconomic stress regimes for scenario simulation."""
    CALM = "calm"
    STRESSED = "stressed"
    CRISIS = "crisis"


class LFTDStressGenerator:
    """
    Latent Financial Time-series Diffusion-inspired synthetic test harness.

    Simulates latent macroeconomic stress dynamics:
        dZ_t = kappa * (mu - Z_t) * dt + sigma * dW_t + J_t * dN_t
    where:
        - Z_t: Latent enterprise credit stress state in [0, 1]
        - kappa: Mean-reversion speed
        - mu: Long-term equilibrium stress level
        - sigma: Diffusion volatility
        - dW_t: Standard Brownian motion increment
        - J_t * dN_t: Compound Poisson jump process (liquidity shocks)
    """

    def __init__(
        self,
        kappa: float = 0.5,
        mu: float = 0.25,
        sigma: float = 0.15,
        jump_intensity: float = 0.05,
        jump_mean: float = 0.20,
        seed: Optional[int] = 42,
    ):
        self.kappa = kappa
        self.mu = mu
        self.sigma = sigma
        self.jump_intensity = jump_intensity
        self.jump_mean = jump_mean
        self.rng = np.random.default_rng(seed)

        # Current latent state
        self._z_t: float = mu
        self._step_count: int = 0

    def step_latent_stress(self, dt: float = 1.0) -> float:
        """
        Advance the latent stress process by dt time step.
        Returns the updated latent stress factor Z_t in [0, 1].
        """
        # Mean-reverting drift
        drift = self.kappa * (self.mu - self._z_t) * dt

        # Stochastic diffusion increment
        diffusion = self.sigma * math.sqrt(dt) * self.rng.standard_normal()

        # Poisson jump (sudden credit event / liquidity shock)
        jump = 0.0
        if self.rng.random() < (self.jump_intensity * dt):
            jump = self.rng.exponential(scale=self.jump_mean)

        # Update latent state with bounding
        self._z_t = float(np.clip(self._z_t + drift + diffusion + jump, 0.01, 0.99))
        self._step_count += 1
        return self._z_t

    def get_current_regime(self) -> StressRegime:
        """Classify the current macro regime based on latent stress level."""
        if self._z_t < 0.35:
            return StressRegime.CALM
        elif self._z_t < 0.65:
            return StressRegime.STRESSED
        else:
            return StressRegime.CRISIS

    def sample_payment_delay(self, base_delay: float = 0.0) -> float:
        """
        Sample a payment delay in days using a regime-conditioned fat-tailed distribution.
        Uses a Pareto-mixture to model long-tail delinquency bursts.
        """
        regime = self.get_current_regime()

        if regime == StressRegime.CALM:
            # 90% on-time or short delay, 10% minor delay
            if self.rng.random() < 0.90:
                return float(max(0.0, self.rng.normal(base_delay, 1.5)))
            return float(self.rng.exponential(scale=5.0))

        elif regime == StressRegime.STRESSED:
            # Shifted exponential with moderate spread
            return float(base_delay + self.rng.exponential(scale=12.0) + self.rng.uniform(2.0, 15.0))

        else:  # CRISIS
            # Fat-tailed Pareto distribution (extreme tail delays)
            pareto_shape = 1.8  # Heavy-tailed
            tail_sample = float((self.rng.pareto(pareto_shape) + 1.0) * 15.0)
            return float(min(120.0, base_delay + tail_sample + 20.0))

    def sample_customer_risk_profile(
        self,
        customer_id: str,
        base_credit_limit: float = 500000.0,
    ) -> Dict[str, Any]:
        """
        Generate a multi-variate customer state vector conditioned on latent stress Z_t.
        """
        z = self._z_t
        regime = self.get_current_regime()

        # Credit utilization scales with stress
        utilization = float(np.clip(self.rng.beta(2 + 4 * z, 4 - 2 * z), 0.05, 0.98))

        # On-time payment ratio degrades as stress rises
        on_time_ratio = float(np.clip(1.0 - (z * 0.7 + self.rng.normal(0, 0.08)), 0.05, 0.98))

        # Average payment delay (days)
        avg_delay = self.sample_payment_delay(base_delay=z * 15.0)

        # External financial ratios
        debt_to_equity = float(max(0.2, 1.0 + z * 3.0 + self.rng.normal(0, 0.3)))
        roe = float(0.20 - z * 0.25 + self.rng.normal(0, 0.03))
        pe_ratio = float(max(5.0, 25.0 - z * 18.0 + self.rng.normal(0, 2.0)))

        # Litigation exposure probability
        litigation_prob = 0.05 + 0.50 * z
        has_litigation = bool(self.rng.random() < litigation_prob)
        litigation_risk = float(np.clip(self.rng.beta(2, 5) + (0.4 if has_litigation else 0.0), 0.0, 1.0))

        return {
            "customer_id": customer_id,
            "latent_stress_z": round(z, 4),
            "macro_regime": regime.value,
            "credit_utilization": round(utilization, 4),
            "on_time_ratio": round(on_time_ratio, 4),
            "avg_delay_days": round(avg_delay, 2),
            "debt_to_equity": round(debt_to_equity, 2),
            "roe": round(roe, 4),
            "pe_ratio": round(pe_ratio, 2),
            "has_litigation": has_litigation,
            "litigation_risk": round(litigation_risk, 4),
            "credit_limit": base_credit_limit,
            "outstanding_amount": round(base_credit_limit * utilization, 2),
        }

    def generate_stress_batch(self, n_customers: int = 50) -> List[Dict[str, Any]]:
        """
        Generate a batch of customer profiles under the current latent stress state.
        """
        self.step_latent_stress(dt=1.0)
        return [
            self.sample_customer_risk_profile(f"CUST_LFTD_{i:04d}")
            for i in range(1, n_customers + 1)
        ]
