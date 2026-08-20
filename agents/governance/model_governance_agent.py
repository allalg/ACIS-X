from datetime import timezone, datetime
import logging
import uuid
import collections
from typing import Dict, List, Any, Optional
import numpy as np
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import LogisticRegression

from agents.base.base_agent import BaseAgent
from schemas.event_schema import (
    Event,
    ModelDriftDetectedPayload,
    ModelGovernanceReportPayload,
)

logger = logging.getLogger(__name__)


class ModelGovernanceAgent(BaseAgent):
    """
    Autonomous Model Risk Governance Agent for ACIS-X (Layer 6).

    Monitors production model performance and risk outputs in real-time:
    - Computes Population Stability Index (PSI) on predicted risk distributions
    - Tracks Kolmogorov-Smirnov (KS) feature drift on incoming financial metrics
    - Trains and benchmarks an online Challenger Model against the Champion Random Forest
    - Automatically publishes audit reports and drift alert events
    """

    TOPIC_PREDICTIONS = "acis.predictions"
    TOPIC_METRICS = "acis.metrics"
    TOPIC_SYSTEM = "acis.system"
    TOPIC_HEALTH = "acis.agent.health"

    FEATURE_KEYS = [
        "payment_delay",
        "credit_utilization",
        "invoice_size_ratio",
        "late_payment_rate",
        "external_risk",
        "financial_weakness",
        "litigation_risk",
        "credit_rating_penalty",
    ]

    def __init__(
        self,
        kafka_client: Any = None,
        instance_id: Optional[str] = None,
        window_size: int = 200,
        psi_threshold: float = 0.10,
        drift_check_interval: int = 50,
    ):
        super().__init__(
            agent_name="ModelGovernanceAgent",
            agent_version="1.0.0",
            group_id="model-governance-group",
            subscribed_topics=[self.TOPIC_PREDICTIONS, self.TOPIC_METRICS],
            capabilities=[
                "model_monitoring",
                "drift_detection",
                "challenger_benchmarking",
                "governance_audit",
            ],
            kafka_client=kafka_client,
            agent_type="ModelGovernanceAgent",
            instance_id=instance_id,
        )
        self.window_size = window_size
        self.psi_threshold = psi_threshold
        self.drift_check_interval = drift_check_interval

        # Distribution buffers for PSI calculation
        self._baseline_scores: List[float] = []
        self._current_scores: collections.deque = collections.deque(maxlen=self.window_size)

        # Feature buffers for KS drift testing
        self._baseline_features: Dict[str, List[float]] = {k: [] for k in self.FEATURE_KEYS}
        self._current_features: Dict[str, collections.deque] = {
            k: collections.deque(maxlen=self.window_size) for k in self.FEATURE_KEYS
        }

        # Challenger model (online linear classifier)
        self._challenger_model = SGDClassifier(loss="log_loss", random_state=42)
        self._challenger_initialized = False
        self._challenger_classes = np.array([0, 1])

        # Prediction and evaluation tracking
        self._eval_count = 0
        self._last_psi: float = 0.0
        self._last_drift_status: str = "no_drift"
        self._last_feature_drifts: Dict[str, float] = {}
        self._recent_champion_preds: collections.deque = collections.deque(maxlen=100)
        self._recent_challenger_preds: collections.deque = collections.deque(maxlen=100)

        # Initialize baseline with representative distribution
        self._init_baseline()

    def subscribe(self) -> List[str]:
        """Return list of topics to subscribe to."""
        return [self.TOPIC_PREDICTIONS, self.TOPIC_METRICS]

    def _init_baseline(self) -> None:
        """Initialize baseline distribution for risk scores and features."""
        np.random.seed(42)
        # Synthetic baseline risk distribution (Beta distribution skewed towards lower risk)
        self._baseline_scores = list(np.random.beta(2, 5, size=self.window_size))

        # Baseline feature distributions
        for key in self.FEATURE_KEYS:
            if key == "credit_utilization":
                self._baseline_features[key] = list(np.random.beta(2, 4, size=self.window_size))
            elif key == "payment_delay":
                self._baseline_features[key] = list(np.random.exponential(scale=0.15, size=self.window_size))
            else:
                self._baseline_features[key] = list(np.random.uniform(0, 0.5, size=self.window_size))

        # Warm up challenger model
        X_init = np.random.rand(100, len(self.FEATURE_KEYS))
        y_init = (X_init[:, 0] * 0.4 + X_init[:, 1] * 0.4 + np.random.normal(0, 0.05, 100) > 0.4).astype(int)
        self._challenger_model.partial_fit(X_init, y_init, classes=self._challenger_classes)
        self._challenger_initialized = True

    def process_event(self, event: Event) -> None:
        """Process incoming prediction and metrics events for model risk governance."""
        try:
            if event.event_type == "payment.risk.predicted":
                self._handle_prediction_event(event)
            elif event.event_type in ("customer.metrics.updated", "metrics.calculated"):
                self._handle_metrics_event(event)
        except Exception as e:
            logger.error(f"[ModelGovernanceAgent] Error processing {event.event_type}: {e}", exc_info=True)

    def _handle_prediction_event(self, event: Event) -> None:
        """Handle predicted payment risk events."""
        payload = event.payload or {}
        risk_score = payload.get("predicted_risk_score") or payload.get("risk_score")
        if risk_score is None:
            return

        risk_score = float(risk_score)
        self._current_scores.append(risk_score)
        self._recent_champion_preds.append(risk_score)
        self._eval_count += 1

        # Extract features if present in payload
        features = payload.get("features") or payload.get("input_features")
        if features and isinstance(features, dict):
            feature_vec = [float(features.get(k, 0.0)) for k in self.FEATURE_KEYS]
            if self._challenger_initialized:
                try:
                    challenger_prob = float(self._challenger_model.predict_proba([feature_vec])[0][1])
                    self._recent_challenger_preds.append(challenger_prob)

                    # Pseudo-labeling / incremental update
                    target_label = int(risk_score > 0.5)
                    self._challenger_model.partial_fit([feature_vec], [target_label])
                except Exception as e:
                    logger.debug(f"[ModelGovernanceAgent] Challenger update skipped: {e}")

        # Periodic drift assessment
        if self._eval_count % self.drift_check_interval == 0:
            self._evaluate_model_governance(event.correlation_id)

    def _handle_metrics_event(self, event: Event) -> None:
        """Handle customer metrics events to record input feature distributions."""
        payload = event.payload or {}
        for key in self.FEATURE_KEYS:
            val = payload.get(key)
            if val is not None:
                try:
                    self._current_features[key].append(float(val))
                except (ValueError, TypeError):
                    pass

    def compute_psi(self, baseline: List[float], target: List[float], num_bins: int = 10) -> float:
        """
        Compute the Population Stability Index (PSI) between baseline and target distributions.

        PSI < 0.10: Stable (no significant shift)
        0.10 <= PSI < 0.25: Moderate shift / moderate drift
        PSI >= 0.25: Significant shift / action required
        """
        if not baseline or not target or len(baseline) < 10 or len(target) < 10:
            return 0.0

        b_arr = np.array(baseline, dtype=float)
        t_arr = np.array(target, dtype=float)

        # Quantile binning on baseline
        quantiles = np.linspace(0, 100, num_bins + 1)
        bin_edges = np.percentile(b_arr, quantiles)
        bin_edges = np.unique(bin_edges)
        if len(bin_edges) < 2:
            return 0.0

        bin_edges[0] -= 1e-5
        bin_edges[-1] += 1e-5

        b_counts, _ = np.histogram(b_arr, bins=bin_edges)
        t_counts, _ = np.histogram(t_arr, bins=bin_edges)

        # Proportions with Laplace smoothing
        eps = 1e-4
        b_prop = (b_counts + eps) / (len(b_arr) + eps * len(b_counts))
        t_prop = (t_counts + eps) / (len(t_arr) + eps * len(t_counts))

        # PSI formula: sum((Actual - Expected) * ln(Actual / Expected))
        psi_val = np.sum((t_prop - b_prop) * np.log(t_prop / b_prop))
        return float(np.clip(psi_val, 0.0, 10.0))

    def compute_ks_drift(self, baseline: List[float], target: List[float]) -> float:
        """
        Compute the Kolmogorov-Smirnov statistic (maximum empirical CDF distance) for feature drift.
        Returns KS statistic in [0, 1].
        """
        if not baseline or not target or len(baseline) < 5 or len(target) < 5:
            return 0.0

        b_sorted = np.sort(baseline)
        t_sorted = np.sort(target)
        n_b = len(b_sorted)
        n_t = len(t_sorted)

        data_all = np.concatenate([b_sorted, t_sorted])
        cdf_b = np.searchsorted(b_sorted, data_all, side="right") / n_b
        cdf_t = np.searchsorted(t_sorted, data_all, side="right") / n_t

        ks_stat = float(np.max(np.abs(cdf_b - cdf_t)))
        return ks_stat

    def _evaluate_model_governance(self, correlation_id: Optional[str] = None) -> None:
        """Run full statistical drift evaluation, challenger comparison, and report publishing."""
        if len(self._current_scores) < 20:
            return

        # 1. Compute Score PSI
        psi = self.compute_psi(self._baseline_scores, list(self._current_scores))
        self._last_psi = psi

        if psi < 0.10:
            drift_status = "no_drift"
        elif psi < 0.25:
            drift_status = "moderate_drift"
        else:
            drift_status = "significant_drift"
        self._last_drift_status = drift_status

        # 2. Compute Feature Drift (KS Statistic)
        feature_drifts = {}
        for key in self.FEATURE_KEYS:
            cur_list = list(self._current_features[key])
            base_list = self._baseline_features[key]
            if len(cur_list) >= 10:
                ks = self.compute_ks_drift(base_list, cur_list)
                feature_drifts[key] = round(ks, 4)
        self._last_feature_drifts = feature_drifts

        # 3. Champion vs. Challenger Metrics
        champ_mean = float(np.mean(self._recent_champion_preds)) if self._recent_champion_preds else 0.0
        chall_mean = float(np.mean(self._recent_challenger_preds)) if self._recent_challenger_preds else 0.0
        champ_std = float(np.std(self._recent_champion_preds)) if self._recent_champion_preds else 0.0
        chall_std = float(np.std(self._recent_challenger_preds)) if self._recent_challenger_preds else 0.0

        # Correlation between Champion and Challenger
        divergence = abs(champ_mean - chall_mean)

        # 4. Emit drift event if threshold exceeded
        drift_detected = psi >= self.psi_threshold
        if drift_detected:
            drift_payload = ModelDriftDetectedPayload(
                model_name="PaymentPredictionAgent.RandomForest",
                psi_score=round(psi, 4),
                drift_status=drift_status,
                feature_drifts=feature_drifts,
                sample_count=len(self._current_scores),
                timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            self.publish_event(
                topic=self.TOPIC_SYSTEM,
                event_type="model.governance.drift_detected",
                entity_id="PaymentPredictionAgent",
                payload=drift_payload.model_dump(),
                correlation_id=correlation_id,
            )
            logger.warning(
                f"[ModelGovernanceAgent] Model drift detected! PSI={psi:.4f} ({drift_status})"
            )

        # 5. Generate and publish governance audit report
        report_payload = ModelGovernanceReportPayload(
            report_id=f"gov_rep_{uuid.uuid4().hex[:8]}",
            model_name="PaymentPredictionAgent.RandomForest",
            challenger_name="ModelGovernanceAgent.OnlineSGD",
            champion_metrics={
                "mean_prediction": round(champ_mean, 4),
                "std_prediction": round(champ_std, 4),
                "sample_count": float(len(self._recent_champion_preds)),
            },
            challenger_metrics={
                "mean_prediction": round(chall_mean, 4),
                "std_prediction": round(chall_std, 4),
                "prediction_divergence": round(divergence, 4),
            },
            psi_score=round(psi, 4),
            drift_detected=drift_detected,
            audit_summary=(
                f"Evaluation window N={len(self._current_scores)}. "
                f"PSI={psi:.4f} ({drift_status}). "
                f"Champion vs Challenger divergence={divergence:.4f}. "
                f"Status: {'Action Required' if drift_detected else 'Compliant'}."
            ),
            generated_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )

        self.publish_event(
            topic=self.TOPIC_SYSTEM,
            event_type="model.governance.report",
            entity_id="PaymentPredictionAgent",
            payload=report_payload.model_dump(),
            correlation_id=correlation_id,
        )
        logger.info(f"[ModelGovernanceAgent] Governance report generated. PSI={psi:.4f}")

    def get_latest_metrics(self) -> Dict[str, Any]:
        """Return the latest governance and drift metrics for health reporting."""
        return {
            "psi_score": self._last_psi,
            "drift_status": self._last_drift_status,
            "feature_drifts": self._last_feature_drifts,
            "eval_count": self._eval_count,
            "window_size": len(self._current_scores),
        }
