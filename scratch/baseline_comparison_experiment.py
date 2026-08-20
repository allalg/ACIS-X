"""Honest baseline comparison on synthetic B2B risk data (no fabricated scores)."""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.model_selection import train_test_split


def make_population(n: int = 500, seed: int = 42):
    rng = np.random.RandomState(seed)
    rows = []
    for i in range(n):
        credit_limit = 200_000.0 + rng.rand() * 800_000.0
        utilisation = rng.rand()
        total_outstanding = utilisation * credit_limit
        avg_delay = rng.rand() * 90.0
        on_time_ratio = max(0.0, 1.0 - rng.rand() * 1.2)
        overdue_count = int(rng.rand() * 8)
        financial_risk = rng.rand()
        litigation_risk = rng.rand()
        gt = (
            0.30 * utilisation
            + 0.20 * (avg_delay / 90.0)
            + 0.20 * (1.0 - on_time_ratio)
            + 0.15 * (overdue_count / 7.0)
            + 0.10 * financial_risk
            + 0.05 * litigation_risk
        )
        gt = min(1.0, gt)
        rows.append(
            {
                "customer_id": f"c{i:04d}",
                "credit_limit": credit_limit,
                "total_outstanding": total_outstanding,
                "utilisation": utilisation,
                "avg_delay": avg_delay,
                "on_time_ratio": on_time_ratio,
                "overdue_count": overdue_count,
                "financial_risk": financial_risk,
                "litigation_risk": litigation_risk,
                "ground_truth_risk": gt,
                "will_default": gt > 0.50,
            }
        )
    return rows


def f1_at(y_true, scores, thr: float = 0.5) -> float:
    y_pred = (np.asarray(scores) >= thr).astype(int)
    return float(f1_score(y_true, y_pred))


def train_eval(name, model, X, y, tr, te):
    model.fit(X[tr], y[tr])
    scores = model.predict_proba(X[te])[:, 1]
    f1 = f1_at(y[te], scores, 0.5)
    auc = float(roc_auc_score(y[te], scores))
    return {
        "name": name,
        "f1": round(f1, 4),
        "auc": round(auc, 4),
        "n_train": int(len(tr)),
        "n_test": int(len(te)),
    }


def main():
    customers = make_population(500, seed=42)
    y = np.array([int(c["will_default"]) for c in customers])
    X_basic = np.array(
        [
            [
                c["utilisation"],
                c["avg_delay"] / 90.0,
                1.0 - c["on_time_ratio"],
                c["overdue_count"] / 7.0,
            ]
            for c in customers
        ]
    )
    X_enriched = np.array(
        [
            [
                c["utilisation"],
                c["avg_delay"] / 90.0,
                1.0 - c["on_time_ratio"],
                c["overdue_count"] / 7.0,
                c["financial_risk"],
                c["litigation_risk"],
            ]
            for c in customers
        ]
    )
    idx = np.arange(len(customers))
    tr, te = train_test_split(idx, test_size=0.3, random_state=42, stratify=y)

    results = [
        train_eval(
            "LogisticRegression_basic",
            LogisticRegression(max_iter=2000, random_state=42),
            X_basic,
            y,
            tr,
            te,
        ),
        train_eval(
            "LogisticRegression_enriched",
            LogisticRegression(max_iter=2000, random_state=42),
            X_enriched,
            y,
            tr,
            te,
        ),
        train_eval(
            "RandomForest_basic",
            RandomForestClassifier(n_estimators=50, random_state=42),
            X_basic,
            y,
            tr,
            te,
        ),
        train_eval(
            "RandomForest_enriched",
            RandomForestClassifier(n_estimators=50, random_state=42),
            X_enriched,
            y,
            tr,
            te,
        ),
        train_eval(
            "GradientBoosting_basic",
            GradientBoostingClassifier(random_state=42),
            X_basic,
            y,
            tr,
            te,
        ),
        train_eval(
            "GradientBoosting_enriched",
            GradientBoostingClassifier(random_state=42),
            X_enriched,
            y,
            tr,
            te,
        ),
    ]

    naive_scores = [
        0.85 if customers[i]["utilisation"] > 0.70 else 0.15 for i in te
    ]
    results.append(
        {
            "name": "NaiveUtilisationThreshold",
            "f1": round(f1_at(y[te], naive_scores, 0.5), 4),
            "auc": None,
            "n_train": 0,
            "n_test": int(len(te)),
        }
    )

    from agents.risk.risk_scoring_agent import RiskScoringAgent

    kafka = MagicMock()
    kafka.publish.return_value = True
    agent = RiskScoringAgent(kafka_client=kafka)
    acis_scores = []
    for i in te:
        c = customers[i]

        def _handler(query_type, params=None, _c=c, **kw):
            if query_type == "get_customer_metrics":
                return {
                    "customer_id": _c["customer_id"],
                    "total_outstanding": _c["total_outstanding"],
                    "avg_delay": _c["avg_delay"],
                    "on_time_ratio": _c["on_time_ratio"],
                    "overdue_count": _c["overdue_count"],
                    "credit_limit": _c["credit_limit"],
                }
            if query_type == "get_risk_velocity":
                return {"velocity": 0.0, "trend": "stable", "volatility": 0.02}
            return None

        base = min(1.0, c["utilisation"])
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            agent._customer_risk_context[c["customer_id"]] = {
                "data": {
                    "aggregated_risk": 0.6 * c["financial_risk"]
                    + 0.4 * c["litigation_risk"],
                    "financial_risk": c["financial_risk"],
                    "litigation_risk": c["litigation_risk"],
                    "external_risk": c["financial_risk"],
                    "severity": "medium",
                    "trend": "stable",
                },
                "updated_at": time.time(),
            }
            refined = agent._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base,
                confidence=0.80,
                reasons=[],
            )
        acis_scores.append(refined)

    results.append(
        {
            "name": "ACIS_X_RiskScoring_enriched_context",
            "f1": round(f1_at(y[te], acis_scores, 0.5), 4),
            "auc": round(float(roc_auc_score(y[te], acis_scores)), 4),
            "n_train": 0,
            "n_test": int(len(te)),
        }
    )

    # Also reproduce CI n=100 numbers from intelligence comparison helpers
    from tests.suite.test_intelligence_comparison import (
        _acis_predict,
        _f1,
        _make_population,
        _naive_threshold_predict,
        _single_signal_ar,
        _single_signal_behavior,
        _single_signal_overdue,
        _spearman,
    )

    pop100 = _make_population(n=100, seed=42)
    gt_bool = [c["will_default"] for c in pop100]
    gt_risk = [c["ground_truth_risk"] for c in pop100]
    acis100 = _acis_predict(pop100)
    naive100 = _naive_threshold_predict(pop100)
    ci_block = {
        "n": 100,
        "f1_acis": round(_f1(gt_bool, acis100, 0.50), 4),
        "f1_naive": round(_f1(gt_bool, naive100, 0.50), 4),
        "rho_acis": round(_spearman(gt_risk, acis100), 4),
        "rho_ar": round(_spearman(gt_risk, _single_signal_ar(pop100)), 4),
        "rho_overdue": round(_spearman(gt_risk, _single_signal_overdue(pop100)), 4),
        "rho_behavior": round(_spearman(gt_risk, _single_signal_behavior(pop100)), 4),
    }

    out = {
        "dataset": "synthetic_n500_seed42_train70_test30",
        "label_rule": "will_default if gt_risk>0.50; gt includes behavioural+external terms",
        "note": "XGBoost not installed; GradientBoostingClassifier used as boosted-tree baseline",
        "results": results,
        "ci_intelligence_n100": ci_block,
    }
    out_path = Path("scratch/baseline_comparison_results.json")
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
