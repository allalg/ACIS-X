"""Regression lock for honest baseline comparison numbers used in the paper."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RESULT = ROOT / "scratch" / "baseline_comparison_results.json"


@pytest.mark.intelligence
def test_baseline_comparison_artifact_exists_and_is_honest():
    """
    Requires scratch/baseline_comparison_experiment.py to have been run.
    Locks the paper's quantitative story: enrichment helps; ACIS beats naive;
    supervised models (trained on labels) are stronger than the untrained refiner.
    """
    if not RESULT.exists():
        pytest.skip("Run: PYTHONPATH=. python scratch/baseline_comparison_experiment.py")

    data = json.loads(RESULT.read_text(encoding="utf-8"))
    by_name = {r["name"]: r for r in data["results"]}

    assert by_name["LogisticRegression_enriched"]["f1"] > by_name["LogisticRegression_basic"]["f1"]
    assert by_name["RandomForest_enriched"]["f1"] > by_name["RandomForest_basic"]["f1"]
    assert by_name["GradientBoosting_enriched"]["f1"] > by_name["GradientBoosting_basic"]["f1"]

    assert by_name["ACIS_X_RiskScoring_enriched_context"]["f1"] > by_name["NaiveUtilisationThreshold"]["f1"]

    # Trained learners beat untrained streaming refiner on this generator (expected).
    assert by_name["LogisticRegression_enriched"]["f1"] > by_name["ACIS_X_RiskScoring_enriched_context"]["f1"]

    ci = data["ci_intelligence_n100"]
    assert ci["f1_acis"] >= ci["f1_naive"]
    assert ci["rho_acis"] > ci["rho_ar"]
    assert ci["rho_acis"] > ci["rho_behavior"]
    assert ci["rho_acis"] > ci["rho_overdue"]
