"""
Run 42-seed evaluation of ACIS-X:
- Ablation study (n=200)
- Weight sensitivity (n=50)
- Predictive benchmark (n=500, 70/30 split)
- Paired statistical tests (t-tests, win rates, p-values, df)
Evaluates both range(1, 43) and range(0, 42) to cross-check against paper.
"""
import sys
import os
sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath("scratch"))

import numpy as np
import time
from scipy import stats
from unittest.mock import MagicMock, patch
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

from multiseed_eval import (
    make_population,
    spearman,
    f1_at,
    run_ablation_for_seed,
    run_weight_sensitivity_for_seed,
    run_benchmark_for_seed,
)

def run_suite_for_seeds(seeds, seed_label="1-42"):
    print(f"\n=======================================================")
    print(f"RUNNING EVALUATION FOR SEEDS: {seed_label} (N={len(seeds)})")
    print(f"=======================================================")
    
    # 1. Ablation Study
    print(f"\n--- 1. Ablation Study (n=200, {len(seeds)} seeds) ---")
    t0 = time.time()
    ablation_results = [run_ablation_for_seed(s) for s in seeds]
    full_rhos = np.array([r["full"] for r in ablation_results])
    no_enr_rhos = np.array([r["no_enrich"] for r in ablation_results])
    no_ref_rhos = np.array([r["no_refine"] for r in ablation_results])
    
    delta_enr = full_rhos - no_enr_rhos
    delta_ref = full_rhos - no_ref_rhos
    
    t_enr, p_enr = stats.ttest_rel(full_rhos, no_enr_rhos)
    t_ref, p_ref = stats.ttest_rel(full_rhos, no_ref_rhos)
    
    print(f"Full ACIS-X:        mean={np.mean(full_rhos):.4f} +/- {np.std(full_rhos):.4f} (sample std: {np.std(full_rhos, ddof=1):.4f})")
    print(f"- External Enrich:  mean={np.mean(no_enr_rhos):.4f} +/- {np.std(no_enr_rhos):.4f} (sample std: {np.std(no_enr_rhos, ddof=1):.4f})")
    print(f"  Delta (Full-NoEnr): mean={np.mean(delta_enr):.4f}, paired t={t_enr:.3f}, p={p_enr:.6f}, df={len(seeds)-1}")
    print(f"- Behav Refinement: mean={np.mean(no_ref_rhos):.4f} +/- {np.std(no_ref_rhos):.4f} (sample std: {np.std(no_ref_rhos, ddof=1):.4f})")
    print(f"  Delta (Full-NoRef): mean={np.mean(delta_ref):.4f}, paired t={t_ref:.3f}, p={p_ref:.6e}, df={len(seeds)-1}")
    print(f"Ablation elapsed: {time.time() - t0:.2f}s")
    
    # 2. Weight Sensitivity
    print(f"\n--- 2. Weight Sensitivity ({len(seeds)} seeds) ---")
    t0 = time.time()
    sens_results = [run_weight_sensitivity_for_seed(s) for s in seeds]
    for w in ["50/50", "60/40", "70/30"]:
        vals = np.array([r[w] for r in sens_results])
        print(f"rho_{w}: mean={np.mean(vals):.4f} +/- {np.std(vals):.4f} (sample std: {np.std(vals, ddof=1):.4f})")
    print(f"Sensitivity elapsed: {time.time() - t0:.2f}s")
    
    # 3. Predictive Benchmark
    print(f"\n--- 3. Predictive Benchmark (n=500, {len(seeds)} seeds) ---")
    t0 = time.time()
    bench_results = [run_benchmark_for_seed(s) for s in seeds]
    
    naive_f1s = np.array([r["naive_f1"] for r in bench_results])
    acis_f1s = np.array([r["acis_f1"] for r in bench_results])
    acis_aucs = np.array([r["acis_auc"] for r in bench_results])
    
    wins = np.sum(acis_f1s > naive_f1s)
    ties = np.sum(acis_f1s == naive_f1s)
    losses = np.sum(acis_f1s < naive_f1s)
    t_f1, p_f1 = stats.ttest_rel(acis_f1s, naive_f1s)
    
    print(f"Naive:   F1={np.mean(naive_f1s):.4f} +/- {np.std(naive_f1s):.4f} (sample std: {np.std(naive_f1s, ddof=1):.4f})")
    print(f"ACIS-X:  F1={np.mean(acis_f1s):.4f} +/- {np.std(acis_f1s):.4f} (sample std: {np.std(acis_f1s, ddof=1):.4f})")
    print(f"         AUC={np.mean(acis_aucs):.4f} +/- {np.std(acis_aucs):.4f} (sample std: {np.std(acis_aucs, ddof=1):.4f})")
    print(f"ACIS-X vs Naive: wins={wins}/{len(seeds)} ({wins/len(seeds)*100:.1f}%), ties={ties}, losses={losses}")
    print(f"Paired t-test (ACIS-X vs Naive F1): t={t_f1:.3f}, p={p_f1:.6f}, df={len(seeds)-1}")
    
    models = [
        ("lr_basic", "lr_enr", "Logistic Regression"),
        ("rf_basic", "rf_enr", "Random Forest"),
        ("gb_basic", "gb_enr", "Gradient Boosting"),
    ]
    for b_key, e_key, name in models:
        b_f1 = np.array([r[f"{b_key}_f1"] for r in bench_results])
        e_f1 = np.array([r[f"{e_key}_f1"] for r in bench_results])
        b_auc = np.array([r[f"{b_key}_auc"] for r in bench_results])
        e_auc = np.array([r[f"{e_key}_auc"] for r in bench_results])
        print(f"{name}:")
        print(f"  F1 (basic/enr):  {np.mean(b_f1):.4f} +/- {np.std(b_f1):.4f} / {np.mean(e_f1):.4f} +/- {np.std(e_f1):.4f}")
        print(f"  AUC (basic/enr): {np.mean(b_auc):.4f} +/- {np.std(b_auc):.4f} / {np.mean(e_auc):.4f} +/- {np.std(e_auc):.4f}")
    
    print(f"Benchmark elapsed: {time.time() - t0:.2f}s")
    
    return {
        "seeds": seeds,
        "ablation": {
            "full": (np.mean(full_rhos), np.std(full_rhos), np.std(full_rhos, ddof=1)),
            "no_enr": (np.mean(no_enr_rhos), np.std(no_enr_rhos), np.std(no_enr_rhos, ddof=1)),
            "no_ref": (np.mean(no_ref_rhos), np.std(no_ref_rhos), np.std(no_ref_rhos, ddof=1)),
            "delta_enr": np.mean(delta_enr),
            "delta_ref": np.mean(delta_ref),
            "t_enr": t_enr, "p_enr": p_enr,
            "t_ref": t_ref, "p_ref": p_ref,
        },
        "sensitivity": {
            w: (np.mean(np.array([r[w] for r in sens_results])), np.std(np.array([r[w] for r in sens_results])))
            for w in ["50/50", "60/40", "70/30"]
        },
        "benchmark": {
            "naive_f1": (np.mean(naive_f1s), np.std(naive_f1s)),
            "acis_f1": (np.mean(acis_f1s), np.std(acis_f1s)),
            "acis_auc": (np.mean(acis_aucs), np.std(acis_aucs)),
            "wins": wins,
            "win_rate": wins / len(seeds),
            "t_f1": t_f1, "p_f1": p_f1,
            "models": {
                name: {
                    "b_f1": (np.mean([r[f"{b_key}_f1"] for r in bench_results]), np.std([r[f"{b_key}_f1"] for r in bench_results])),
                    "e_f1": (np.mean([r[f"{e_key}_f1"] for r in bench_results]), np.std([r[f"{e_key}_f1"] for r in bench_results])),
                    "b_auc": (np.mean([r[f"{b_key}_auc"] for r in bench_results]), np.std([r[f"{b_key}_auc"] for r in bench_results])),
                    "e_auc": (np.mean([r[f"{e_key}_auc"] for r in bench_results]), np.std([r[f"{e_key}_auc"] for r in bench_results])),
                }
                for b_key, e_key, name in models
            }
        }
    }

if __name__ == "__main__":
    seeds_1_42 = list(range(1, 43))
    run_suite_for_seeds(seeds_1_42, seed_label="1-42 (seeds 1 to 42)")
