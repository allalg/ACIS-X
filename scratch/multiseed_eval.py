"""
Multi-seed evaluation script for ACIS-X.
Evaluates:
1. Architectural Ablation across seeds [42, 123, 456, 789, 1024]
2. Weight Sensitivity across seeds [42, 123, 456, 789, 1024]
3. Predictive Benchmarks across seeds [42, 123, 456, 789, 1024]
"""
import sys
import os
sys.path.insert(0, os.path.abspath("."))
import numpy as np
import time
from unittest.mock import MagicMock, patch
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.model_selection import train_test_split

SEEDS = [42, 123, 456, 789, 1024]

def make_population(n: int = 200, seed: int = 42):
    rng = np.random.RandomState(seed)
    customers = []
    for i in range(n):
        credit_limit = 200_000.0 + rng.rand() * 800_000.0
        utilisation = rng.rand()
        total_outstanding = utilisation * credit_limit
        avg_delay = rng.rand() * 90.0
        on_time_ratio = max(0.0, 1.0 - rng.rand() * 1.2)
        overdue_count = int(rng.rand() * 8)
        financial_risk = 0.1 + rng.rand() * 0.7
        litigation_risk = 0.05 + rng.rand() * 0.4

        gt_risk = (
            0.30 * utilisation
            + 0.20 * (avg_delay / 90.0)
            + 0.20 * (1.0 - on_time_ratio)
            + 0.10 * (overdue_count / 7.0)
            + 0.10 * financial_risk
            + 0.10 * litigation_risk
        )
        gt_risk = min(1.0, gt_risk)
        will_default = gt_risk > 0.50

        customers.append({
            "customer_id": f"cust_{i:04d}",
            "credit_limit": credit_limit,
            "total_outstanding": total_outstanding,
            "utilisation": utilisation,
            "avg_delay": avg_delay,
            "on_time_ratio": on_time_ratio,
            "overdue_count": overdue_count,
            "financial_risk": financial_risk,
            "litigation_risk": litigation_risk,
            "ground_truth_risk": gt_risk,
            "will_default": will_default,
        })
    return customers

def spearman(a, b):
    def _rank(lst):
        indexed = sorted(enumerate(lst), key=lambda x: x[1])
        ranks = [0.0] * len(lst)
        for rank, (idx, _) in enumerate(indexed):
            ranks[idx] = float(rank)
        return ranks
    n = len(a)
    if n < 2:
        return 0.0
    ra, rb = _rank(a), _rank(b)
    d_sq = sum((ra[i] - rb[i]) ** 2 for i in range(n))
    denom = n * (n ** 2 - 1)
    return 0.0 if denom == 0 else 1.0 - (6 * d_sq) / denom

def f1_at(y_true, scores, thr: float = 0.5) -> float:
    y_pred = (np.asarray(scores) >= thr).astype(int)
    return float(f1_score(y_true, y_pred))

# --- 1. Ablation Study ---
def run_ablation_for_seed(seed):
    from agents.risk.risk_scoring_agent import RiskScoringAgent
    customers = make_population(n=200, seed=seed)
    gt_risk = [c["ground_truth_risk"] for c in customers]

    kafka = MagicMock()
    kafka.publish.return_value = True
    agent = RiskScoringAgent(kafka_client=kafka)

    # Full ACIS-X
    scores_full = []
    for c in customers:
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

        agent._customer_risk_context[c["customer_id"]] = {
            "data": {
                "aggregated_risk": 0.6 * c["financial_risk"] + 0.4 * c["litigation_risk"],
                "financial_risk": c["financial_risk"],
                "litigation_risk": c["litigation_risk"],
                "external_risk": c["financial_risk"],
                "severity": None,
                "trend": "stable",
            },
            "updated_at": time.time(),
        }
        base_risk = min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            refined = agent._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base_risk,
                confidence=0.80,
                reasons=[],
            )
        scores_full.append(refined)

    # No enrichment
    scores_no_enrich = []
    agent_no_enrich = RiskScoringAgent(kafka_client=kafka)
    for c in customers:
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

        base_risk = min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            refined = agent_no_enrich._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base_risk,
                confidence=0.80,
                reasons=[],
            )
        scores_no_enrich.append(refined)

    # No refinement
    scores_no_refine = [
        min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        for c in customers
    ]

    rho_full = spearman(gt_risk, scores_full)
    rho_no_enrich = spearman(gt_risk, scores_no_enrich)
    rho_no_refine = spearman(gt_risk, scores_no_refine)

    return {
        "full": rho_full,
        "no_enrich": rho_no_enrich,
        "no_refine": rho_no_refine,
    }

# --- 2. Weight Sensitivity ---
def run_weight_sensitivity_for_seed(seed):
    from agents.intelligence.aggregator_agent import AggregatorAgent
    from schemas.event_schema import Event
    from datetime import datetime, timezone

    rng = np.random.RandomState(seed)
    companies = [
        {
            "customer_id": f"cust_{i:03d}",
            "name": f"Corp {i:03d}",
            "ground_truth_risk": 0.1 + (i / 50) * 0.8 + (rng.rand() - 0.5) * 0.05,
            "litigation_risk": 0.15 + (i % 8) * 0.08 + (rng.rand() - 0.5) * 0.02,
        }
        for i in range(50)
    ]
    gt_risk = [c["ground_truth_risk"] for c in companies]

    weights = [(0.50, 0.50), (0.60, 0.40), (0.70, 0.30)]
    res = {}
    for fin_w, lit_w in weights:
        kafka = MagicMock()
        published = []
        kafka.publish.side_effect = lambda t, e, **kw: published.append(e) or True
        agent = AggregatorAgent(kafka_client=kafka, financial_weight=fin_w, litigation_weight=lit_w)
        per_cust = {}
        for c in companies:
            cid = c["customer_id"]
            fin_event = Event(
                event_id=f"evt_fin_{cid}",
                event_type="external.data.enriched",
                event_source="ExternalDataAgent",
                event_time=datetime.now(timezone.utc).replace(tzinfo=None),
                entity_id=cid,
                correlation_id=f"corr_fin_{cid}",
                schema_version="1.1",
                payload={"customer_id": cid, "company_name": c["name"], "external_risk": c["ground_truth_risk"] * 0.9 + 0.05, "confidence": 0.85},
                metadata={},
            )
            agent.process_event(fin_event)
            lit_event = Event(
                event_id=f"evt_lit_{cid}",
                event_type="external.litigation.updated",
                event_source="ExternalScrapingAgent",
                event_time=datetime.now(timezone.utc).replace(tzinfo=None),
                entity_id=cid,
                correlation_id=f"corr_lit_{cid}",
                schema_version="1.1",
                payload={"customer_id": cid, "company_name": c["name"], "litigation_risk": c["litigation_risk"], "confidence": 0.75},
                metadata={},
            )
            agent.process_event(lit_event)
        for e in published:
            p = e.get("payload", {}) if isinstance(e, dict) else getattr(e, "payload", {})
            cr = p.get("combined_risk")
            cid = p.get("customer_id")
            if cr is not None and cid is not None:
                per_cust[cid] = float(cr)
        scores = [per_cust[c["customer_id"]] for c in companies if c["customer_id"] in per_cust]
        n = min(len(gt_risk), len(scores))
        rho = spearman(gt_risk[:n], scores[:n])
        res[f"{int(fin_w*100)}/{int(lit_w*100)}"] = rho
    return res

# --- 3. Predictive Benchmark ---
def run_benchmark_for_seed(seed):
    from agents.risk.risk_scoring_agent import RiskScoringAgent

    customers = make_population(500, seed=seed)
    y = np.array([int(c["will_default"]) for c in customers])
    X_basic = np.array([
        [c["utilisation"], c["avg_delay"] / 90.0, 1.0 - c["on_time_ratio"], c["overdue_count"] / 7.0]
        for c in customers
    ])
    X_enriched = np.array([
        [c["utilisation"], c["avg_delay"] / 90.0, 1.0 - c["on_time_ratio"], c["overdue_count"] / 7.0, c["financial_risk"], c["litigation_risk"]]
        for c in customers
    ])
    tr, te = train_test_split(np.arange(len(customers)), test_size=0.30, random_state=seed, stratify=y)

    def _eval(clf, X_tr, y_tr, X_te, y_te):
        clf.fit(X_tr, y_tr)
        probs = clf.predict_proba(X_te)[:, 1]
        return f1_at(y_te, probs, 0.5), float(roc_auc_score(y_te, probs))

    lr_basic_f1, lr_basic_auc = _eval(LogisticRegression(max_iter=1000, random_state=seed), X_basic[tr], y[tr], X_basic[te], y[te])
    lr_enr_f1, lr_enr_auc = _eval(LogisticRegression(max_iter=1000, random_state=seed), X_enriched[tr], y[tr], X_enriched[te], y[te])
    rf_basic_f1, rf_basic_auc = _eval(RandomForestClassifier(n_estimators=50, random_state=seed), X_basic[tr], y[tr], X_basic[te], y[te])
    rf_enr_f1, rf_enr_auc = _eval(RandomForestClassifier(n_estimators=50, random_state=seed), X_enriched[tr], y[tr], X_enriched[te], y[te])
    gb_basic_f1, gb_basic_auc = _eval(GradientBoostingClassifier(n_estimators=50, random_state=seed), X_basic[tr], y[tr], X_basic[te], y[te])
    gb_enr_f1, gb_enr_auc = _eval(GradientBoostingClassifier(n_estimators=50, random_state=seed), X_enriched[tr], y[tr], X_enriched[te], y[te])

    # Naive
    naive_scores = [customers[i]["utilisation"] for i in te]
    naive_f1 = f1_at(y[te], naive_scores, 0.5)

    # ACIS-X label-free
    kafka = MagicMock()
    kafka.publish.return_value = True
    agent = RiskScoringAgent(kafka_client=kafka)
    acis_scores = []
    for idx in te:
        c = customers[idx]
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
        agent._customer_risk_context[c["customer_id"]] = {
            "data": {
                "aggregated_risk": 0.6 * c["financial_risk"] + 0.4 * c["litigation_risk"],
                "financial_risk": c["financial_risk"],
                "litigation_risk": c["litigation_risk"],
                "external_risk": c["financial_risk"],
                "severity": None,
                "trend": "stable",
            },
            "updated_at": time.time(),
        }
        base_risk = min(1.0, c["total_outstanding"] / max(c["credit_limit"], 1.0))
        with patch("utils.query_client.QueryClient.query", side_effect=_handler):
            refined = agent._refine_risk_with_context(
                customer_id=c["customer_id"],
                invoice_id=f"inv_{c['customer_id']}",
                base_risk=base_risk,
                confidence=0.80,
                reasons=[],
            )
        acis_scores.append(refined)

    acis_f1 = f1_at(y[te], acis_scores, 0.5)
    acis_auc = float(roc_auc_score(y[te], acis_scores))

    return {
        "naive_f1": naive_f1,
        "acis_f1": acis_f1,
        "acis_auc": acis_auc,
        "lr_basic_f1": lr_basic_f1, "lr_basic_auc": lr_basic_auc,
        "lr_enr_f1": lr_enr_f1, "lr_enr_auc": lr_enr_auc,
        "rf_basic_f1": rf_basic_f1, "rf_basic_auc": rf_basic_auc,
        "rf_enr_f1": rf_enr_f1, "rf_enr_auc": rf_enr_auc,
        "gb_basic_f1": gb_basic_f1, "gb_basic_auc": gb_basic_auc,
        "gb_enr_f1": gb_enr_f1, "gb_enr_auc": gb_enr_auc,
    }

if __name__ == "__main__":
    print("Running Multi-Seed Evaluation (5 seeds: [42, 123, 456, 789, 1024])...")
    
    # 1. Ablation
    ablation_results = [run_ablation_for_seed(s) for s in SEEDS]
    print("\n--- 1. Ablation Results (Spearman rho across 5 seeds) ---")
    for key in ["full", "no_enrich", "no_refine"]:
        vals = [r[key] for r in ablation_results]
        print(f"  {key}: {np.mean(vals):.3f} +/- {np.std(vals):.3f}")

    # 2. Weight Sensitivity
    sens_results = [run_weight_sensitivity_for_seed(s) for s in SEEDS]
    print("\n--- 2. Weight Sensitivity (Spearman rho across 5 seeds) ---")
    for key in ["50/50", "60/40", "70/30"]:
        vals = [r[key] for r in sens_results]
        print(f"  {key}: {np.mean(vals):.3f} +/- {np.std(vals):.3f}")

    # 3. Predictive Benchmark
    bench_results = [run_benchmark_for_seed(s) for s in SEEDS]
    print("\n--- 3. Predictive Benchmark (F1 / AUC across 5 seeds) ---")
    print(f"  Naive Threshold: F1={np.mean([r['naive_f1'] for r in bench_results]):.3f} +/- {np.std([r['naive_f1'] for r in bench_results]):.3f}")
    print(f"  ACIS-X (label-free): F1={np.mean([r['acis_f1'] for r in bench_results]):.3f} +/- {np.std([r['acis_f1'] for r in bench_results]):.3f}, AUC={np.mean([r['acis_auc'] for r in bench_results]):.3f} +/- {np.std([r['acis_auc'] for r in bench_results]):.3f}")
    print(f"  LR (basic/enr): F1={np.mean([r['lr_basic_f1'] for r in bench_results]):.3f} +/- {np.std([r['lr_basic_f1'] for r in bench_results]):.3f} / {np.mean([r['lr_enr_f1'] for r in bench_results]):.3f} +/- {np.std([r['lr_enr_f1'] for r in bench_results]):.3f}, AUC={np.mean([r['lr_basic_auc'] for r in bench_results]):.3f} +/- {np.std([r['lr_basic_auc'] for r in bench_results]):.3f} / {np.mean([r['lr_enr_auc'] for r in bench_results]):.3f} +/- {np.std([r['lr_enr_auc'] for r in bench_results]):.3f}")
    print(f"  RF (basic/enr): F1={np.mean([r['rf_basic_f1'] for r in bench_results]):.3f} +/- {np.std([r['rf_basic_f1'] for r in bench_results]):.3f} / {np.mean([r['rf_enr_f1'] for r in bench_results]):.3f} +/- {np.std([r['rf_enr_f1'] for r in bench_results]):.3f}, AUC={np.mean([r['rf_basic_auc'] for r in bench_results]):.3f} +/- {np.std([r['rf_basic_auc'] for r in bench_results]):.3f} / {np.mean([r['rf_enr_auc'] for r in bench_results]):.3f} +/- {np.std([r['rf_enr_auc'] for r in bench_results]):.3f}")
    print(f"  GB (basic/enr): F1={np.mean([r['gb_basic_f1'] for r in bench_results]):.3f} +/- {np.std([r['gb_basic_f1'] for r in bench_results]):.3f} / {np.mean([r['gb_enr_f1'] for r in bench_results]):.3f} +/- {np.std([r['gb_enr_f1'] for r in bench_results]):.3f}, AUC={np.mean([r['gb_basic_auc'] for r in bench_results]):.3f} +/- {np.std([r['gb_basic_auc'] for r in bench_results]):.3f} / {np.mean([r['gb_enr_auc'] for r in bench_results]):.3f} +/- {np.std([r['gb_enr_auc'] for r in bench_results]):.3f}")
