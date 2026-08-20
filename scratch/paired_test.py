import sys
import os
sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath("."))
import numpy as np
from scipy import stats
from multiseed_eval import SEEDS, run_benchmark_for_seed, run_ablation_for_seed

print('--- Predictive Benchmark Paired Stats ---')
bench = [run_benchmark_for_seed(s) for s in SEEDS]
naive_f1 = [b['naive_f1'] for b in bench]
acis_f1 = [b['acis_f1'] for b in bench]
print('Naive F1:', [round(x, 4) for x in naive_f1])
print('ACIS F1: ', [round(x, 4) for x in acis_f1])
diffs = np.array(acis_f1) - np.array(naive_f1)
print('Diffs (ACIS - Naive):', [round(x, 4) for x in diffs])
print('ACIS wins:', sum(d > 0 for d in diffs), 'out of', len(diffs))
t_stat, p_val = stats.ttest_rel(acis_f1, naive_f1)
print(f'Paired t-test: t={t_stat:.3f}, p={p_val:.4f}')

print('\n--- Ablation Paired Stats ---')
ablation = [run_ablation_for_seed(s) for s in SEEDS]
full_rho = [a['full'] for a in ablation]
no_enr_rho = [a['no_enrich'] for a in ablation]
no_ref_rho = [a['no_refine'] for a in ablation]
print('Full rho:     ', [round(x, 4) for x in full_rho])
print('No Enrich rho:', [round(x, 4) for x in no_enr_rho])
print('No Refine rho:', [round(x, 4) for x in no_ref_rho])
diff_enr = np.array(full_rho) - np.array(no_enr_rho)
diff_ref = np.array(full_rho) - np.array(no_ref_rho)
print('Diffs (Full - NoEnrich):', [round(x, 4) for x in diff_enr])
print('Diffs (Full - NoRefine):', [round(x, 4) for x in diff_ref])
t_enr, p_enr = stats.ttest_rel(full_rho, no_enr_rho)
t_ref, p_ref = stats.ttest_rel(full_rho, no_ref_rho)
print(f'Paired t-test (Enrichment): t={t_enr:.3f}, p={p_enr:.4f}')
print(f'Paired t-test (Refinement): t={t_ref:.3f}, p={p_ref:.4f}')
