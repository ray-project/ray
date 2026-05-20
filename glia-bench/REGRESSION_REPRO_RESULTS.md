# Regression-repro suite — results on devpod

Five micro-benchmarks (`glia-bench/regression_repro/b1`–`b5`) reproduce
on a 24 vCPU devpod the regressions a friend's sf100 cluster comparison
flagged between `glia/scheduler-perf-v4` (= 2.55.0 + M-series) and
upstream master. After rebasing M-series onto current master, the
regressions vs master close (within rep noise) and one bench (b3) shows
M-series adding ~30% bonus speedup on top of master.

Library versions matched between `master` and `rebased`
(`pyarrow=24.0.0, pandas=3.0.2, numpy=2.4.4`) so the comparison
isolates Ray Data code-level changes.

## Headline

| Bench | glia (m6_fixed) | master nightly | rebased | rebased vs master |
|---|---|---|---|---|
| b1 map_batches_fusion (`batch_size=10000`) | 8.03s | 9.00s | 9.42s | +4.7% |
| b1 map_batches_fusion (`batch_size="auto"`) | crash | 2.43s | 2.36s | −2.8% |
| b2 inference_actor_pool (M=100, N=2000) | 19.23s | 16.61s | 16.92s | +1.8% |
| b3 tpch_q3_autoscale | 35.63s | 31.57s | **22.26s** | **−29.5%** |
| b4 wide_schema_read | 10.48s | 8.48s | 8.45s | −0.3% |
| b5 tpch_q7_join_order | 80.79s | 40.38s | 41.16s | +1.9% |

All output hashes match across all five benches.

## Notes per bench

### b1 — Read↔MapBatches operator fusion
The "regression" originally reported (-86% vs glia) was largely the
test-script default change (`--batch-size` flipped from `10000` to
`"auto"`), which routes through a fusion path glia 2.55.0 base could
not access (`TypeError`). Rebased branch handles `"auto"` and matches
master's fusion. Wins are roughly equal to master's; the M-series
neither helps nor hurts here.

### b2 — Actor-pool task selection
Targets master commit #62309 (rank actors per node in a heap, replacing
O(N·M) selection with O(N + log M)). Rebased ≈ master here; the M-series
doesn't add to or subtract from this fix at devpod scale.

### b3 — TPC-H q3 multi-op DAG
Largest M-series win on top of master: rebased is 30% faster than master
nightly. This is the regime M5 (incremental budget decrement) and M3
(cached dispatch options) target — high task volume across a 12-op DAG
with multiple actor pools. On master alone, the work fits without these
optimizations; M-series eliminates per-dispatch budget recomputation
cost that grows with task count.

### b4 — Wide-schema parquet read
Targets master commits #62579 (chunk combining threshold) and #62720
(yield-only-first-schema). Rebased ≈ master; the M-series neither
helps nor hurts the read path.

### b5 — TPC-H q7 join order
The "−50% regression vs master" originally reported was not actually
plan-reordering (plans match) but block-format (Pandas→Arrow internal,
master commit #61733). Rebased matches master since the fix is
master's, not M-series's.

## Reproduction

```bash
cd glia-bench/regression_repro
./run_compare.sh   # runs b1–b5 against /opt/venv (rebased) and /opt/venv-master
```

See `_common.py` for the harness; per-bench `RESULTS.md` for original
diagnosis and methodology.
