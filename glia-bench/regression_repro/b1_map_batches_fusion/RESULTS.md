# b1_map_batches_fusion — devpod results

## Setup

- Hardware: 24 vCPU devpod
- Parquet: 100 files × 1M rows × 12 int64 columns × 5 row groups/file (~10 GB total)
- Pipeline: `read_parquet → map_batches(increment) → map_batches(increment) → map_batches(dummy_write)`
- `override_num_blocks=100` so additional_split_factor=1 (lifts the operator-fusion gate at line 227 of operator_fusion.py)
- 1 unrecorded warmup + 3 reps per cell

## Results

| Cell | Tree | `batch_size` | Op-graph | Wall (mean) | vs glia_b10k |
|---|---|---|---|---|---|
| **glia_b10k** | m6_fixed (2.55.0 + M-series) | `10000` | 2 ops: ReadParquet \| MapBatches→MapBatches→MapBatches | **8.03s** | (baseline) |
| **master_b10k** | master nightly (35629a802a) | `10000` | 2 ops: same as glia | **9.00s** | +12% |
| **master_auto** | master nightly | `"auto"` | **1 op: ReadParquet→MapBatches→MapBatches→MapBatches** | **2.43s** | **−70%** |
| **glia_auto** | m6_fixed | `"auto"` | (crash) | n/a | `TypeError: '>=' not supported between instances of 'str' and 'int'` |

## Diagnosis

The "regression" the friend's `comparison.md` reports for `map_batches_*` (-77% to -86%) decomposes into **two independent contributions**:

1. **Test-script default change (~70% of the gap).** Upstream's `release/nightly_tests/dataset/map_benchmark.py` changed its `--batch-size` default from `10000` to `"auto"` after 2.55.0. With `"auto"`, master fuses Read→Map into one TaskPoolMapOperator. The friend's glia branch tests still default to `10000`, master release tests default to `"auto"` — so 91513 ran unfused and 91797 ran fused.

2. **Pure code overhead since 2.55.0 (~12%).** Even with the same `batch_size=10000`, master is slightly slower than glia (9.0s vs 8.0s). Master added per-task overhead (chunk combining, schema yielding, etc.) that doesn't pay off without fusion.

When (1) is taken, master fuses → 2.43s; glia can't take (1) without code changes (the `"auto"` feature was added on master and 2.55.0 base raises `TypeError` in `BlockRefBundler`).

## Implication for the rebase

Rebasing m6_fixed onto master will:
- Add `batch_size="auto"` support so the upstream test no longer crashes.
- Enable the Read→Map fusion path for any workload that uses `"auto"` and hits split_factor=1.
- Net for this test family: glia gains ~5.6s of wall back from fusion, loses ~1s to per-task overhead = ~4.5s net win, matching the upstream −70% direction.

## Validity check (per Phase 3 of test_to_benchmark)

- Wall delta direction matches upstream (master is faster).
- Magnitude on devpod (−70%) is comparable to upstream (−85%); the smaller gap is consistent with smaller cluster (24 vs 80 CPU) and smaller data (10 GB vs ~100 GB).
- Op-graph confirmation: master_auto has the EXACT operator name (`ReadParquet->MapBatches→MapBatches→MapBatches`) seen in friend's 91797 logs.

## Reproducer

```bash
cd glia-bench/regression_repro/b1_map_batches_fusion

# generate (one-time, ~1 min)
/opt/venv/bin/python -c "from run import _make_parquet_files; _make_parquet_files('/tmp/regression_repro_b1_parquet_v2')"

# four cells
/opt/venv/bin/python        run.py --batch-size 10000 --cell glia_b10k
/opt/venv-master/bin/python run.py --batch-size 10000 --cell master_b10k
/opt/venv-master/bin/python run.py --batch-size auto  --cell master_auto
/opt/venv/bin/python        run.py --batch-size auto  --cell glia_auto    # expected: TypeError
```
