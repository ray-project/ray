# join_benchmark_bench

Local benchmarks derived from the upstream Ray Data nightly test
`release/nightly_tests/dataset/join_benchmark.py`. The upstream test runs
`Dataset.join(...).count()` on TPC-H `lineitem` ⋈ `orders` at scale
factor 100 (~100 GB) read from S3 across four join types
(`inner`, `left_outer`, `right_outer`, `full_outer`) on a
`fixed_size_100_cpu_compute` cluster, with `num_partitions=50`. These
benchmarks engage the same operating regimes locally on a 24 vCPU /
256 GB devpod, in tens of seconds rather than hour-scale cluster runs.

The upstream test reports wall time and `object_store_spilled_total_gb`.
The regimes covered are the failure/pressure modes the upstream matrix
enters at scale, plus the regime-detection signals used to confirm each
one engages. Phase 5 sizing decision: synthetic local datasets of
comparable shape (4:1 left:right multiplicity) replace the S3 TPC-H
inputs; rows scaled down from ~600 M:150 M to 24 M:6 M.

## Calibrated against

- **Source tree under measurement**: ray 2.55.0 (pristine), located at
  `/workspace/ray/v11_pristine/python/ray` in the calibration environment.
- **Hardware**: 24 vCPU cgroup, 256 GB RAM, /dev/shm = 8 GB, 28 TB
  overlay disk.
- **Python**: `/opt/venv/bin/python` (PyArrow 23.0.1, pandas 2.3.3,
  NumPy 2.2.6, psutil installed).
- **Data**: synthetic TPC-H-shaped lineitem (24 M rows, 16 cols) + orders
  (6 M rows, 9 cols), generated deterministically into
  `/tmp/join_bench_data/j_l24000000_lf24_r6000000_rf12_s20260427/{lineitem,orders}/`
  on first run and reused thereafter. Override the location by setting
  `JOIN_BENCH_DATA_ROOT`.

Reps default to 3 measured + 1 unrecorded warmup. All benchmarks
complete under the 120 s target with the exception of B3 at ~50 s
(still well under the 300 s cap).

## Layout

```
join_benchmark_bench/
  BENCHMARKS.md              (this file)
  _common.py                 (shared data generator, signal capture, harness)
  b1_inner_hot/run.py
  b2_full_outer_hot/run.py
  b3_inner_manyparts/run.py
  b4_inner_spill/run.py
```

The numeric prefixes match the design table in the calibration notes
(`/workspace/ray/skill-runs/join_benchmark/phase2_design.md`). The
read-only probe documented there as **P0** was used only to validate R2
in Phase 3 (P0 wall ~1 s vs B1 wall ~21 s, confirming the join — not the
read — dominates B1) and is not shipped here. Two upstream matrix cells
are intentionally absent: `left_outer` and `right_outer` live between
`inner` and `full_outer` on the only axis where they differ (output
cardinality, R4) and so add cost without adding signal coverage; B1
covers the inner end, B2 the full_outer end.

## Signals

All benchmarks emit per-rep:
- `wall_seconds`: wall time of the join + count.
- `out_rows`: result row count (deterministic hash across reps).
- `spilled_gb`: cluster-wide `spilled_bytes_total` delta from the GCS
  memory summary (same metric the upstream test reports).
- `agg_peak_total`: sum of RSS across all `HashShuffleAggregator` actor
  processes at any sample point during the rep (R2 signal).
- `agg_peak_single`: max per-process RSS observed for any single
  aggregator actor (R2 signal — partition-level Arrow join working set).
- `agg_n`: peak count of concurrent aggregator processes.

The aggregator-RSS sampler walks `/proc` every 0.1 s, filters by `comm`
containing `HashShuffl`, and reads `VmRSS` from `/proc/<pid>/status`.
Source-tree assumption: the `HashShuffleAggregator` actor class still
sets its `comm` via Ray's standard `setproctitle` path (in
`AggregatorPool.start` →
`HashShuffleAggregator.options(...).remote(...)` in
`ray.data._internal.execution.operators.hash_shuffle`).

## Calibrated baseline (mean over 3 measured reps)

| Bench | Regimes | Wall (s) | Spill (GB) | Out rows | Single-agg peak RSS (GB) |
|---|---|---|---|---|---|
| B1 inner_hot | R1, R2 | 20.5 ± 0.4 | 0.00 | 24,000,000 | 1.54 |
| B2 full_outer_hot | R1, R2, R4 | 21.0 ± 0.2 | 0.00 | 24,109,966 | 1.33 |
| B3 inner_manyparts | R1 amplified | 50.5 ± 1.1 | 0.00 | 24,000,000 | 1.22 |
| B4 inner_spill | R3 | 33.1 ± 5.0 | 6.34 | 24,000,000 | 1.14 |

---

## B1 — `inner_hot`

**Regimes**: R1 (hash-shuffle map fan-out CPU saturation), R2
(per-partition Arrow-join heap working-set).

**Workload**: 24 M-row synthetic TPC-H lineitem ⋈ 6 M-row orders on
`l_orderkey == o_orderkey`, `num_partitions=50`, `join_type=inner`,
`object_store_memory=12 GiB` (sized so the shuffle working set fits
plasma — R3 explicitly absent).

**Run**:
```
/opt/venv/bin/python b1_inner_hot/run.py
```

**Signal — performance**: `wall_seconds_mean` (per-rep
`[b1_inner_hot] rep N: <s>`). Calibrated mean ≈ 20.5 s, σ ≈ 0.44 s. An
optimisation that touches hash-shuffle on the critical path or reduces
per-partition Arrow join cost will move wall noticeably; an optimisation
unrelated to the join path should not. Pair with `agg_peak_single` to
distinguish R1 movement from R2 movement: R2-targeted optimisations move
that number, R1-targeted optimisations move wall but not single-agg
peak.

**Signal — correctness**: `out_rows` is a single deterministic integer
across reps (24,000,000 for this benchmark). The harness asserts a
single-element `out_rows_set`; any movement is a correctness regression.

**Validation lever**: increase `object_store_memory` further → wall and
spill unchanged (already non-spilling, R3 absent). Decrease to 4.5 GB →
becomes B4 (R3 engages; wall +60%; spill 6 GB).

---

## B2 — `full_outer_hot`

**Regimes**: R1, R2, R4 (output cardinality skew across join types).

**Workload**: same as B1 except `join_type=full_outer`.

**Run**:
```
/opt/venv/bin/python b2_full_outer_hot/run.py
```

**Signal — performance**: `wall_seconds_mean` ≈ 21.0 s. Wall delta
vs. B1 (~0.5 s) holding shuffle params constant is the per-row
join-output construction cost. Tiny on this dataset because nearly
every left row matches; the upstream SF100 test sees a larger delta
because of the absolute size.

**Signal — correctness/regime**: `out_rows` is **24,109,966** for B2 vs
**24,000,000** for B1. The +109,966 unmatched right-side rows are the
direct R4 detection signal — full_outer keeps right-only rows, inner
does not. Compare to B1 in the same iteration; the row delta is exactly
the right-side orderkeys with no matching lineitem (deterministic under
the fixed seed).

**Validation lever**: change to `inner` → equals B1. Change to
`left_outer` or `right_outer` → row count between 24,000,000 and
24,109,966.

---

## B3 — `inner_manyparts`

**Regime**: R1 amplified (hash-shuffle map fan-out: 4× more output
blocks per side, 4× more shuffle tasks).

**Workload**: same data as B1 but `num_partitions=200` (4× upstream's
50). Aggregator count pinned at 24 via
`RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS=24` to make the
partitions-per-aggregator scaling visible.

**Run**:
```
/opt/venv/bin/python b3_inner_manyparts/run.py
```

**Signal — performance**: `wall_seconds_mean` ≈ 50.5 s (~2.5× B1).
Optimisations to the shuffle map step (block partitioning, shuffle task
launch overhead, output-block writeback) move B3 wall *more* than B1
wall — making B3 the high-leverage probe for that part of the operator.

**Signal — correctness**: `out_rows = 24,000,000` (matches B1 exactly).

**Validation lever**: drop `num_partitions` back to 50 → equals B1
within noise. Increase to 400 → wall scales up further; 100 → roughly
linear interpolation.

---

## B4 — `inner_spill`

**Regime**: R3 (object-store spill because the shuffled-shard working
set exceeds plasma capacity).

**Workload**: same as B1 except `object_store_memory=4.5 GiB` (below the
~6 GB shuffled-shard working set). `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1`
is set automatically so Ray will host the small plasma on /tmp.

**Run**:
```
/opt/venv/bin/python b4_inner_spill/run.py
```

**Signal — regime**: `spilled_gb_mean` ≈ 6.3 GB (vs B1's 0.0). This is
the same `spilled_bytes_total` counter the upstream test reports, and
is the direct R3 detection signal.

**Signal — performance**: `wall_seconds_mean` ≈ 33.1 s (~62% slower
than B1). Variance is higher (σ ≈ 5 s) because /tmp IO timing is
jittery — that's an inherent property of spill-bound workloads. Use
**spilled_gb** as the primary R3 signal and wall as the secondary.

**Signal — correctness**: `out_rows = 24,000,000` (matches B1).

**Validation lever**: raise `object_store_memory` to 12 GB → equals B1
(spill drops to 0, wall drops to 20.5 s — the canonical "remove the
trigger" check). Lowering plasma below 4 GB on this hardware causes the
workload to deadlock — 4.5 GB is the smallest size at which the
workload completes within the 300 s cap.

---

## Notes on inner-loop usage

For optimisation work targeting one regime, use the matching benchmark:

- **Hash-shuffle / partition handling**: B3 (R1 amplified). Use B1 as the
  "no amplification" comparison. Movement in B3 wall but not B1 wall is
  a clean signal that the change touched the shuffle-map path.
- **Arrow join finalize / per-partition heap**: B1, B2. The
  `agg_peak_single` RSS signal moves with R2-specific changes.
- **Spill IO / plasma management**: B4. The `spilled_gb` signal is the
  primary axis; pair with B1 to confirm the change does not introduce
  spill in the no-pressure case.
- **Join-type semantics / output construction**: B1 + B2 row counts
  must remain 24,000,000 vs 24,109,966. Any movement is a correctness
  regression.

For broader cross-regime validation, run all four benchmarks; total
wall is ~125 s plus warmup ~40 s plus generator amortised first-time
cost ~120 s.
