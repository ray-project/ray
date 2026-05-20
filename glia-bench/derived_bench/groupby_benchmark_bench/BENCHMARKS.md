# groupby_benchmark_bench

Local benchmarks derived from the upstream Ray Data nightly test
`release/nightly_tests/dataset/groupby_benchmark.py`.  The upstream test
runs `Dataset.groupby(...).mean(...)` (aggregate path) or
`Dataset.groupby(...).map_groups(...)` (map_groups path) on TPC-H
`lineitem` at scale factor 100 (~100 GB) read from S3, under either
`sort_shuffle_pull_based` or `hash_shuffle`, with two key cardinalities
(84 groups via `column08 column13 column14`; 7M groups via
`column02 column14`).  These benchmarks engage the same operating regimes
locally on a 24 vCPU / 256 GB devpod, in tens of seconds rather than
hour-scale cluster runs.

The upstream test reports wall time and `object_store_spilled_total_gb`.
The regimes covered are the failure/pressure modes that the upstream
matrix enters at scale, plus the regime-detection signal used to confirm
each one engages.

## Calibrated against

- **Source tree under measurement**: ray 2.55.0 (pristine), located at
  `/workspace/ray/v11_pristine/python/ray` in the calibration environment.
- **Hardware**: 24 vCPU cgroup, 256 GB RAM, /dev/shm = 8 GB.  B5 sets
  `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` to disk-back the object store
  when the cap requires /tmp.
- **Python**: `/opt/venv/bin/python` (Python venv with Ray 2.55.0
  importable from the source tree above).
- **Data**: synthetic TPC-H-shaped lineitem Parquet, generated
  deterministically into `/tmp/groupby_bench_data/sf_<rows>_<files>_<seed>`
  on first run and reused thereafter.  Override the location by setting
  `GROUPBY_BENCH_DATA_ROOT`.

Reps default to 3 measured + 1 unrecorded warmup.  All benchmarks complete
under the 120 s target with the exception of B5 at ~92 s (under the 300 s
cap).

## Layout

```
groupby_benchmark_bench/
  BENCHMARKS.md              (this file)
  _common.py                 (shared data generator, signal capture, harness)
  b1_agg_sort_lowcard/run.py
  b2_agg_sort_highcard/run.py
  b3_mapgroups_sort_lowcard/run.py
  b4_mapgroups_hash_highcard/run.py
  b5_agg_sort_spill/run.py
```

The numeric prefixes match the design table in the calibration notes
(`/workspace/ray/skill-runs/groupby_benchmark/phase2_design.md`).  The
read-only probe documented there as **P0** was used only to validate R1
in Phase 3 and is not shipped here.  Two upstream matrix cells are also
intentionally absent: `aggregate × hash_shuffle` is asserted out by Ray
itself (`generate_aggregate_fn` only accepts the sort variants), and
`map_groups × {hash, sort} × lowcard × largeset` collapses to B3 (sort)
and a wall-time-prohibitive hash variant outside the local time budget.

---

## B1 — agg_sort_lowcard

**Regime**: R1 — sort-shuffle all-to-all bandwidth saturation.

**Workload**: 60 M-row TPC-H-shaped lineitem; group by
`column08, column13, column14` (≈84 distinct combinations); aggregate
`mean("column05")` under `sort_shuffle_pull_based` with
`override_num_blocks=50`.

**Run**:
```
/opt/venv/bin/python b1_agg_sort_lowcard/run.py
```

**Signal**: wall time (per-rep `[b1_agg_sort_lowcard] rep N: <s>`).
Calibrated mean ≈ 49.8 s, σ ≈ 0.6 s.  An optimisation that touches sort-
shuffle on the critical path moves wall noticeably; one that doesn't,
won't.  The `spilled` line in each rep is incidental here (default
plasma is 9.3 GiB so a slice of shuffle output spills) but stable across
reps (~8.7 GiB).  Hash a deterministic property of the consumed result
across reps if the consumed dataset's mean is needed for correctness
checking.

---

## B2 — agg_sort_highcard

**Regime**: R5 — boundary-sampling cost amplified at high key
cardinality (also exercises R1 at the smaller dataset scale).

**Workload**: 6 M-row dataset; group by `column02, column14` (high-card,
~all-rows-distinct on `column02`); aggregate `mean("column05")` under
`sort_shuffle_pull_based`.

**Run**:
```
/opt/venv/bin/python b2_agg_sort_highcard/run.py
```

**Signal**: wall time.  Calibrated mean ≈ 30 s, σ ≈ 1.7 s.  Cardinality
A/B (Phase 3): swapping the key for the low-cardinality 84-group key
drops wall ~19×, so any change that shifts how sample/shuffle scales
with cardinality will be visible in this benchmark.

The harness also installs a signature-forwarding patch on
`SortTaskSpec.sample_boundaries` and reports `sample_calls` /
`sample_total_s` in each rep's `extra`.  Calibration showed
`sample_total_s` is small (≈ 0.013 s/rep) — the boundary-sample
*driver-side dispatch* is not the cost amplifier; the wall-time
amplification at high cardinality lives in the resulting shuffle work
itself.  The patch is still useful: an optimisation that *moves*
boundary computation onto the driver (or off it) will move
`sample_total_s` even when total wall time barely changes.

---

## B3 — mapgroups_sort_lowcard

**Regime**: R4 — per-group heap blow-up under `map_groups` (also
engages R3 spill and R1 sort bandwidth).

**Workload**: 60 M rows, low-cardinality key (column08 column13 column14),
sort-shuffle, `map_groups(normalize_table)` with `batch_format="pyarrow"`.
Each group is ~700 K rows and `map_batches(batch_size=None)` over the
sorted dataset puts the entire group into one task batch.

**Run**:
```
/opt/venv/bin/python b3_mapgroups_sort_lowcard/run.py
```

**Signal**: wall time AND `spilled_bytes`.  Calibrated wall mean ≈ 58 s,
σ ≈ 1.7 s; spill mean ≈ 25.5 GiB.  The R4 control is exactly B1 (same
dataset/key, swap consume map_groups → aggregate): in Phase 3 the swap
dropped spill by ~3× (25.5 → 8.7 GiB) and wall by ~15%.  Optimisations
that fuse partial reductions into the shuffle map or that cap per-task
group memory will move the spill metric here; ones that only touch
sort-shuffle compute will move wall but not spill.

---

## B4 — mapgroups_hash_highcard

**Regime**: R2 — hash-aggregator actor pool sizing / steady-state
pressure.

**Workload**: 3 M rows, high-cardinality key (column02 column14),
`hash_shuffle` with `RAY_DATA_MAX_HASH_SHUFFLE_AGGREGATORS=8` and
`default_hash_shuffle_parallelism=16` (16 reduce partitions).
Map_groups consume.

**Run**:
```
/opt/venv/bin/python b4_mapgroups_hash_highcard/run.py
```

**Signal**: wall time.  Calibrated wall mean ≈ 83 s, σ < 0.4 s (the
tightest variance in the portfolio — hash routing is deterministic
because the aggregator pool size and partition count are fixed).
Strategy A/B (Phase 3): swapping `hash_shuffle` → `sort_shuffle_pull_based`
on the same workload increases wall to ~120 s.  Optimisations to the
hash-aggregator code path (actor pool start-up, partitioning, finalize)
will move this benchmark; sort-shuffle optimisations will not.

---

## B5 — agg_sort_spill

**Regime**: R3 — object-store spill under cap.

**Workload**: same logical config as B1 (60 M-row aggregate sort-shuffle
low-card key) but with `object_store_memory=2 GB`.  The shuffle working
set far exceeds the cap, so Ray spills ~9.4 GB to local disk during the
shuffle and the spill IO appears on the critical path.

**Run**:
```
/opt/venv/bin/python b5_agg_sort_spill/run.py
```
(`RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` is set automatically by the
harness when `object_store_memory_bytes` is configured.)

**Signal**: wall time AND `spilled_bytes`.  Calibrated wall mean ≈ 91.8 s
(σ < 0.3 s), spill mean ≈ 8.7 GiB (deterministic to byte level across
reps).  Validity (Phase 3): lifting the cap to 32 GB drops spill to 0
and wall to 49 s.  Optimisations to the spill path (eviction policy,
spill IO batching, spill compression) will move this benchmark; ones
that only touch the in-memory shuffle path will move wall but leave
the spill volume unchanged.

---

## Cross-benchmark notes

- **Determinism**.  All datasets are written from a fixed per-file seed
  (`20260427 + file_idx`) and reused across runs (sentinel
  `.manifest.json` in each `sf_<rows>_<files>_<seed>` directory).  Per-rep
  output of any deterministic consume function is invariant across reps
  modulo Ray scheduling order; no consume function in the upstream test
  produces order-dependent output (mean is commutative; map_groups
  yields per-group results).
- **Ray init**.  Each `run.py` calls `ray.init(num_cpus=24, ...)` exactly
  once and shuts down at the end.  Reps run inside the same Ray instance
  to avoid re-amortising init overhead.
- **Validity controls (Phase 3) are not shipped here**.  They live in
  `/workspace/ray/skill-runs/groupby_benchmark/results/*.json`.  Re-run
  them manually if a regime stops engaging during inner-loop work.
- **R6 (read↔shuffle backpressure)** is a passive backdrop; the read
  op finishes well before the shuffle in every benchmark and is not a
  primary inner-loop signal here.  See the Phase 3 results for the
  read-only validity probe.
- **Gaps recorded** in Phase 3:
  - R5 isolation reads sample-stage time via the optional
    `SortTaskSpec.sample_boundaries` patch in B2; per-stage timing for
    other sub-ops (`Sort sample`, `Shuffle Map`, `Shuffle Reduce`) is
    available only via parsing `ds.stats()` strings, which we don't do
    automatically.  Add a stats-parsing hook in `_common.py` if needed.
  - R2 per-aggregator skew distribution is not currently captured by
    B4; the metric is exposed in `OpRuntimeMetrics` but the harness does
    not snapshot it.
