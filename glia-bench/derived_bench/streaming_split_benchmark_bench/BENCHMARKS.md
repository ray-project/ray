# streaming_split_benchmark_bench

Local benchmarks derived from the upstream Ray Data nightly test
`release/nightly_tests/dataset/streaming_split_benchmark.py`. The
upstream test reads ImageNet parquet (~150 GB) from S3 and splits it
across 10 SPREAD-scheduled consumer actors on a 10× node release
cluster. It exists as the integration test for `Dataset.streaming_split`
— the API that powers Ray Train's per-worker dataset sharding.

These benchmarks engage the same operating regimes locally on a
24-vCPU / 256-GB devpod, in 6-15 s walls per rep, with synthetic
parquet substituted for S3.

## Calibrated against

- **Source tree under measurement**: ray 2.55.0 (pristine), located at
  `/workspace/ray/v11_pristine/python/ray` in the calibration
  environment.
- **Hardware**: 24 vCPU cgroup, 256 GB RAM, /dev/shm = 7.7 GB. The
  Ray warning about /dev/shm size is expected on this devpod and
  doesn't affect measurements (object store backs to /tmp/ray).
- **Python**: `/opt/venv/bin/python` with pyarrow 23+, numpy 2+,
  psutil installed.

Reps default: 1 unrecorded warmup + 3 measured (B1, B2) or 4 (B3).
All benchmarks complete well under the 120 s target rep cap.

## Layout

```
streaming_split_benchmark_bench/
  BENCHMARKS.md                       (this file)
  _common.py                          (synthetic parquet, consumer
                                       actor, stats parsers, harness)
  b1_streaming_split_n_workers/run.py
  b2_streaming_split_equal/run.py
  b3_streaming_split_early_stop/run.py
  _data/                              (created on first run; one
                                       16 GB parquet dataset shared
                                       across all three benchmarks,
                                       generation is idempotent)
```

## Signal capture strategy

`SplitCoordinator` and `OutputSplitter` both run inside the
coordinator actor's Python process (a remote Ray actor pinned to
the driver node by `NodeAffinitySchedulingStrategy(soft=False)`).
A driver-side class patch is invisible inside that process, so the
benchmarks do not monkey-patch.

Instead, the consumer actor enables `DataContext.verbose_stats_logs`
and returns the iterator's `stats()` text to the driver. The driver
parses two signals:

- `Streaming split coordinator overhead time: <X>s` — the
  cumulative wall time the SplitCoordinator's `get()` method spent
  for ALL N consumers combined (per-call summed; with N concurrent
  consumers this can far exceed real wall time).
- `'output_splitter_overhead_time': <Y>` from the verbose
  extra_metrics block — the OutputSplitter operator's
  `_try_dispatch_bundles` cumulative time.

These are built-in stats fields exposed by the codebase under
measurement; the parsers fall through to 0.0 if the text format
changes, so the benchmark stays runnable even after a stats
rewording (the regex breaks gracefully).

---

## B1 — streaming_split_n_workers

**Regime**: R1 (SplitCoordinator RPC-serialization bottleneck because
N consumers all serialize through one coordinator's `get()` method).
Mirrors the upstream `streaming_split.regular` YAML row.

**Run**:

```bash
/opt/venv/bin/python b1_streaming_split_n_workers/run.py
```

Defaults: 512 parquet files × 8K rows × 4 KiB payload (4.1 M rows,
~16 GB), N=10 consumer actors, ray.init num_cpus=20, object store 6 GB.
Wall ~14.6 s per rep, stdev ~1.3% (very stable).

**Correctness**: `rows_consumed == num_files * rows_per_file` asserted
each rep (4,096,000 rows).

**Performance signals**:

- Primary: `coord_overhead_s_mean`. Pristine baseline ~1012 s. This
  is the cumulative `get()` wall summed across all N consumers; with
  N=10 it averages ~100 s per consumer. A change that reduces
  per-consumer coord time without breaking correctness is real
  progress on R1.
- Primary: `wall_seconds_mean`. Pristine ~14.6 s, stdev ~0.2 s.
- Secondary: `splitter_overhead_s_mean`. Small (~11 ms) — splitter
  is not on the critical path here.
- Secondary: `actor_skew_s_mean`. Pristine ~40 ms — actors finish
  within milliseconds of each other (the coord rate-limits them
  uniformly).

**Validity check**: `--n-workers 2` (R1 trigger removed; only 2
consumers contend on the coordinator). `coord_overhead_s_mean` drops
from ~1012 s to ~12 s — **~85× reduction**, confirming the signal
moves with the regime. Wall drops from ~14.6 s to ~6.3 s.

---

## B2 — streaming_split_equal

**Regime**: R2 (OutputSplitter equal-mode dispatch overhead) primary,
R1 secondary. Mirrors the upstream `streaming_split.regular_equal`
YAML row.

**Run**:

```bash
/opt/venv/bin/python b2_streaming_split_equal/run.py
```

Defaults match B1; only `equal=True`. Wall ~8.25 s per rep, stdev ~5%.

**Correctness**: `rows_consumed == num_files * rows_per_file` (within
N rows; equal-mode may drop a small remainder for exact equalization).

**Performance signals** — note an important calibration finding:

- Primary: `wall_seconds_mean`. Pristine ~8.25 s — **substantially
  faster than B1**. Equal-mode triggers a different OutputSplitter
  code path (`_can_safely_dispatch` defers small dispatches; once the
  buffer is large enough, it dispatches in a batch with no per-bundle
  locality scan), which ends up reducing the number of coord
  round-trips. A regression here that pushes B2 wall above B1 wall
  indicates equal-mode has lost its batching property.
- Primary: `coord_overhead_s_mean`. Pristine ~58 s — **17× lower than
  B1**, for the same reason. Use the *ratio* `B1.coord / B2.coord` as
  the primary R2 indicator: pristine ratio ~17×; a ratio that
  collapses toward 1 means the equal-mode batching has been broken.
- Secondary: `splitter_overhead_s_mean`. Pristine ~11 ms — similar to
  B1 (the operator-level metric doesn't separate the two regimes at
  this scale). Reported but not load-bearing.

**Validity check**: built-in. The benchmark *is* the comparison
between equal and non-equal modes (B1 vs B2 walls + coord overhead).
A separate "B2 with equal=False" rep would just reproduce B1, so it
belongs in implementation-time validation rather than the portfolio.

---

## B3 — streaming_split_early_stop

**Regime**: R5 (early-stop teardown correctness). Mirrors the upstream
`streaming_split.early_stop` YAML row, which references issue #34819
(a GPU memory leak in the early-stop path that this test was
specifically added to cover). On a CPU-only host we proxy the leak
check with driver RSS.

**Run**:

```bash
/opt/venv/bin/python b3_streaming_split_early_stop/run.py
```

Defaults: same dataset as B1/B2; consumers break after
`num_rows_total // 2 // N` rows = 204,800 each. Wall ~9.1 s per rep,
4 reps + 1 warmup, stdev ~3%.

**Correctness signals** (this is a correctness benchmark first):

- Primary: `rows_consumed == 2,048,000` for every rep (asserted in
  `one_rep`). Per-worker rows are bounded above by the per-worker
  target × 1.5 (allowing for one final batch overshoot).
- Primary: `rss_growth_mb_first_to_last`. Pristine ~8 MB across 4
  reps (well below the 50 MB threshold for sustained leak). A run
  that exits with this >50 MB indicates RSS is growing
  monotonically and the early-stop teardown is leaking driver-side
  state.
- Secondary: `rss_progression_mb` — the per-rep RSS series, printed
  for visual inspection.

**Performance signals**:

- Primary: `wall_seconds_mean`. Pristine ~9.1 s, stdev ~0.3 s.
  Surprisingly *not* much faster than B1 (~14.6 s) despite reading
  half the rows — the executor still drains its queue while
  consumers idle on `iter_batches()` after their break.
- Secondary: `actor_skew_s_mean`. Pristine ~0.79 s — markedly larger
  than B1's ~0.04 s. The skew is the R5 timing fingerprint:
  early-stopped consumers exit ~0.8 s before the slowest (the slowest
  finishes draining its queue). A change that drives this skew
  toward zero (without breaking correctness) suggests the executor
  is short-circuiting the post-break drain.

**Validity check**: built-in. B3 vs B1 — same dataset, same N, only
`early_stop` differs. The 0.8 s actor_skew vs 0.04 s in B1 confirms
that R5 (early-stop teardown) is engaged and observable.

**Calibration note** — B3 walls are stable at ~9 s when each
invocation does a fresh `ray.init` (as the script does). In a
long-lived cluster across many sequential streaming_split runs,
walls have shown a bimodal split (~5 s for early reps, ~11 s
afterwards). Recorded as a side-finding in the calibration notes;
not exposed in the production benchmark.

---

## Excluded benchmarks

- **N=16 stress run**: would push R1 to extreme intensity
  (`probe_calibrate5.py` showed coord_overhead ~918 s at N=16 vs
  ~85 s at N=10), but it's a quantitative rather than structural
  variant of B1 and competes with the 24-vCPU budget. Belongs in
  implementation-time validation if a fix is N-sensitive.

- **Validity rep "B1 with N=2"**: drops B1's coord_overhead from
  ~1012 s to ~12 s and confirms R1 detection. Comparison-only;
  invokable via `--n-workers 2` on B1, not a separate portfolio
  entry.

## Gaps recorded

- **R3 (cross-actor barrier synchronization)**: designed but does not
  engage at intensity on a single-host devpod. Actor scheduling
  skew in B1 is consistently <60 ms — the `_barrier()` in the
  coordinator resolves before any consumer's first
  `iter_batches()` call. Engaging it would require multi-host or
  artificially-staggered actor startup, which doesn't match how
  the regime appears in production. Skipped honestly; no synthetic
  stand-in shipped.

- **R4 (external_consumer_bytes feedback loop)**: visible in coord
  stats but has no separable signal from R1 — every `coord.get()`
  triggers one `set_external_consumer_bytes` write under the same
  lock. The two regimes share their primary signal
  (`coord_overhead_s`), so they can't be distinguished by the
  detection hypotheses listed in PHASES.md. Folded into R1.

## Working tree pointers

- Skill scratch (Phase 1–3 design notes + calibration evidence):
  `/workspace/ray/skill-runs/streaming_split_benchmark/`
- Phase 1+2 design table:
  `/workspace/ray/skill-runs/streaming_split_benchmark/PHASES.md`
- Phase 3 calibration record:
  `/workspace/ray/skill-runs/streaming_split_benchmark/CALIBRATION.md`
- Upstream test:
  `/workspace/ray/v11_pristine/release/nightly_tests/dataset/streaming_split_benchmark.py`
- Upstream YAML invocations:
  `/workspace/ray/v11_pristine/release/release_data_tests.yaml` (`name: streaming_split`)
