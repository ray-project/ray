# sort_benchmark_bench

Local benchmarks derived from the upstream Ray Data nightly test
`release/nightly_tests/dataset/sort_benchmark.py`. The upstream test runs a
~1 TB shuffle on 32 m5.4xlarge nodes; these benchmarks engage the same
operating regimes locally on a 24 vCPU / 256 GB devpod, in seconds rather
than tens of minutes.

The upstream test reports wall time, total spilled bytes (cluster object
store), and driver peak RSS, on either the sort or random-shuffle code
path. Both paths drive Ray Data's all-to-all map → reduce shuffle. The
regimes covered are the failure/pressure modes that path enters at scale.

## Calibrated against

- **Source tree under measurement**: ray 2.55.0 (pristine), located at
  `/workspace/ray/v11_pristine/python/ray` in the calibration environment.
- **Hardware**: 24 vCPU cgroup, 256 GB RAM, /dev/shm = 8 GB. Several
  benchmarks set `RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` to disk-back the
  object store when they need a plasma cap larger than 8 GB.
- **Python**: `/opt/venv/bin/python` (Python 3.11 venv with Ray 2.55.0
  importable from the source tree above).

Reps default to 3 measured + 1 unrecorded warmup. All benchmarks complete
well under the 300 s absolute cap and the 120 s target.

## Layout

```
sort_benchmark_bench/
  BENCHMARKS.md          (this file)
  _common.py             (shared datasource, signal capture, patches, harness)
  b1_sort_spill_thrash/run.py
  b3_sort_merge_hot/run.py
  b4_sort_driver_refs/run.py
```

The numeric prefixes match the design table in the calibration notes
(`/workspace/ray/skill-runs/sort_benchmark/PHASES.md`); B2 and B5 were
designed but excluded — see "Excluded" at the bottom.

---

## B1 — sort_spill_thrash

**Regime**: Object-store spill thrash because shuffle working set far
exceeds object store memory. (Also engages: shuffle blocks pegged at
plasma cap during materialize.)

**Run**:

```bash
/opt/venv/bin/python b1_sort_spill_thrash/run.py
```

Defaults: M=16 partitions × 256 MiB = ~4 GiB working set; plasma capped
at 2.5 GiB; 16 vCPUs. Wall time ~12–18 s per rep.

**Correctness**: `Dataset.sort(key="c_0")` materializes all rows; assert
`rows == 39_768_208` after each rep. Output is deterministic given the
seeded `DeterministicIntRowDatasource`.

**Performance signals**:

- Primary: `spilled_gb_mean` from the summary output. The pristine Ray
  baseline reports ~11–12 GiB spilled per rep. A change that reduces this
  by reducing redundant intermediate-block materialization is real
  progress; a change that pushes it up indicates regression.
- Secondary: `wall_seconds_mean`. Variance is ~15% on this hardware; treat
  changes < 20% as noise unless `wall_seconds_stdev` is also unusually
  small.

**Validity check**: `--no-spill` shrinks `partition_bytes` 8×. Working
set fits in plasma; `spilled_gb_mean` should drop to ~0 and wall time to
< 1 s. If spilling persists in `--no-spill` mode, the spill-trigger
detection has been broken by a change.

---

## B3 — sort_merge_hot

**Regime**: Reducer merge-sort dominates compute. With wide rows
(1 KiB payload), the merge step's memcpy work outweighs the per-block
sort and the int64 key compares.

**Run**:

```bash
RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 /opt/venv/bin/python b3_sort_merge_hot/run.py
```

(The script also self-sets the env var if missing, but exporting it makes
the intent explicit.) Defaults: M=64 partitions × 64 MiB × 1 KiB rows;
plasma 32 GiB disk-backed; 16 vCPUs. Wall ~4–5 s per rep.

**Correctness**: assert `rows == 4_161_728` and the run reports
`spilled_gb_mean ≈ 0`. Output deterministic.

**Performance signals**:

- Primary: `ratio_reduce_over_map` in `extras`. The pristine baseline
  reports ~2.3–2.5 (reduce dominates by 2.3–2.5×). A reduction in this
  ratio — *holding `wall_seconds_mean` constant or lower* — indicates
  the merge step got cheaper relative to map.
- Primary: `reduce_total_s` (built-in metric from `ds.stats()`'s
  `Suboperator * SortReduce` block, parsed by `parse_op_total_seconds`).
  Pristine ~8.1–8.6 s. This is the signal the optimization targets —
  total reducer wall time across all reduce tasks.
- Secondary: `wall_seconds_mean`. Variance is ~6% (tight).

**Validity check**: `--low-fanout` shrinks M to 8 while multiplying
`partition_bytes` by 8 (total volume held constant). Reducer fanout
drops 8× → `ratio_reduce_over_map` drops from ~2.4 to ~1.75 and
`reduce_total_s` drops by ~25%. If the ratio doesn't move, the
reduce-fanout signal has been disconnected from the regime.

---

## B4 — sort_driver_refs

**Regime**: Driver-side O(M·N) object-ref bookkeeping in the pull-based
shuffle scheduler. Each map task has `num_returns = 1 + output_num_blocks`
object refs; with N reducers this is M·N driver-side ObjectRef holders,
each of which costs `CALLER_MEMORY_USAGE_PER_OBJECT_REF = 3000` bytes
plus Python overhead.

**Run**:

```bash
RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 /opt/venv/bin/python b4_sort_driver_refs/run.py
```

Defaults: in one process, runs M=16 (M·N=256) then M=128 (M·N=16384,
64× more refs). Per-partition size 8 MiB constant. Plasma 16 GiB
disk-backed; 16 vCPUs.

**Correctness**: assert `rows == 1_242_752` for M=16 and
`rows == 9_942_016` for M=128. `spilled_gb_mean` should be ~0 in both
groups.

**Performance signals**:

- Primary: `driver_maxrss_gb_max` reported in each group's summary.
  Pristine baseline: M=16 group ~0.36 GB, M=128 group ~1.96 GB. The
  ratio (~5×) is what reflects the driver-ref overhead. Optimizations
  that batch object refs or release them earlier should narrow the gap.
- Secondary: `MN` field in each rep's `extras` (captured by the
  signature-forwarding `install_pull_scheduler_patch`) — the regime
  intensity proxy. Per-ref slope ≈ (maxrss_high − maxrss_low) /
  (MN_high − MN_low) gives a back-of-envelope per-ref cost in bytes;
  pristine is around 100 KB/ref (much higher than the upstream 3 KB
  constant due to Python-side bookkeeping per ObjectRef).
- Secondary: `wall_seconds_mean` per group. Pristine: M=16 ≈ 0.21 s,
  M=128 ≈ 1.99 s. The 9.4× wall-time ratio at 8× data and 64× M·N is
  consistent with O(M·N) overhead added on top of O(N) data work.

**Validity check**: this benchmark *is* the validity check — it
compares two M points within one process. A change to the scheduler
that doesn't preserve a positive slope (driver_maxrss_gb_max grows with
M·N) has broken the regime.

---

## Excluded benchmarks

- **B2 (random_shuffle_spill_thrash)** — comparison-only probe that
  confirms R1 (spill thrash) engages on the random-shuffle code path
  too. Same regime as B1; per the skill ("drop benchmarks that exist
  only as comparison points — those belong in implementation-time
  validation"). Source preserved at
  `/workspace/ray/skill-runs/sort_benchmark/bench_b2_random_shuffle_spill.py`.
- **B5 (sort_sample_overhead)** — designed for the sample-quantile
  boundary computation regime. **Gap recorded**: on this hardware, the
  `SortTaskSpec.sample_boundaries` driver-side wall time stays at ≤ 5%
  of total sort op time across all calibrations tried (M ∈ {128, 256,
  512}, partition sizes from 256 KiB to 2 MiB). Sampling never engages
  "at intensity" within the budget — at large M, R3's M·N driver
  bookkeeping eclipses it. The patch helper
  (`install_sample_boundaries_patch` in `_common.py`) is preserved as a
  measurement primitive for inner-loop work that targets sampling
  specifically. Source: `bench_b5_sort_sample_overhead.py` in the
  skill-runs scratch dir.

## Working tree pointers

- Skill scratch (Phase 1–3 design notes, calibration evidence, all
  iterations including excluded benchmarks):
  `/workspace/ray/skill-runs/sort_benchmark/`
- Phase 3 calibration results table:
  `/workspace/ray/skill-runs/sort_benchmark/CALIBRATION.md`
- Upstream test:
  `/workspace/ray/v11_pristine/release/nightly_tests/dataset/sort_benchmark.py`
