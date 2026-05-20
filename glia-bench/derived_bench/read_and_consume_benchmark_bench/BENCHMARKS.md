# read_and_consume_benchmark_bench

Local benchmarks derived from the upstream Ray Data nightly test
`release/nightly_tests/dataset/read_and_consume_benchmark.py`. The
upstream test is a parameterized matrix that combines an input format
(parquet / image / tfrecords) with a consumer (count, iter_bundles,
iter_batches, write, to_tf, iter_torch_batches), reading multi-GB to
multi-TB datasets from S3 on a 10× m5.2xlarge cluster
(80 vCPU / 320 GiB total). Each YAML matrix row times one
`(format, consumer)` pair.

These benchmarks engage the same operating regimes locally on a 24
vCPU / 256 GB devpod, in single-digit seconds rather than tens of
minutes, with synthetic input substituted for S3 (the upstream
test measures `read+consume` wall, not S3 latency in isolation, so
this is a sizing decision).

## Calibrated against

- **Source tree under measurement**: ray 2.55.0 (pristine), located
  at `/workspace/ray/v11_pristine/python/ray` in the calibration
  environment.
- **Hardware**: 24 vCPU cgroup, 256 GB RAM, /dev/shm = 7.7 GB. All
  benchmarks fit their object store in /tmp/ray (Ray warns about the
  small /dev/shm; it's expected on this devpod and doesn't affect
  measurements).
- **Python**: `/opt/venv/bin/python` (Python 3.11 venv) with pyarrow
  23.0.1, numpy 2.2.6, pillow 12.2.0.

Reps default to 3 measured + 1 unrecorded warmup. All benchmarks
complete well under the 300 s absolute cap and the 120 s target.

## Layout

```
read_and_consume_benchmark_bench/
  BENCHMARKS.md          (this file)
  _common.py             (shared synthetic data gen, signal capture,
                          patches, harness)
  b1_read_parquet_iter_bundles/run.py
  b2_read_images_pil_decode/run.py
  b4_write_parquet_sink/run.py
  b6_iter_batches_format_cost/run.py
  _data/                 (created on first run; synthetic inputs are
                          deterministic and re-used across reps)
```

The numeric prefixes match the design table in the calibration notes
(`/workspace/ray/skill-runs/read_and_consume_benchmark/PHASES.md`); B3
and B5 were designed but excluded — see "Excluded" at the bottom.

---

## B1 — read_parquet_iter_bundles

**Regime**: R1 (object-store backpressure stall because consumer
throughput is set by spill/restore, not by decode), and R2 (parquet
decompression as the gating compute). Mirrors the upstream
`read_parquet_*_{fixed_size,autoscaling}` matrix entries.

**Run**:

```bash
/opt/venv/bin/python b1_read_parquet_iter_bundles/run.py
```

Defaults: 32 parquet files × 200K rows × 1 KiB payload (~6.6 GiB
total). Object store capped at 0.5 GiB; 8 vCPUs. Wall ~7.4s per rep.

**Correctness**: each rep iterates `iter_internal_ref_bundles`,
sums per-block `metadata.num_rows`, and asserts
`rows == num_files * rows_per_file`. Synthetic input is deterministic
given the seed, so the row count is byte-stable across reps.

**Performance signals**:

- Primary: `spilled_gb_mean` from the summary output. Pristine baseline
  ~1.0 GiB per rep. A change that reduces spilling without breaking
  correctness — e.g., better resource-budget backpressure, fewer
  redundant block materializations — is real progress.
- Secondary: `wall_seconds_mean`. Pristine ~7.4 s, stdev ~1%.
- Secondary: `cpus_busy_target_ratio_p90_mean`. Pristine ~0.45
  (3.6/8 CPUs busy) — workers idle waiting on object store. Should
  rise toward 1.0 if R1 is mitigated; if it stays low and wall stays
  high, the regime is unchanged.

**Validity check**: `--no-spill` shrinks `num_files` 8× → working set
fits in object store cap. `spilled_gb_mean` should drop to ~0,
`wall_seconds_mean` to ~2.8 s. If spilling persists in `--no-spill`
mode, the spill-trigger detection has been broken.

For an R2-only reference (R1 absent), pass `--object-store-gb 4.0`:
spilling drops to 0, `cpus_busy` rises to ~5.3/8 (67%), wall drops to
~3.7 s. The wall ratio between caps (~2×) quantifies R1's cost.

---

## B2 — read_images_pil_decode

**Regime**: R2 (PIL JPEG decode CPU-bound on the read map operator).
Mirrors the upstream `read_images_*` matrix entries plus the host-side
portion of `to_tf` and `iter_torch_batches` (which we can't run
without TensorFlow / CUDA on this hardware).

**Run**:

```bash
/opt/venv/bin/python b2_read_images_pil_decode/run.py
```

Defaults: 8192 synthetic JPEG files at 384×384 RGB (~30 KB each);
object store cap 0.4 GiB; 8 vCPUs. Wall ~7.2 s per rep.

The synthetic JPEGs are random uniform pixels — high-entropy noise
keeps JPEG of those bytes near the raw size and forces real decode
work per file (vs. a blank-image shortcut that would be near-free).

**Correctness**: assert `rows == num_files` (each image is one row).
Output deterministic.

**Performance signals**:

- Primary: `cpus_busy_target_ratio_p90_mean`. Pristine ~0.74
  (5.9/8 CPUs busy). Strong R2 saturation; should stay near or above
  this in any change that doesn't move decode off the critical path.
- Primary: `wall_seconds_mean`. Pristine ~7.2 s, stdev ~1%.
- Secondary: `read_remote_s` from `extras` (sum of remote wall time
  across read tasks via stats parsing). Pristine ~11.3 s — the work
  budget: total CPU-seconds spent decoding.

**Validity check**: `--low-cpu` (sets `num_cpus=1`). Wall jumps to
~12.7 s. The ratio is less than 8× because PIL's libjpeg-turbo and
numpy decompose use internal threading, but the regression is
unambiguous and confirms decode is CPU-bound, not IO-bound.

---

## B4 — write_parquet_sink

**Regime**: R5 (parquet write sink — encode + persist). Mirrors the
upstream `write_parquet` test row.

**Run**:

```bash
/opt/venv/bin/python b4_write_parquet_sink/run.py
```

Defaults: 32 parquet input files × 200K rows × 512 B payload
(~3.3 GiB), encoded back to parquet with default compression to a
temp dir under `_data/`; 8 vCPUs. Wall ~4.2 s per rep. The output dir
is rmtree'd on exit — the disk impact is per-rep transient.

**Correctness**: assert the input total row count is preserved.
Output deterministic.

**Performance signals**:

- Primary: `write_frac` in `extras` — the wall ratio
  `Write_op_seconds / (Write_op_seconds + ReadParquet_op_seconds)`.
  Pristine ~0.49 (write is half of pipeline op-wall). A reduction
  here, holding wall constant or lower, indicates the encode/persist
  step got cheaper relative to read.
- Primary: `wall_seconds_mean`. Pristine ~4.2 s, stdev ~1%.
- Secondary: `write_op_s` from `extras` — the absolute Write op wall
  time. Pristine ~2.5–2.9 s.

**Validity check**: `--no-write` swaps the consumer to
`iter_internal_ref_bundles` (no encode/persist). Wall drops to ~3.0 s
and the Write operator disappears from `ds.stats()` (`write_op_s = 0`).
Confirms that the wall-time delta is attributable to R5, not to
input-side variance.

---

## B6 — iter_batches_format_cost

**Regime**: R4 (batch-format conversion — pyarrow → numpy → pandas).
Mirrors the upstream `iter_batches_{numpy,pandas,pyarrow}` matrix
entries.

**Run**:

```bash
/opt/venv/bin/python b6_iter_batches_format_cost/run.py
```

Defaults: 8 parquet files × 400K rows × 32 B payload + 32 extra int64
columns; batch size 1024; 8 vCPUs. Per-format wall ~5–11 s; total
runtime ~95 s for 3 formats × 4 reps (1 warmup + 3 measured each).

**Correctness**: each format rep asserts the total iterated row count
matches the dataset (3.2 M rows). All three formats see the same
upstream data; only the consumer-side batch_format changes.

**Performance signals**:

- Primary: cross-format wall ratios printed at the end:
  ```
  pyarrow: wall_mean=4.8s ratio_vs_pyarrow=1.00x
  numpy:   wall_mean=7.9s ratio_vs_pyarrow=1.65x
  pandas:  wall_mean=10.5s ratio_vs_pyarrow=2.20x
  ```
  Pristine ratios: pyarrow 1.00, numpy ~1.65, pandas ~2.20. A change
  that closes the pandas-vs-pyarrow gap below ~1.4× (without breaking
  correctness) indicates R4 mitigation. A change that pushes any
  ratio above the pristine baseline indicates regression.
- Secondary: per-format `wall_seconds_mean` and `wall_seconds_stdev`
  from each `B6.<fmt>` summary. Pristine stdev < 0.1 s, so 5%
  changes are visible.

**Validity check**: built-in. The benchmark *is* the comparison —
three reps in one process with identical input but different
`batch_format`. If all three formats finish within 5% of each other
(or pandas drops below pyarrow), R4 has been broken or one of the
format paths has been re-routed.

---

## Excluded benchmarks

- **B3 (read_tfrecords_proto_parse)** — TFRecord default reader
  requires TensorFlow (`tf.train.Example`). The local venv lacks TF.
  The regime mix (R1 + R2) is the same as B2 (image decode); per the
  skill ("drop benchmarks that exist only as comparison points —
  those belong in implementation-time validation"), excluded.

- **B5 (count_parquet_metafast)** — the parquet count fast path
  (`_meta_count`) does not engage in 2.55 because the parquet
  datasource's read tasks emit `BlockMetadata(num_rows=None)`.
  `count()` falls back to `ReadParquet → AggregateNumRows`, where
  ReadParquet wall dominates and the regime is indistinguishable from
  R2 (which B1 covers under R1+R2 and B2 covers under R2 alone). The
  scratch source preserves a `bench_b5_count_parquet.py` and an
  `install_meta_count_patch` measurement primitive at
  `/workspace/ray/skill-runs/read_and_consume_benchmark/scratch/`
  for future implementations that restore the metadata fast path.

## Gaps recorded

- **R3 (driver-side iteration overhead)** — designed but not
  engageable at intensity in 2.55 with `iter_internal_ref_bundles`.
  The driver thread blocks on the executor, so per-bundle Python cost
  doesn't surface as a separate signal. Engaging it would require a
  different consumer pattern, which would overlap with R4 in B6.
  Skipped honestly; no synthetic stand-in shipped.

- **R6 (parquet count fast path)** — see above (B5 exclusion). Not a
  separable regime in 2.55.

## Working tree pointers

- Skill scratch (Phase 1–3 design notes, calibration evidence,
  excluded benchmarks): `/workspace/ray/skill-runs/read_and_consume_benchmark/`
- Phase 1+2 design table: `/workspace/ray/skill-runs/read_and_consume_benchmark/PHASES.md`
- Phase 3 calibration results: `/workspace/ray/skill-runs/read_and_consume_benchmark/CALIBRATION.md`
- Upstream test: `/workspace/ray/v11_pristine/release/nightly_tests/dataset/read_and_consume_benchmark.py`
- Upstream YAML invocations: `/workspace/ray/v11_pristine/release/release_data_tests.yaml`
