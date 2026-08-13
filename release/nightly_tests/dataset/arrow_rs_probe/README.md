# arrow-rs read probe — Linux + S3 regression reproduction

## TL;DR — the grand experiment (fresh Linux box, one command)

Maps the whole tuning surface of arrow-rs vs PyArrow on the NEW footer-based
planner (#64985 series): environment setup, the two pytest suites as a
correctness gate, 5 local fixture shapes, then sweeps over the new bin-packing
knob, the decode budget, fragment threads, and (with `ARROW_RS_S3_BUCKET` set)
S3 + fetch window. See the docstrings of `grand_experiment.py` /
`gen_local_fixtures.py` for the stage/shape rationale.

```bash
git clone https://github.com/AarryaSaraf/ray.git ~/ray && cd ~/ray
git checkout arrow-rs-on-64985
bash release/nightly_tests/dataset/arrow_rs_probe/run_grand_experiment.sh
# optional S3 stage: export AWS creds + ARROW_RS_S3_BUCKET=s3://... first
# quick smoke run:   FIXTURE_SCALE=0.25 STAGES=A bash .../run_grand_experiment.sh
```

Results: `grand_runs/<ts>/summary.md` (ratio tables; R/P > 1.00 = arrow-rs worse),
`summary.json`, one log per cell.

## Replication matrix — the 2026-08-12 release-A/B trusted signals (TODO 1ab phase 1)

Replicates the multi-node A/B's *trusted* (wall / decode task-s) good/bad list on one
Linux box: `tensors` (item 1y, decode 5.59×), `binsweep` (item 10 — bins from 1 row
group up to 10× a file, plus a PyArrow `pre_buffer=off` arm), **`binbound`** (R2b — is a
read task's USS bounded by the bin budget, or is something retaining?), `write`
(item 1aa), `fatcol` (item 1o), **`oom`** (R5 — the failure-mode demo: same memory
ceiling via Ray's own memory monitor, sweep the bin; PyArrow's arm is *expected* to die
with `OutOfMemoryError` at the big bins while arrow-rs survives all of them — an OOM
there is the stage's result, not a broken run). Stage rationale in
`replication_matrix.py`'s docstring; the predictions each stage falsifies are in
`arrow_rs_docs/TODO.md` items 1ab/10.

`binbound` is the bound/leak check: one bin is one read task, so it runs at
`--task-concurrency 1` (one bin resident per process) with `--mem-poll-s 0.05` (the 1 Hz
default samples a short task once or not at all) and fits per-task USS against **decoded**
bytes per task — slope ≲0.3 flat, ≲1.1 bounded by the bin, >1.1 unbounded ⇒ leak. It
prints the bin→decoded expansion per cell because the knob is spent in
`row_group.total_byte_size` (encoded bytes), measured 1.64× below decoded Arrow on the
fixture — finding C10. **Needs Linux**: per-task USS is `None` on macOS and the fit is
skipped (the task-count and expansion columns still work).

Fresh Linux box, one command (same setup skeleton as the grand experiment):

```bash
git clone https://github.com/AarryaSaraf/ray.git ~/ray && cd ~/ray
git checkout arrow-rs-on-64985
bash release/nightly_tests/dataset/arrow_rs_probe/run_replication.sh
# quick smoke:            FIXTURE_SCALE=0.25 REPEAT=1 bash .../run_replication.sh
# one stage:              ONLY=binsweep bash .../run_replication.sh
# after a git pull that touches the crate: FORCE_SETUP=1 (or you benchmark a stale .so)
```

Piecemeal (env already set up — `source arrow_rs_probe/env.sh` first):

```bash
python gen_local_fixtures.py --root ~/arrow_rs_repl_fixtures \
    --shapes bin_sweep,tensors_wide,fat_col
python replication_matrix.py --fixture-root ~/arrow_rs_repl_fixtures --repeat 3
```

Results: `replication_runs/<ts>/summary.json` + per-cell logs; the summary block
prints R = arrow_rs/pyarrow per cell pair and the pre_buffer on/off deltas.

---

Single-node harness to measure the two cases where the arrow-rs Parquet reader was
worse than PyArrow in the release run (build 102757), so we can optimize them:

| release test | axis | gap | why it needs Linux + S3 |
|---|---|---|---|
| `mix.8ds_equal_random_mix` (imagenet, many tiny row groups) | **time** | 1.67× | I/O-bound on S3 (many small serial range GETs). Faster than PyArrow on local disk at every scale — no network to expose it. |
| `wide_schema_pipeline_primitives` (5000 cols) | **memory** | 1.50× | The crate's page-sized working set only engages on the **S3** decode path; and per-worker USS is Linux-only. |

Both are single-worker read properties, so one node + S3 reproduces them — no cluster.

## Setup (fresh Linux workspace on branch `arrow-rs-parquet-reader-pr`)

```bash
# 1. Repo + a commit-matched Ray nightly wheel (a "latest" wheel drifts from this
#    branch's compiled protobufs and asserts "out of sync" at import).
git clone https://github.com/AarryaSaraf/ray.git ~/ray && cd ~/ray
git checkout arrow-rs-parquet-reader-pr
git remote add upstream https://github.com/ray-project/ray.git && git fetch upstream master --quiet

uv venv --python 3.12 ~/ray/.venv && source ~/ray/.venv/bin/activate
# Pick the x86-64 manylinux nightly matching `git merge-base HEAD upstream/master`,
# install --no-deps, then re-link this branch's source:
uv pip install --no-deps <ray-nightly-wheel-url>
uv pip install "ray[data]" psutil
python python/ray/setup-dev.py -y   # symlinks python/ray/ over the wheel

# 2. Build the native crate for Linux (first time off macOS).
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && source "$HOME/.cargo/env"
uv pip install maturin
cd python/ray/data/_internal/datasource_v2/native/ray_data_arrow_rs
maturin build --release && uv pip install --force-reinstall --no-deps target/wheels/*.whl
cd ~/ray
```

### Anyscale-workspace gotchas (none are arrow-rs-related)
- **Run a private cluster:** `export RAY_ADDRESS=local` — never attach to the managed
  cluster (different Ray version → version-check failure).
- **Re-activate the venv in every shell** (`source ~/ray/.venv/bin/activate`) — otherwise
  `python` is the image's anaconda Ray (the Anyscale runtime), where this branch's
  reader/crate/flags don't exist. Confirm with
  `python -c "import ray.data, os; print(os.path.realpath(ray.data.__file__))"` → must
  resolve into this checkout.
- If `ray.init()` hangs: `unset RAY_RUNTIME_ENV_HOOK RAY_RUNTIME_ENV_PLUGINS` and
  `export RAY_task_events_report_interval_ms=0` (dodges a 2026-07 master task-event
  SIGSEGV); check `/tmp/ray/session_latest/logs/runtime_env_agent.err` for import errors.

## Run

Confirm the S3 prefixes first (they drift): `aws s3 ls s3://ray-benchmark-data-internal-us-west-2/wide_schema/`
and `.../imagenet/`, and export AWS creds/region. Put the box in the bucket's region.

```bash
cd release/nightly_tests/dataset/arrow_rs_probe
export RAY_ADDRESS=local

# (A) CPU-bound-vs-IO diagnostic — force one read task, compare cpu_over_wall.
#     ~1 => CPU-bound decode; <<1 => I/O-waiting on S3.
python read_probe.py --preset imagenet   --reader pyarrow  --concurrency 1
python read_probe.py --preset imagenet   --reader arrow_rs --concurrency 1
python read_probe.py --preset wide_schema --reader pyarrow  --concurrency 1
python read_probe.py --preset wide_schema --reader arrow_rs --concurrency 1

# (B) Realistic memory — let it fan out; compare peak_uss_gb (the metric of record).
python read_probe.py --preset wide_schema --reader pyarrow
python read_probe.py --preset wide_schema --reader arrow_rs

# (C) Allocator A/B on the arrow_rs run (no rebuild) — is any residual mem gap glibc
#     arena retention rather than the decoder?
MALLOC_ARENA_MAX=2 python read_probe.py --preset wide_schema --reader arrow_rs
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 python read_probe.py --preset wide_schema --reader arrow_rs
```

Local scale sweeps (no S3 — separates a scaling effect from the S3/Linux effect; on
Linux they also fill in `peak_uss_gb`):

```bash
python scale_sweep.py imagenet   # bytes sweep: does arrow_rs stay faster as data grows?
python scale_sweep.py wide       # row-group-size sweep: USS ratio vs rg size
```

## What to look for
- **imagenet time:** if `cpu_over_wall` ≪ 1 on the S3 read, the gap is prefetch/fetch —
  the fix is crate-level concurrent column-chunk prefetch within a row group (PyArrow's
  `pre_buffer` issues parallel column GETs; the crate's windowed stream fetches serially),
  or reading N row groups per task concurrently.
- **wide_schema memory:** compare `peak_uss_gb` (and Ray's `read_avg_max_uss_gb`). The
  architectural expectation is arrow-rs **at or below** PyArrow once the S3 windowed path
  engages, growing flat in row-group size while PyArrow grows with it. If arrow-rs is still
  worse on Linux+S3, it's a genuine integration regression to chase there (not the macOS
  allocator/RSS artifact the local sweep shows).

For richer log output on a full release run, `collect_operator_metrics` (in
`../benchmark.py`) now emits per-operator wall/output-bytes/decode-USS.
