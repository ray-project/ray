# REP-64 POC reproducers

One-command repro for every numerical claim in the dossier. All commands are run from the repo root (`/.../ray/`).

Required tooling: Bazel (5.4.1 via `bazelisk`, automatic via Ray's WORKSPACE), GCC 11 or later, Python 3.9+, ~10 GB free disk for the Bazel cache after a cold build.

## Quick sanity sweep — "does the POC actually work on this host?"

```bash
# Builds the smoke test, store-client unit test, concurrency stress, and
# StoreClientTestBase parity test.  Cold build ~10–12 minutes the first
# time, ~10 seconds on subsequent runs.
bazel test --config=ci \
  //:rocksdb_smoke_test \
  //:rocksdb_store_client_test \
  //rep-64-poc/harness/concurrency:concurrency_test \
  //rep-64-poc/harness/store_client_parity:rocksdb_parity_test \
  --test_output=summary
```

Expected: all four PASS, total wall-clock ~3 s after the build.

## Phase 1 — fsync honesty probe

```bash
mkdir -p rep-64-poc/harness/durability/results
python3 rep-64-poc/harness/durability/probe_fsync.py \
  --target-dir $HOME/rep64-fsync-probe \
  --output rep-64-poc/harness/durability/results/$(date +%Y-%m-%d)-$(hostname -s)-ext4.json
```

`output_path`'s JSON has a `verdict` field. Honest substrates report p50 fsync ≥ ~500 µs and verdict `honest`. tmpfs reports p50 < 10 µs and verdict `lying` — that's expected, that's the substrate flagging itself.

## Phase 4 — kill-9 durability harness

```bash
bazel build --config=ci \
  //rep-64-poc/harness/durability/kill9:writer \
  //rep-64-poc/harness/durability/kill9:verifier

# Positive case: production config (sync=true). Expect verdict PASS, exit 0.
python3 rep-64-poc/harness/durability/kill9/run_kill9.py \
  --writer   bazel-bin/rep-64-poc/harness/durability/kill9/writer \
  --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \
  --num-writes 1000 --sync 1 \
  --output rep-64-poc/harness/durability/results/kill9_ext4_sync1.json

# Negative control: ghost-writes. MUST FAIL with exactly 50 acked-but-missing keys.
python3 rep-64-poc/harness/durability/kill9/run_kill9.py \
  --writer   bazel-bin/rep-64-poc/harness/durability/kill9/writer \
  --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \
  --num-writes 1000 --sync 1 --ghost-writes 50 --kill-after-ack 10000 \
  --output rep-64-poc/harness/durability/results/kill9_ext4_ghost.json
```

## Phase 5 — concurrency stress

```bash
bazel test --config=ci //rep-64-poc/harness/concurrency:concurrency_test \
  --test_output=streamed --runs_per_test=10
```

10/10 PASS expected. Wall-clock per run 0.6–1.4 s. Surfaces a real bug if the cf-mutex / column-family-lazy-create race ever regresses.

## Phase 6 — full StoreClient API parity

```bash
bazel test --config=ci //rep-64-poc/harness/store_client_parity:rocksdb_parity_test \
  --test_output=streamed
```

Passes the same `StoreClientTestBase` suite the in-memory and Redis backends pass.

## Phase 7 — side-by-side microbenchmarks

```bash
bazel build --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench
mkdir -p $HOME/.cache/rep64-microbench rep-64-poc/harness/microbench/results

# IMPORTANT: default --db-dir is $HOME/.cache/rep64-microbench, NOT /tmp.
# Most Linux /tmp is tmpfs and lies about fsync; running on tmpfs gives
# numbers ~500x too fast. The binary defaults to a non-tmpfs path.
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --output rep-64-poc/harness/microbench/results/microbench_ext4.json
```

Compare your numbers to `microbench_ext4.json` from this VM (in the same dir). Read latencies should be ~1 µs class on either backend; sync-write latency should be in the 1–10 ms range and track your substrate's fsync probe (Phase 1).

## Phase 8 — storage-layer recovery time

```bash
bazel build --config=ci -c opt //rep-64-poc/harness/recovery:recovery_bench
mkdir -p $HOME/.cache/rep64-recovery rep-64-poc/harness/recovery/results

python3 rep-64-poc/harness/recovery/run_recovery.py \
  --bin bazel-bin/rep-64-poc/harness/recovery/recovery_bench \
  --db-root $HOME/.cache/rep64-recovery \
  --sizes 100,1000,10000 \
  --output rep-64-poc/harness/recovery/results/recovery_ext4.json
```

Expected: sub-100 ms total recovery (open + scan + lookup) at 10,000 entries on a typical Linux ext4.

## Phase 8 — K8s reproducer (pending Ray container image)

See `rep-64-poc/harness/kind/README.md`. The manifest at `rep-64-poc/harness/kind/ray-head-rocksdb.yaml` has an `image:` placeholder pending a Ray container built from this branch (the highest-priority Phase 8 follow-on per the EVIDENCE dossier).

## Phase 8 — cloud-volume sanity (collaborator-driven)

For someone with EBS / GCE PD / equivalent:

```bash
# After cloning this repo onto a cloud VM and building the bazel binaries:
bash rep-64-poc/harness/cloud/cloud_fsync_check.sh /mnt/data
```

The script runs Phase 1 fsync probe + Phase 4 kill-9 (positive + ghost negative control) + Phase 7 microbench + Phase 8 recovery. Copies result JSONs back into the relevant `rep-64-poc/harness/*/results/` dirs for committing.

## Toolchain notes

- **Bazel version:** Ray's `WORKSPACE` resolves to Bazel **5.4.1** via `bazelisk`. Bazel ≥ 7 in strict mode rejects Ray's WORKSPACE-style external repos (`unknown repo 'python3_9'`).
- **CMake / Ninja:** Required by `rules_foreign_cc` for the RocksDB build. Versions ≥ 3.23 / 1.10 are sufficient.
- **Boost mirror:** This branch switches `@boost`'s URL from `boostorg.jfrog.io` (broken) to `archives.boost.io` (canonical). Without that fix, every fresh `@boost` fetch on Ray master fails with a SHA mismatch — that's a Ray-core regression independent of REP-64. See commit `07c65c84`.
- **`-c opt` matters for benchmarks.** Latency numbers from a `dbg`-mode binary include compile artifacts that aren't in production. Phase 7 and 8 reproducers above pass `-c opt` explicitly.
- **Substrate honesty matters for fsync-bound numbers.** A surprising number of Linux mounts default to tmpfs (or to filesystems that ignore `fsync`). Always run the Phase 1 probe on the substrate first; the bench harnesses default to `$HOME/.cache/...` which is usually on the real disk.
