# Phase 7 — Side-by-side microbenchmarks

**Status:** C++ microbenchmark shipped, run on this VM's honest-ext4 substrate. Numbers contradict the REP's "0.01–0.1 ms RocksDB write" claim and reframe it as a memtable-only number rather than a durable-write number. Headline result: RocksDB sync-on-write p50 = **3.81 ms** on probe-verified-honest ext4, consistent with the Phase 1 fsync probe.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> REP "Performance Characteristics (Expected)" table — write 0.5–2 ms (Redis) vs 0.01–0.1 ms (RocksDB), read same range.

This phase measures real numbers, head-to-head, to falsify or confirm the REP's positioning.

## Method

`rep-64-poc/harness/microbench/storage_microbench.cc` — a `cc_binary` (not a `cc_test`, so the run is explicit). Same-process side-by-side comparison of `InMemoryStoreClient` vs `RocksDbStoreClient`:

- 10,000 sequential `AsyncPut(table, key_i, value_i, overwrite=true, callback)` calls per backend.
- 10,000 sequential `AsyncGet(table, random_key)` calls (random key drawn from the inserted set; deterministic seed).
- One `AsyncGetAll(table)` returning all entries.
- One `AsyncGetKeys(table, "k1")` prefix scan returning ~1,111 keys.

Each per-op wall-clock is sampled (first 10,000 ops); the binary computes mean / p50 / p95 / p99 / min / max and emits a structured JSON report. A warm-up `AsyncPut` precedes the measurement to avoid the column-family-create cost biasing the first sample.

The binary defaults the RocksDB working directory to `$HOME/.cache/rep64-microbench/...` rather than `$TMPDIR`. **This is load-bearing:** on most Linux systems `/tmp` is `tmpfs`, which lies about `fsync` (returns immediately because the data is RAM-backed). A first attempt that defaulted to `/tmp` produced "RocksDB Put p50 = 7 µs" — about 500× too fast — for exactly that reason. Phase 1's probe verified `/home` on `/dev/sda3` is honest ext4 (fsync p50 = 3.6 ms).

Build:

```bash
bazel build --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench
```

Run:

```bash
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --output rep-64-poc/harness/microbench/results/microbench_ext4.json
```

`-c opt` matters — the numbers from a `dbg`-mode binary include compile-time inlining and bounds-checking artifacts that aren't in production.

## Result

`rep-64-poc/harness/microbench/results/microbench_ext4.json`:

### AsyncPut latency (per-op wall-clock)

| Backend | Mean | p50 | p95 | p99 | Max | Aggregate (10k ops) | Throughput |
|---|---|---|---|---|---|---|---|
| `InMemory` | 1.11 µs | 0.82 µs | 2.42 µs | 4.03 µs | 527 µs | 13.5 ms | **740 k ops/s** |
| `RocksDB` (sync=true, ext4) | **4.80 ms** | **3.81 ms** | 9.01 ms | 13.33 ms | 47.17 ms | **48.03 s** | **208 ops/s** |

### AsyncGet latency (per-op wall-clock, random keys, warm cache)

| Backend | Mean | p50 | p95 | p99 | Max | Aggregate (10k ops) | Throughput |
|---|---|---|---|---|---|---|---|
| `InMemory` | 0.94 µs | 0.82 µs | 1.54 µs | 3.48 µs | 16 µs | 11.1 ms | **898 k ops/s** |
| `RocksDB` | 1.36 µs | 0.97 µs | 3.29 µs | 3.96 µs | 17 µs | 15.3 ms | **652 k ops/s** |

### Bulk-read aggregates

| Op | InMemory | RocksDB |
|---|---|---|
| `AsyncGetAll` over 10,001 entries | 2.81 ms | 3.58 ms |
| `AsyncGetKeys("k1")` returning 1,111 keys | 1.35 ms | 1.18 ms |

### What these numbers actually say

**Reads are essentially indistinguishable.** RocksDB's block cache + memtable means random `Get` p50 is within ~150 ns of `InMemory`'s in-process hashmap lookup. `AsyncGetAll` is 27% slower on RocksDB; `AsyncGetKeys` is *faster* on RocksDB (the iterator stops on first prefix mismatch; the in-memory implementation does an unordered scan). For GCS workloads dominated by reads, RocksDB is competitive.

**Writes are bounded by fsync, which is exactly what we want.** RocksDB's `Put` p50 of 3.81 ms tracks Phase 1's fsync p50 of 3.61 ms within 6%. Per-op overhead (memtable insert, WAL append, callback dispatch) is therefore <200 µs; the rest is the WAL fsync, which is the durability guarantee. The 9–13 ms p95/p99 reflect occasional fsync stalls on this VM's ext4 (the same kind of tail Phase 1's probe captured).

**The REP's "0.01–0.1 ms RocksDB" claim is misleading without context.** Those numbers are achievable for memtable-only writes (`WriteOptions::sync = false`), or for reads, or on top-tier NVMe with sub-100-µs fsync. On a typical K8s persistent volume (EBS / GCE PD class), expect numbers closer to ours on ext4 — and the production write path requires `sync = true` for the durability claim to hold. **The REP should be updated to state writes as 1–10 ms class (sync, fsync-bound on commodity PVs), 0.01–0.1 ms class (reads / memtable-only writes).**

**Single-threaded RocksDB sync write throughput of 208 ops/s is enough for GCS.** GCS workload is mostly reads (cluster state queries, actor lookups) with bursts of writes during cluster events (worker registration, actor lifecycle changes). A recovery scenario inserting 1,000 actors at start-up takes ~5 s sequentially; with group-commit batching across multiple producer threads (Phase 5's `ParallelAsyncPutAllSurviveWithCorrectValues` shows the parallel write path works), real recovery time is bounded by RocksDB's batched fsync rate, not 208 × N.

### Comparison to REP positioning

| REP claim | Phase 7 measurement | Verdict |
|---|---|---|
| RocksDB write 0.01–0.1 ms | 3.81 ms p50 (sync, ext4) | **Contradicted for sync-on-write.** Holds only for memtable-only writes. |
| RocksDB read 0.01–0.1 ms | 0.97 µs p50 | **Confirmed.** Reads are sub-µs on warm cache. |
| RocksDB recovery faster than Redis | Not measured here (Phase 8) | Pending. |
| Redis write 0.5–2 ms | Not measured (no Redis in this run) | Open. |

## Skepticism

### Methodology caveats

- **Single-threaded writer.** RocksDB's WAL group commit batches multiple in-flight writes into one fsync. A multi-threaded writer benchmark would show higher aggregate throughput (and lower per-op latency in some configurations) than the 208 ops/s single-thread number. Phase 5's concurrency tests demonstrate parallel writes are correct; Phase 7 v2 should add a multi-threaded-writer variant to capture group-commit benefit.
- **Random read with warm cache.** The `AsyncGet` pattern is random-over-insert-set, but all 10k keys fit in the OS page cache after the writer phase. A cold-cache read benchmark (forced page-cache drop, fresh process) would surface SSD read latency rather than cache hit latency. Requires `sudo` to `drop_caches`, blocked on this VM.
- **Single substrate.** Only ext4 on `/dev/sda3` measured. The same harness should run on cloud PVs (EBS / GCE PD), NFS loopback, and macOS APFS. Each will have different fsync characteristics.
- **Redis omitted.** A like-for-like Redis comparison needs a Redis container plus careful network-vs-local accounting, which is properly the Docker Compose harness in the PLAN's release-test tier. The cc_binary layer here is for fast feedback on RocksDB-side numbers; cross-backend comparison sits in the Python harness Phase 7 PLAN flagged as "for `release_tests.yaml`".
- **No confidence intervals.** Reporting p50/p95/p99 from a single run is suggestive, not statistically rigorous. A robust report would loop the run 10–30 times and compute CI bounds. Easy to add via a wrapper shell loop; deferred to Phase 7 v2.
- **Compaction not exercised.** 10k keys × ~30-byte values stays in the memtable. Real GCS workloads will eventually trigger memtable flush + compaction, which has its own latency profile. The PLAN's compaction tuning sweep belongs here too.

### What would invalidate this result

- A run on a properly-tooled host showing fundamentally different numbers (RocksDB p50 of <1 ms on the same ext4-class substrate would mean fsync was not honored, and we'd need to revisit the substrate-honesty argument).
- A multi-threaded-writer benchmark that *doesn't* show group-commit benefit; that would mean the throughput estimate above is wrong and would force revisiting GCS-recovery-time claims.
- Real GCS workload traces showing the read/write ratio is heavier on writes than assumed (i.e. write-bottlenecked rather than fsync-bottlenecked).

### What R-register status changes

- **R6 (RocksDB sync-write latency materially worse than REP claims).** Updated: **the REP's claim is materially off for sync writes**, and Phase 7 explicitly documents the 3.81 ms p50 number that should replace it. The pivot trigger from the PLAN ("if RocksDB is materially slower than the REP claims … profile and tune; if no tuning recovers parity, the REP's performance argument needs revision") triggers the second clause: **the REP's performance argument needs revision** to distinguish memtable-only-write numbers from durable-write numbers.
- **R11 (benchmarks measure something other than what we think).** This phase already caught one such mismeasurement (tmpfs masking fsync). Document the methodology explicitly so future iterations don't fall into the same trap.

## Reproducer

```bash
# Build the opt-mode binary.
bazel build --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench

# Run on the default honest-ext4 substrate ($HOME/.cache/rep64-microbench).
mkdir -p $HOME/.cache/rep64-microbench
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --output rep-64-poc/harness/microbench/results/microbench_ext4.json

# Cross-check on tmpfs to demonstrate the "lying substrate" finding.
# Expect RocksDB Put p50 ~10 µs (fsync is a no-op on tmpfs).
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --db-dir /tmp \
  --output rep-64-poc/harness/microbench/results/microbench_tmpfs.json
```

The two-substrate comparison is itself a methodology-skepticism artifact: anyone re-running the bench on `/tmp` and getting "0.007 ms RocksDB Put p50" should immediately suspect their substrate.

## Pivot decision

**Proceed with caveats raised to maintainers.** RocksDB write numbers on commodity-PV-class substrates are 1–10 ms (fsync-bounded), not 0.01–0.1 ms. This is fast enough for GCS workloads (reads dominate; writes batch via group commit), but **the REP's perf section needs the correction** documented above. Reads on RocksDB are competitive with the in-memory backend.

## Next concrete actions

1. **Multi-threaded-writer variant** to capture group-commit benefit on aggregate throughput.
2. **Cold-cache read benchmark** (drop page cache before the read phase). Needs `sudo` access.
3. **Cloud-PV substrate run** (EBS / GCE PD) via collaborator. Same binary, same harness, different `--db-dir`.
4. **macOS run with F_FULLFSYNC** on the user's MacBook. F_FULLFSYNC requires extra plumbing (RocksDB `WriteOptions::sync` doesn't issue it on macOS by default); track as part of Phase 1's substrate diversity work.
5. **Docker Compose harness** for the release-test tier with a real Redis container side-by-side.
6. **Compaction-cycle benchmark** — load 10× the memtable size and measure latency through the compaction trigger.
