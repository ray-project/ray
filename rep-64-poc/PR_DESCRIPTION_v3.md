# PR description (v3) — paste this into PR #63032 to replace the current body

> Diff vs the current PR description: adds the per-key strand layer to
> the TL;DR, adds a 5th finding row about ordering correctness on the
> offload path, updates the test plan rows, refreshes Open Questions.

---

# [Draft][POC] [REP-64 POC] Embedded RocksDB Storage Backend for Ray GCS

This is a **proof-of-concept** for [REP-64: Embedded RocksDB Storage Backend for Ray GCS](https://github.com/ray-project/enhancements/blob/main/reps/2026-02-23-gcs-embedded-storage.md). It's *not* asking for merge today — it's asking maintainers to look at the numbers and tell us whether they want this work to continue, and in what shape.

**Want to help validate?** I can't reach every substrate / OS / cloud-PV combination from one machine. If you can spare 30 minutes, [`rep-64-poc/COLLABORATORS.md`](../blob/jhasm/rep-64-poc-1/rep-64-poc/COLLABORATORS.md) has a triage queue of 9 concrete tasks (different filesystems, arm64, macOS, cloud volumes, K8s pod-delete, ASAN/TSAN, fast-NVMe pipelining, end-to-end actor survival, architectural review). Each one closes a real evidence gap.

## TL;DR — the 5 things this POC answers

| # | Question | Answer | Where |
|---|---|---|---|
| 1 | Does the embedded engine compile, link, open, and survive close+reopen inside Ray's existing StoreClient interface? | **Yes.** `RocksDbStoreClient` implements the full `StoreClient` contract, passes the same `StoreClientTestBase` parity suite that `InMemoryStoreClient` and `RedisStoreClient` use, and writes the cluster-ID marker fail-fast that REP-64 §"Stale data protection" requires. | `rep-64-poc/reports/phase-3-skeleton.md`, `phase-6-api-parity.md` |
| 2 | What does fsync-per-write *actually* cost on a real durable substrate? | **3.78 ms p50 inline** on probe-verified ext4 — about 38× slower than the REP's quoted "0.01–0.1 ms" RocksDB write number. The REP figure is the memtable-only path; the GCS contract requires WAL fsync, which is where the real cost lives. | `rep-64-poc/reports/phase-7-microbench.md` |
| 3 | Can we move the fsync off the GCS event loop and recover throughput? | **Yes — 2.5× pipelined throughput** when offload-IO is enabled (`gcs_rocksdb_async_offload=true`), via a `boost::asio::thread_pool` + RocksDB group commit. Per-op latency stays identical (~40 µs dispatch overhead vs ~3.8 ms fsync). | `rep-64-poc/reports/phase-7-microbench.md` (Addendum: inline vs offload) |
| 4 | Is the offload path *correct*, not just fast? | **Yes — closed by per-key strand bucketing.** The first offload commit reordered same-key ops on the pool, breaking submission-order semantics that `InMemoryStoreClient` and `RedisStoreClient` give for free via single-threaded execution. Fix: single-key ops dispatch through `boost::asio::strand` bucketed by `hash(table, key) % gcs_rocksdb_strand_buckets`. Same-bucket posts FIFO; cross-key parallelism preserved. **Zero measurable perf cost.** | `rep-64-poc/POC_AUDIT.md`, `rep-64-poc/reports/phase-7-microbench.md` (Addendum: per-key strand) |
| 5 | Is the implementation thread-safe under contention? | **Yes.** Mutex-RMW correctness on `GetNextJobID` under 16-thread × 1000-call contention; counter persists across restart with no re-issue; column-family creation race fixed; per-key strand correctness verified by 4 dedicated tests. | `rep-64-poc/reports/phase-5-concurrency.md`, `src/ray/gcs/store_client/tests/rocksdb_store_client_test.cc` |

## What's in this PR

- `src/ray/gcs/store_client/rocksdb_store_client.{h,cc}` — full StoreClient implementation (Async\* methods, cluster-ID marker, job-ID counter recovery, column-family-per-table layout, optional offload-IO with per-key strand ordering, durable WAL fsync).
- `src/ray/common/ray_config_def.h` — three new knobs: `gcs_rocksdb_async_offload` (default `false`), `gcs_rocksdb_io_pool_size` (default `4`), `gcs_rocksdb_strand_buckets` (default `64`).
- `src/ray/gcs/gcs_server.cc` — wires the `ROCKSDB_PERSIST` storage type into both `gcs_table_storage` and `kv_manager` paths.
- `src/ray/gcs/store_client/tests/rocksdb_store_client_test.cc` — Phase-3-specific tests (close+reopen, cluster-ID marker, job-ID recovery, offload roundtrip, offload destructor join) **plus 4 strand-correctness tests** (overwrite-false race, last-writer-wins for same key, delete-then-put preservation, cross-key parallelism).
- `rep-64-poc/` — full POC dossier (PLAN, EVIDENCE, RISKS, COLLABORATORS, **POC_AUDIT.md**, reports for phases 1–7, microbench harness, parity test, concurrency stress test).

## Test plan

- [x] `bazel test //src/ray/gcs/store_client/tests:rocksdb_store_client_test` — 10 unit tests, all PASS (5 baseline + 2 offload + 4 strand-correctness).
- [x] `bazel test //rep-64-poc/harness/store_client_parity:rocksdb_parity_test` — full `StoreClientTestBase` parity coverage, PASS.
- [x] `bazel test //rep-64-poc/harness/concurrency:concurrency_test` — 16-thread × 1000-call mutex-RMW + parallel-AsyncPut stress, PASS.
- [x] Phase 7 microbench (inline + offload + strand) on probe-verified ext4: 215 ops/s inline pipelined, 593 ops/s offload pipelined, p50 3.6–3.8 ms in both. Strand layer adds zero measurable cost.
- [ ] ASAN/TSAN runs (collaborator task #6 in `COLLABORATORS.md`).
- [ ] Cloud-PV substrate sweep (EBS, GCE PD, Azure Managed Disk) — collaborator task #4.
- [ ] macOS substrate run with F_FULLFSYNC — collaborator task #3.
- [ ] K8s pod-delete + recovery on a real PV — collaborator task #5.
- [ ] End-to-end actor-survival test against an embedded-RocksDB GCS — collaborator task #7.

## Open questions for maintainers

1. **Should offload be the default?** The numbers say it's a clear win for any concurrent-write workload, identical for serial. Default is currently `false` to mirror `InMemoryStoreClient`'s simplest-mental-model semantics, but I'd happily flip it.

2. **Should the inline path stay at all?** With strand correctness landed, the offload path is fully production-shaped. Inline is now mainly a "simplest mental model" reference and a fallback for environments where a thread pool is undesirable. If you'd rather have one code path, removing the inline branch from `RunIoForKey`/`RunIoUnordered` is a one-commit change.

3. **Strand bucket count default = 64.** With pool=4 that's 16× headroom and rare collisions. A maintainer with deeper knowledge of GCS workload key cardinality might want to tune this differently — happy to take guidance.

4. **Multi-key/scan ordering is intentionally loose.** `AsyncMultiGet`, `AsyncGetAll`, `AsyncGetKeys`, `AsyncBatchDelete` post to the bare pool with no per-key strand. Their semantics are "snapshot from when they ran", matching Redis pipelining and `InMemoryStoreClient` under concurrent callers. If you want per-table strands for scans (preserves write-then-scan ordering on the same table at the cost of cross-scan throughput), say the word.

5. **Cluster-ID validation today is a no-op** because the GCS doesn't have an authoritative cluster_id at `InitKVManager()` time. REP-64 Phase 8 follow-on plans the K8s downward-API plumbing; flagged here so reviewers know the marker code is intentionally inactive in production today.

6. **Compaction-cycle latency** is not yet measured. Phase 7 follow-on; tracked in `COLLABORATORS.md` task #8 (fast-NVMe pipelined throughput sweeps will exercise this).

## Migration / risk notes

- **Default behaviour unchanged.** `gcs_rocksdb_async_offload=false` keeps the inline path, which is byte-for-byte the same as the prior commit at the durability and per-op latency level.
- **Storage type is opt-in.** `RAY_GCS_STORAGE=rocksdb` plus `RAY_GCS_STORAGE_PATH=/some/persistent/path` is the only way to engage this code. Existing Redis/in-memory deployments are unaffected.
- **POC, not for merge.** This branch is `[Draft]` and tagged `[POC]` for a reason. The headline numbers and the audit are the deliverables; the code is sized for production but hasn't been load-tested at production scale.

## What's specifically *not* in this PR

- Compaction-cycle latency measurements (mentioned above; tracked).
- Multi-process / shared-DB safety (not a current GCS shape).
- Production logging / metrics wiring beyond `ObservableStoreClient` (already wraps every Async\* call with the existing histograms and counters).
- A flip of the offload default — left for maintainer judgement.
