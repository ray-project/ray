# REP-64 POC — Evidence Dossier

**Branch:** `jhasm/rep-64-poc-1` &nbsp;|&nbsp; **REP:** `2026-02-23-gcs-embedded-storage.md` &nbsp;|&nbsp; **Author:** Santosh Jha &lt;santosh.m.jha@gmail.com&gt;

This is the maintainer-facing summary. Every row in the claims table below points at the phase report that has the numbers and the reproducer.

## Executive summary

The REP-64 design **works** on the substrate(s) we can test rigorously here, with three findings that warrant explicit attention from maintainers before merge:

1. **The REP's "0.01–0.1 ms RocksDB write" claim is misleading.** Sync-on-write (the durability-honoring config the REP elsewhere requires) is fsync-bounded: **3.81 ms p50** on this VM's probe-verified ext4. The REP's number reflects memtable-only writes. **Ask:** the REP perf section should be split into "writes (sync, fsync-bounded): 1–10 ms class" and "reads / sync=false writes: 0.01–0.1 ms class". (Phase 7.)
2. **The cluster-ID-mismatch fail-fast story (REP "Stale data protection") cannot be implemented as the POC originally drew it.** Ray's GCS init order has `InitKVManager()` running *before* `GetOrGenerateClusterId`, and the persisted cluster ID *is* the cluster ID — there's no external authoritative ID to compare against. This phase's code defers the fail-fast to Phase 8's K8s integration where the K8s downward API can supply an authoritative cluster ID. The deferral is honestly documented and the storage-layer marker plumbing is unit-tested. (Phase 3.)
3. **Two Ray-core regressions surfaced as blockers and were fixed.** Both are independently upstream-worthy and would block any current Ray master build:
   - `boostorg.jfrog.io` no longer serves the `boost_1_81_0.tar.bz2` archive Ray's `WORKSPACE` pinned the SHA for; switched to `archives.boost.io` (canonical, matches the SHA exactly). Commit `07c65c84`.
   - `bazel/BUILD.rocksdb`'s `out_static_libs = ["librocksdb.a"]` assumed install-to-`lib/`, but RocksDB's CMake uses `GNUInstallDirs` which picks `lib64/` on x86_64 Linux. Pinned `CMAKE_INSTALL_LIBDIR=lib` in cache_entries for cross-distro / cross-platform portability. Commit `b043e462`.

The walking-skeleton GCS integration, full StoreClient API surface, durability under SIGKILL, concurrency under N-thread contention, and storage-layer recovery are all verified with green tests. The K8s + cloud-volume validation is **scaffolded but not run** — running it requires a Ray container image built from this branch (the highest-priority Phase 8 follow-on) and cloud-volume access (collaborator-driven).

## Claims-to-evidence table

| # | REP claim (paraphrased) | Phase | Evidence | Verdict |
|---|---|---|---|---|
| 1 | RocksDB integrates into Ray's Bazel build via the same idiom as every other C++ dep, no invasive patching. | 2 | `bazel build //:rocksdb_smoke_test` cold = 229 s, test PASS in 64 ms; binary 8.3 MB. After two surgical fixes (lib path, boost URL) — both upstream-worthy. | **Verified** |
| 2 | RocksDB's static lib does not push gcs_server over its memory budget (PLAN pivot trigger: 50 MB). | 2 | Master `gcs_server` = 23.8 MB unstripped / **16.5 MB stripped**. With RocksDB = 32.0 MB unstripped / **23.1 MB stripped**. **Delta: +8.2 MB unstripped (+34%), +6.6 MB stripped (+40%).** Well under 50 MB. | **Verified** |
| 3 | The `StoreClient` interface fits RocksDB cleanly. | 3, 6 | All 9 methods implemented; `StoreClientTestBase` (the suite InMemory and Redis backends pass) PASSES against RocksDB unmodified in 1.8 s. | **Verified** |
| 4 | State persists across close+reopen at the storage layer (the "head pod restart" scenario at the storage layer). | 3, 8 | `RocksDbStoreClient.RecoverAcrossReopen` test PASS. Storage-layer cold-open + GetAll: 34 ms (100 actors), 81 ms (10k actors). | **Verified** |
| 5 | Ack ⇒ durable. `Put(WriteOptions{sync=true})` returns OK only after the write is recoverable from disk. | 4 | kill-9 harness: 1k and 5k writes, SIGKILL mid-run, 0 acked-but-missing keys on probe-verified honest ext4. Negative control (50 ghost-writes) flagged exactly 50 acked-but-missing — harness has fault sensitivity. | **Verified for ext4. Cloud PVs pending.** |
| 6 | Mutex-RMW `GetNextJobID` is correct under N-thread contention. | 5 | 16 threads × 1000 calls each; result is exactly the contiguous range [1, 16000]; per-thread monotonic; survives close+reopen. 10/10 runs PASS, no flake. Caught + fixed a real TOCTOU race in `GetOrCreateColumnFamily` along the way. | **Verified on GCC 11. TSAN/ASAN pending.** |
| 7 | RocksDB write 0.01–0.1 ms (REP perf table). | 7 | Measured **3.81 ms p50 sync-on-write** on probe-verified ext4. **REP claim contradicted for sync writes.** Holds only for memtable-only / sync=false writes. | **Contradicted — REP needs revision** |
| 8 | RocksDB read 0.01–0.1 ms (REP perf table). | 7 | Measured 0.97 µs p50, 3.96 µs p99. | **Confirmed** |
| 9 | RocksDB recovery time is equal to or better than Redis-based FT. | 8 | Storage-layer cold-open + scan: sub-100 ms at 10k entries. Strictly faster than "wait for Redis container + RPC fetch all state." End-to-end Redis comparison pending Docker Compose harness. | **Provisional yes; head-to-head pending** |
| 10 | Stale-data protection: PVC swap detection via cluster-ID marker. | 3, 8 | Storage-layer marker mechanism unit-tested (writes on first open, validates on subsequent). Fail-fast on mismatch requires an external authoritative cluster_id source (K8s downward API). Deferred from Phase 3 to Phase 8 with the deferral honestly documented. | **Partially verified. K8s plumbing pending.** |

## Substrate matrix

| Substrate | Phase 1 fsync probe | Phase 4 kill-9 sync=1 | Phase 7 microbench | Phase 8 recovery |
|---|---|---|---|---|
| ext4 on `/dev/sda3` (this VM) | **Honest** (p50 = 3608 µs) | **PASS** (1k, 5k writes) | **Captured** (Put p50 3.81 ms, Get p50 0.97 µs) | **Captured** (sub-100 ms cold open at 10k) |
| `/tmp` tmpfs | **Lying** (p50 = 1 µs, expected) | Not run — SIGKILL preserves page cache, tmpfs cannot fail this test. | Used as the "compare-against-honest" cross-check (RocksDB Put p50 = 7 µs, ~500× too fast → flags substrate dishonesty). | n/a |
| macOS APFS (user's MacBook) | Probe ready (`probe_fsync.py` supports `F_FULLFSYNC`) | Pending | Pending | Pending |
| NFS loopback | Pending | Pending | Pending | Pending |
| AWS EBS / GCE PD | Pending — wrapper script `cloud_fsync_check.sh` ready for collaborator | Pending | Pending | Pending |

## Risk register summary

(Live version: `RISKS.md`. Numbered changes are documented in the corresponding phase report.)

| ID | Risk | Final status |
|---|---|---|
| R1 | Bazel WORKSPACE integration | **Closed** (Phase 2) |
| R2 | C++ toolchain mismatch | **Closed for GCC 11** (Phase 2). ASAN/TSAN pending. |
| R3 | Binary size pushes head pod over budget | **Partially closed** (Phase 3, 8) — 23.1 MB stripped (vs 16.5 MB master baseline; +6.6 MB / +40%), well under 50 MB pivot trigger. Memory at runtime pending Phase 7 v2. |
| R4 | fsync semantics on K8s PVs silently violate durability | **Partially closed (ext4)** (Phase 4). Cloud PV substrates pending. |
| R5 | Atomic GetNextJobID correctness under concurrency | **Closed for mutex-RMW on GCC 11** (Phase 5). TSAN/ASAN pending. |
| R6 | RocksDB sync-write latency materially worse than REP claims | **Open — REP claim is materially off**, correction documented in Phase 7. |
| R7 | Recovery time scales poorly | **Closed at the storage layer** (Phase 8). Full GCS-process timing pending. |
| R8 | Stale-data protection on shared/re-used PVCs | **Partially closed (storage layer)** (Phase 3). PVC-mismatch fail-fast deferred to Phase 8 (needs external cluster_id source). |
| R9 | REP doesn't get maintainer alignment | meta — addressed by *this* PR + dossier. |
| R10 | Reproducer environment drift | Mitigated by reproducer scripts + opt-mode pinning. |
| R11 | Benchmarks measure something other than what we think | One real instance caught (tmpfs masking fsync, Phase 7); methodology pinned in the report. |

## Open follow-ups (in priority order)

1. **Build a Ray container image from this branch.** Unblocks: actor-survival end-to-end test, K8s pod-delete recovery timing, full Redis-vs-RocksDB head-to-head.
2. **TSAN / ASAN runs** on a Ray-CI-toolchain host. Same test targets, no code change required.
3. **Cloud-volume substrate sweep** (EBS, GCE PD) via `cloud_fsync_check.sh` + collaborator. Closes R4 fully.
4. **NFS loopback substrate.** Local quick-win before cloud.
5. **`drop_caches`-equipped variants** of Phases 4 and 8 on a host with `sudo`. Closes the OS-level fsync honesty argument.
6. **macOS+F_FULLFSYNC** runs on the user's MacBook for substrate diversity at zero infrastructure cost.
7. **Multi-threaded writer microbench variant** (Phase 7) to capture group-commit benefit on aggregate throughput.
8. **`chaos_rocksdb_store_client_test.cc`** mirroring `chaos_redis_store_client_test.cc`, if maintainers want it before merge.
9. **Cluster-ID-mismatch fail-fast** with K8s downward API plumbing (Phase 8 follow-on).
10. **Compaction-cycle benchmark** (Phase 7 v2) once memtable-overflow workloads are in the harness.

## Production-hardening backlog (out of POC scope)

The PLAN explicitly defers the following. They are real work, but they are not what the POC is asked to prove:

- Comprehensive error paths inside `RocksDbStoreClient` (today most failure modes RAY_CHECK).
- Observability / metrics: per-method latency histograms, fsync stalls, compaction time series.
- KubeRay operator changes for PVC lifecycle automation.
- High-availability / active-standby GCS — out of scope for the embedded backend; the REP itself does not propose HA here.
- State compaction, TTL, GCS OOM mitigation.
- Migration tooling for existing Redis-backed Ray clusters.
- `doc/source/` user docs.

## Pivot-decision log (one line per phase)

| Phase | Decision | Reasoning |
|---|---|---|
| 1 | Proceed | Risk register seeded; Redis baseline harness in place; first-env baseline taken; fsync probe + ext4-honesty correction landed. |
| 2 | Proceed | Bazel + RocksDB integration green after lib-path fix; binary delta within budget. |
| 3 | Proceed (with cluster-ID marker deferral) | Walking skeleton works; bug surfaced + fixed in cluster-ID timing; deferral honestly documented. |
| 4 | Proceed | Ack⇒durable verified on ext4; methodology skepticism finding (page cache + SIGKILL) documented. |
| 5 | Proceed | Mutex-RMW correct under contention; TOCTOU race caught + fixed. |
| 6 | Proceed | API parity demonstrated; no method has fundamentally-RocksDB-hostile semantics. |
| 7 | **Proceed with caveat to maintainers** | Reads competitive; sync-write numbers contradict the REP's table — REP needs revision. |
| 8 | Proceed | Storage-layer recovery sub-100 ms at 10k entries; K8s + cloud scaffolding ready for the next iteration. |
| 9 | This dossier. | The dossier is the artifact. |

---

**Reproducer entry point:** `rep-64-poc/reproducers/README.md`. **Risk register live source:** `rep-64-poc/RISKS.md`. **Phase reports:** `rep-64-poc/reports/phase-N-*.md`.
