# Risk Register — REP-64 POC

Living document. Updated at every phase boundary (Section "Status log" at the end).

Each risk has: **what could go wrong**, **why it matters**, **mitigation** (what we do to keep it from happening), **pivot trigger** (the observation that would force us to rethink rather than just retry), and **phase** (where it gets settled).

## Summary

| # | Risk | Phase | Status |
|---|---|---|---|
| R1 | RocksDB doesn't integrate cleanly into Ray's Bazel WORKSPACE | 2 | **closed (ext4/Bazel 5.4.1/GCC 11)** — phase-2 verified |
| R2 | C++ toolchain mismatch — RocksDB needs newer compiler than Ray's CI baseline | 2 | **closed (GCC 11)** — phase-2 verified; `--config=asan-clang` still pending |
| R3 | Binary size / memory overhead from RocksDB pushes head pod over budget | 2, 7 | **partially closed** — gcs_server with rocksdb 32 MB unstripped / 24 MB stripped, well under PLAN's 50 MB pivot trigger; memory overhead still pending Phase 7 |
| R4 | `fsync` semantics on K8s persistent volumes silently violate the durability contract | 4 | **partially closed (ext4)** — phase-4 kill-9 harness PASS on probe-verified honest ext4; K8s PV substrates pending Phase 8 |
| R5 | Atomic `GetNextJobID` correctness under concurrency (Merge operator edge cases) | 5 | open |
| R6 | RocksDB write latency with `WriteOptions::sync=true` is materially worse than REP claims | 7 | open — provisional data: ~1 ms/write at app layer (Phase 4 corollary), Phase 7 owns rigorous numbers |
| R7 | Recovery time scales poorly — cold RocksDB open is slow | 8 | open |
| R8 | Stale-data protection on shared / re-used PVCs missing or wrong → silent corruption | 3, 8 | **partially closed (storage layer)** — marker mechanism unit-tested; PVC-mismatch fail-fast deferred to Phase 8 (needs external cluster_id source) |
| R9 | REP doesn't get maintainer alignment, making the POC moot | meta | open |
| R10 | Reproducer environment drift (image tags, kind versions, kernel) over POC's lifetime | all | open |
| R11 | Benchmarks measure something other than what we think (warm caches, NUMA, sync stack) | 7 | open |

---

## R1 — Bazel / dependency integration

**What could go wrong:** Ray uses legacy `WORKSPACE` (not bzlmod), so the BCR `bazel_dep(name = "rocksdb")` form referenced in the REP is not directly usable. RocksDB has transitive deps (snappy, lz4, zstd) that may collide or require custom BUILD files. Patching may be invasive enough that maintainers reject the build change on its own.

**Why it matters:** This is the earliest gate. If we cannot build it cleanly, the rest of the POC is moot.

**Mitigation:** Phase 2 follows Ray's existing `auto_http_archive` + custom `bazel/BUILD.<name>` pattern (same as Redis, hiredis, spdlog). Document each transitive dep, its license, and whether Ray already vendors it.

**Pivot trigger:** integration requires patching RocksDB itself or vendoring multiple competing copies of a transitive dep. → Pivot to SQLite (REP's runner-up: single C file, public domain, no transitive deps).

**Phase:** 2.

## R2 — C++ toolchain mismatch

**What could go wrong:** Recent RocksDB releases require C++20 / GCC ≥ 11. Ray's CI may be on an older baseline. Forcing a toolchain bump is an out-of-band change that could be rejected.

**Why it matters:** Same blast radius as R1 — blocks every later phase.

**Mitigation:** Phase 2 first checks Ray's current toolchain (`bazel/ray_deps_setup.bzl`, `.bazelrc`, CI logs). If newer-compiler-required, pin to the latest RocksDB release that still builds on Ray's current toolchain rather than bumping Ray.

**Pivot trigger:** No RocksDB version both supports our build and has the features we need (column families + Merge operator + WriteOptions::sync). → Either propose a coordinated toolchain bump (separate PR) or pivot to SQLite.

**Phase:** 2.

## R3 — Binary size and steady-state memory overhead

**What could go wrong:** RocksDB + transitive compression libs add tens of MBs to the head binary; plus block cache and write buffers add ~20–50 MB resident at runtime. The REP acknowledges this as "modestly worse, not better" — but on memory-constrained head pods, even 50 MB matters.

**Why it matters:** A correct, fast embedded backend that pushes head pods into OOMKilled is not adoptable.

**Mitigation:** Phase 2 reports binary delta. Phase 7 captures RSS over time during steady-state ops. Build configuration disables features we don't need (e.g., RocksDB's tools, Ldb shell, jemalloc integration) to keep size down.

**Pivot trigger:** binary growth > 50 MB or steady-state RSS overhead > 100 MB after configuration tuning. → Reconsider feature subset or pivot to SQLite.

**Phase:** 2 (size), 7 (memory).

## R4 — fsync semantics on K8s persistent volumes

**What could go wrong:** The REP's whole durability story rests on `WriteOptions::sync = true` translating to a real `fsync` that survives a kernel-level crash. On NFS-backed volumes, FUSE pass-throughs, or some cloud CSI drivers, `fsync` can be a no-op or be batched in a way that violates the contract.

**Why it matters:** This is the one place the REP could be silently wrong. A backend that *says* it's durable and isn't is worse than a backend that admits it isn't durable.

**Mitigation:** Phase 4 builds a kill-9 harness and runs it on tmpfs (control — should fail), ext4 SSD, NFS loopback, and one cloud volume. Reports loss rate per substrate. Pre-reads `man fsync` semantics for each filesystem before claiming results.

**Pivot trigger:** any non-tmpfs substrate shows loss after kill-9 of acked writes. → Either constrain supported PV types in the REP (NFS / FUSE excluded) or layer additional barriers (sync_file_range + fdatasync). Document explicitly.

**Phase:** 4.

## R5 — Atomic `GetNextJobID` under concurrency

**What could go wrong:** Two implementations are plausible (mutex RMW vs RocksDB Merge operator). The Merge operator path has subtle correctness traps — partial merges during compaction, ordering with iterators, behavior on first-open before any merge has run. Using it incorrectly can produce duplicate or skipped IDs and corrupt the job sequence.

**Why it matters:** Job ID collisions cause cascading bugs — actor misrouting, duplicate-job confusion. The REP explicitly acknowledges this is a non-trivial method.

**Mitigation:** Phase 5 implements both, runs 10K-concurrent + 1M-iteration tests under TSAN/ASAN, recommends one approach in writing.

**Pivot trigger:** Merge operator produces any duplicate/skip in 1M iterations. → Mutex RMW only; document why Merge isn't safe here.

**Phase:** 5.

## R6 — Write latency vs REP's claimed range

**What could go wrong:** REP claims RocksDB writes at 0.01–0.1 ms vs Redis at 0.5–2 ms. With `WriteOptions::sync = true` on a real SSD, fsync-per-write is at the slow end of the claim and may exceed it on slower disks.

**Why it matters:** The performance argument in the REP is one of the two main motivations (along with operational simplicity). If performance isn't there, the case weakens.

**Mitigation:** Phase 7 microbenchmark measures p50/p99 against the REP's claim explicitly. Reports include hardware spec (CPU, disk, FS).

**Pivot trigger:** p99 > 1 ms on commodity SSD after tuning (write buffer size, manual_wal_flush=false, bytes_per_sync). → Revise the REP's performance claim before going further; performance argument needs different framing.

**Phase:** 7.

## R7 — Recovery time scales badly

**What could go wrong:** The REP claims "faster than Redis" recovery because reads are local. But cold-opening a RocksDB with many SST files, no warm OS cache, and many column families can be slow. State growth (long-running clusters, accumulated job history) makes this worse.

**Why it matters:** Recovery time is the user-visible metric for "head pod restarted, when can workers reconnect?" Slow recovery is the disaster the REP exists to fix.

**Mitigation:** Phase 8 measures recovery time against state-size sweep (1k / 10k / 100k entries). Compares to Redis baseline. `max_open_files = -1` and warm-up reads as configurable knobs.

**Pivot trigger:** recovery scales worse than O(state size) or is materially slower than Redis at any tested state size. → Investigate column-family open ordering, parallelize loads, or reconsider the data model.

**Phase:** 8.

## R8 — Stale data on re-used PVCs

**What could go wrong:** A PVC accidentally reused with a different cluster (BYO claim, shared NFS subpath) silently loads another cluster's state. GCS comes up "successfully" with wrong actors, wrong nodes, wrong job history.

**Why it matters:** Silent state corruption is the worst possible failure mode — it's invisible until something downstream breaks.

**Mitigation:** Phase 3 implements a minimal cluster-ID marker file written on first open and verified on subsequent opens. Mismatch → `RAY_CHECK` fail-fast at startup. Phase 8 K8s test exercises the wrong-cluster scenario explicitly.

**Pivot trigger:** marker design is racy or unreliable across all the topologies we want to support. → Use a stronger handshake (e.g., signed marker or filesystem-level lock).

**Phase:** 3 (mechanism), 8 (validation).

## R9 — REP doesn't reach maintainer alignment

**What could go wrong:** We invest weeks in a POC, present it, and maintainers prefer SQLite, or want different ergonomics, or veto the dependency. POC is technically sound but operationally moot.

**Why it matters:** The POC's deeper purpose is establishing trust; the trust is also what determines whether the work gets accepted.

**Mitigation:** Engage @jjyao (named in the REP as required reviewer) before the end of Phase 3 with the walking-skeleton evidence. Each phase report explicitly enumerates what would change if a different backend were chosen, so pivot cost is transparent.

**Pivot trigger:** Maintainer feedback indicates structural issue with the design (not "tune this knob" but "wrong abstraction"). → Pause implementation, refresh the design with maintainer input.

**Phase:** ongoing (meta-risk, not phase-bound).

## R10 — Reproducer environment drift

**What could go wrong:** POC takes 7–8 weeks. In that time, Docker images update, kind ships new releases, kernel/glibc on cloud VMs changes. A reviewer running the harness 6 weeks after Phase 1 wrote it sees different numbers.

**Why it matters:** "Truly exceptional" reproducibility is a stated goal. Drift undermines trust more than slightly worse numbers would.

**Mitigation:** All Docker images pinned to immutable digests (not tags). kind version pinned. Cloud scripts assert OS version and exit early on mismatch. Each phase report records exact image digests / versions / kernel.

**Pivot trigger:** none — this is ongoing hygiene. If drift causes a result to no longer reproduce, re-run with current pins and document the diff.

**Phase:** all.

## R11 — Benchmark validity (we measure the wrong thing)

**What could go wrong:** Microbenchmarks measure warm OS page cache, not real cold-recovery. A "fast read" we report may be page cache hit, not RocksDB SST read. NUMA effects on multi-socket boxes inflate numbers. Sync vs async storage stack interact in non-obvious ways.

**Why it matters:** Internally consistent but externally meaningless numbers are how POCs lose credibility.

**Mitigation:** Phase 7 runs each measurement after `echo 3 > /proc/sys/vm/drop_caches` (when permitted) and pins to one CPU socket. "What would invalidate this result" section of every benchmark report calls out the specific noise sources we controlled for and the ones we didn't.

**Pivot trigger:** if a sensitivity analysis shows a single environmental knob (cache, NUMA, sync stack) moves the headline number by > 2x, the result is too fragile to claim. → Change methodology or framing.

**Phase:** 7.

---

## Status log

| Date | Phase | Update |
|---|---|---|
| 2026-04-29 | Phase 1 | Initial register. All 11 risks open. |
| 2026-04-29 | Phase 1 | **R4 corroborated early.** Phase 1 baseline run on the LinkedIn dev VM showed `fsync(4 KiB)` returning in 0.7 μs p50 — the underlying Hyper-V disk is not actually flushing to media. *This entry was based on a probe of `/tmp` (tmpfs), not the actual ext4 substrate. See the 2026-04-30 correction.* |
| 2026-04-30 | Phase 1 | **R4 partially retracted.** A more thorough probe (`harness/durability/probe_fsync.py`) shows the LinkedIn VM's `/tmp` is tmpfs (so the 0.7 μs reading was correct but uninformative — RAM is RAM), while the real ext4 substrate on `/dev/sda3` honours fsync at p50 = 3608 μs ("honest" verdict). Phase 4's durability tests can run here against ext4 paths. Cross-substrate diversity is still desirable but not blocking. |
| 2026-04-29 | Phase 1 | **R10 partially mitigated.** Redis image digest pinned in `harness/docker-compose/docker-compose.yml`. Same pinning needed for kind / cloud harnesses when they're built in Phase 8. |
| 2026-04-30 | Phase 2 | **R1 → likely yes (pending build).** RocksDB 9.11.2 wired into `bazel/ray_deps_setup.bzl` + custom `bazel/BUILD.rocksdb` using the same `auto_http_archive` + `rules_foreign_cc` idiom Ray uses for Redis. SHA verified against upstream. C++ source compiles against real RocksDB headers under GCC 11 / C++17. Full Bazel build still owed (this VM lacks an upstream Bazel + CMake + Ninja). See `reports/phase-2-bazel.md`. |
| 2026-04-30 | Phase 2 | **R2 → likely yes (pending build).** GCC 11.2 + C++17 parses RocksDB headers cleanly. Ray's CI runs a newer compiler, so toolchain mismatch is unlikely. Final close-out on first real build. |
| 2026-04-30 | Phase 3 | **R8 storage-layer half closed.** Cluster-ID marker mechanism implemented in `RocksDbStoreClient`'s constructor: writes a marker on first open, RAY_CHECK-fails on mismatch. C++ unit test exercises the write-and-validate path. Open caveat: cluster-ID timing in `GcsServer` may need rework if `GetClusterId().Hex()` is empty at `InitKVManager` time. Tracked in `reports/phase-3-skeleton.md` "Next concrete actions". |
| 2026-04-30 | Phase 3 | **API-fit risk reduced.** Three of nine `StoreClient` methods (AsyncPut, AsyncGet, GetNextJobID) implemented cleanly using the same callback-on-`main_io_service_` pattern as `InMemoryStoreClient`. No interface changes needed. Remaining six stubbed with RAY_CHECK(false) until Phase 6. |
