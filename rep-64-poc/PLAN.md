# POC Plan: Embedded RocksDB Storage Backend for Ray GCS Fault Tolerance

**REP:** `2026-02-23-gcs-embedded-storage.md` (lives in the separate [`ray-project/enhancements`](https://github.com/ray-project/enhancements) repo, not in this tree).
**Branch:** `jhasm/rep-64-poc-1` (this branch — the POC artifact)
**Author:** Santosh Jha <santosh.m.jha@gmail.com>

## Context

Ray's GCS today requires an external Redis instance for fault tolerance. The REP proposes an embedded RocksDB backend instead: GCS writes state locally to a persistent volume; on restart it reads back. The goals motivating LinkedIn are operational simplicity on KubeRay and survival of long PyTorch training jobs across head-pod restarts.

This is a **proof of concept**, not a feature PR. Goal: prove the design is workable, surface ambiguity and risk early, and produce evidence for every claim the REP makes. Because LinkedIn does not run managed Redis and contributors from Google have offered to help, every benchmark must be reproducible by anyone — laptop, CI, or cloud.

The POC's deeper purpose is establishing trust with the Ray OSS community on a first contribution. The artifact must be self-explanatory, honest about its limits, and re-runnable without insider knowledge.

## Goals

- Provide evidence (or counter-evidence) for every concrete claim in the REP.
- Front-load risk so we fail-fast or pivot before sinking weeks into a dead end.
- Side-by-side numbers vs Redis for every performance claim, with reproducible harnesses on bazel / Docker Compose / kind / cloud.
- A CI-ready microbenchmark suite the Ray team can keep running long-term.
- A self-contained dossier reviewers can read without needing the author present.

## Non-goals (explicitly out of scope, listed in final report)

- Production hardening of the `RocksDbStoreClient` (error paths, observability, metrics)
- KubeRay operator changes (PVC lifecycle automation) — we ship a hand-written K8s manifest instead
- High-availability / Active-Standby GCS
- State compaction, TTL, GCS OOM mitigation
- Migrating existing Redis users to RocksDB

## Approach

Risk-driven phasing. Nine phases, each tied to one pivotal risk or one concrete claim from the REP. Each phase produces a 1-page evidence report committed alongside the code. The dossier is the union of those reports plus a reproducer kit.

**Phase boundaries are gates.** At the end of each phase we explicitly record:

1. The claim or risk that phase addressed.
2. The evidence (numbers, test output, code).
3. Whether we proceed, revise, or pivot.
4. What would invalidate this result (sensitivity / counter-evidence).

When the POC is approved by maintainers, the production implementation will land on a fresh branch off `master` and this `rep-64-poc/` directory will be retired (or moved to the enhancements repo as a record).

### Repo layout for the POC artifact

The POC artifact lives entirely under `rep-64-poc/` in this branch (this directory). Code changes touch the existing tree under `src/ray/gcs/store_client/` and `bazel/`.

```
ray/                                    # this repo, branch jhasm/rep-64-poc-1
├── src/ray/gcs/store_client/
│   ├── rocksdb_store_client.{h,cc}     # new — ray-core changes
│   └── test/rocksdb_store_client_test.cc
├── bazel/
│   ├── BUILD.rocksdb                   # new — custom BUILD if BCR doesn't fit
│   └── ray_deps_setup.bzl              # modified — add rocksdb dep
├── BUILD.bazel                         # modified — add cc_library + cc_test
└── rep-64-poc/                         # POC dossier (this directory)
    ├── PLAN.md                         # this file
    ├── README.md                       # entry point — orientation + index
    ├── EVIDENCE.md                     # consolidated numbers + claim table
    ├── RISKS.md                        # live risk register (updated each phase)
    ├── reports/
    │   ├── phase-1-foundations.md
    │   ├── phase-2-bazel.md
    │   ├── phase-3-skeleton.md
    │   ├── phase-4-durability.md
    │   ├── phase-5-concurrency.md
    │   ├── phase-6-api-parity.md
    │   ├── phase-7-microbench.md
    │   ├── phase-8-k8s-cloud.md
    │   └── phase-9-final.md
    ├── harness/
    │   ├── cpp/                        # cc_test microbenchmark target
    │   ├── docker-compose/             # one-command Redis-vs-RocksDB harness
    │   ├── kind/                       # kind reproducer script + manifests
    │   └── cloud/                      # one-shot scripts for EBS / GCE PD
    └── reproducers/
        └── README.md                   # "I'm a reviewer, how do I rerun this?"
```

### Key existing patterns to reuse (do not reinvent)

| Need | Reuse | Location |
|---|---|---|
| Test base for StoreClient | `StoreClientTestBase` | `src/ray/gcs/store_client/test/store_client_test_base.h` |
| Add C++ third-party dep | `auto_http_archive(...)` | `bazel/ray_deps_setup.bzl:86-150` |
| Custom dep BUILD pattern | `bazel/BUILD.redis` style | `bazel/BUILD.redis` |
| Stand up Redis in tests | `TestSetupUtil::StartUpRedisServers` | `src/ray/common/test_util.h:113-132` |
| Backend selection switch | `GcsServer::GetStorageType()` + `InitKVManager()` | `src/ray/gcs/gcs_server/gcs_server.cc:526-586` |
| Storage-type enum values | `kInMemoryStorage`, `kRedisStorage` constants | `src/ray/common/ray_config_def.h` (extend with `kRocksDbStorage`) |
| Recovery driver | `GcsInitData::AsyncLoad()` (already backend-agnostic) | `src/ray/gcs/gcs_server/gcs_init_data.{h,cc}` |
| Nightly perf test harness | `release/release_tests.yaml` + `release/microbenchmark/` | `release/` |
| C++ test pipeline | `bazel test --config=ci //src/...` | `.buildkite/pipeline.build_cpp.yml` |

## Phase plan

Each phase below lists: **claim addressed**, **deliverables**, **evidence**, **pivot trigger**.

### Phase 1 — Foundations & Redis baseline

**Claim addressed:** none yet — we measure the system we are about to compete with.

**Deliverables:**

- `RISKS.md` with the full risk register (~10 risks, each with mitigation/pivot).
- `harness/docker-compose/` with a one-command runner that starts a Redis container and a Python process executing N puts/gets/getalls, emitting `baseline-redis.json`.
- `phase-1-foundations.md` recording baseline numbers on at least two environments (local laptop, one cloud VM with EBS).

**Evidence:** numbers we will compare every later phase against.

**Pivot trigger:** none. This is foundational.

**Effort:** ~3 days.

---

### Phase 2 — Bazel + RocksDB build integration

**Claim addressed:** "RocksDB is available in BCR (`bazel_dep(name = "rocksdb")`) and is widely used."

**Deliverables:**

- RocksDB wired into Ray's Bazel build via `auto_http_archive` + custom `bazel/BUILD.rocksdb` (Ray is on legacy WORKSPACE, not bzlmod; BCR cannot be used directly).
- A trivial cc_binary in `src/ray/gcs/store_client/test/` that opens a RocksDB, writes a key, reads it back. Builds clean with `--config=ci` and `--config=asan-clang`.
- `phase-2-bazel.md` documents transitive deps pulled in (snappy, lz4, zstd), their licenses, and binary size delta on the head ray binary.

**Evidence:** `bazel build //...` succeeds; binary delta numbers; license report.

**Pivot trigger:** if RocksDB cannot be cleanly wired into Ray's WORKSPACE without invasive third-party patching, escalate to maintainers — possibly pivot to SQLite (the runner-up in the REP's selection analysis) which has a single-file build.

**What would invalidate this result:** ASAN/TSAN warnings from the trivial binary; large unexpected binary growth (>50 MB) that would push the head pod over its memory budget.

**Effort:** ~3 days.

---

### Phase 3 — Walking skeleton: minimal RocksDbStoreClient + GCS integration

**Claim addressed:** the `StoreClient` interface fits RocksDB cleanly enough that the GCS recovery model is identical to today.

**Deliverables:**

- `RocksDbStoreClient` implementing only the methods needed for a basic recovery test: `AsyncPut`, `AsyncGet`, `GetNextJobID` (mutex-guarded RMW for now). Other methods stub out with `RAY_CHECK(false)` so unimplemented paths fail loudly.
- `GcsServer::GetStorageType()` learns a third value `STORAGE_ROCKSDB` keyed off `RAY_GCS_STORAGE=rocksdb` + `RAY_GCS_STORAGE_PATH=...`. New `kRocksDbStorage` constant added.
- One end-to-end integration test: start a single-node Ray cluster with `RAY_GCS_STORAGE=rocksdb`, create a detached actor, kill the GCS process, restart, verify the actor is recovered.
- `phase-3-skeleton.md` includes a sequence diagram of the actual recovery path (compared to the REP's diagram) and notes any places the API didn't fit cleanly.

**Evidence:** integration test passes; recovery is observable.

**Pivot trigger:** if a `StoreClient` method has semantics RocksDB cannot satisfy without invasive workarounds (e.g., callback-on-event-loop ordering inconsistent with RocksDB's I/O model), revisit interface or pivot to LMDB.

**What would invalidate this result:** flaky test (passes 9/10 times) — that's a signal the recovery path has timing dependencies we don't understand.

**Effort:** ~5 days.

---

### Phase 4 — Durability proof under crashes

**Claim addressed:** "a caller observes an ack only after the write is durable on disk … fsync-on-write is non-negotiable."

**Deliverables:**

- A reproducible kill-9 harness: a process that issues N writes and records the last ack ID it received. A separate watcher kills the writer at random points. After restart, a verifier checks that every acked ID is present in RocksDB.
- Run on four substrates: tmpfs (control — should fail), ext4-on-SSD (laptop), NFS loopback mount (K8s analogue), one cloud volume (EBS or GCE PD, manual).
- `phase-4-durability.md` reports the loss rate per substrate. Includes a section on `fsync` semantics on each substrate (e.g., NFS clients buffering, FUSE pass-through).

**Evidence:** zero acked-but-lost writes on every substrate except tmpfs (where loss is expected and demonstrates the test detects faults).

**Pivot trigger:** if NFS or any cloud PV silently violates fsync, revisit: either constrain supported PV types (the REP already calls this out as a caveat) or use sync writes + fdatasync explicitly + barrier IO.

**What would invalidate this result:** test sensitivity is too low — kill-9 too late means we miss races. Mitigate with `SIGSTOP`-then-`SIGKILL` injection at varying offsets in the write path.

**Effort:** ~5 days. Highest-risk phase.

---

### Phase 5 — Concurrency: atomic GetNextJobID and parallel writes

**Claim addressed:** "Option 1: Read-modify-write under a mutex (simple, GCS is single-process)."

**Deliverables:**

- Implement `GetNextJobID` two ways: (a) mutex RMW on a reserved key, (b) RocksDB Merge operator with uint64 addition. Same test exercises both.
- Concurrency test: 10K concurrent calls, verify no duplicates and no skips. Run under TSAN and ASAN.
- Parallel-write stress test: many writers to many tables, verify per-key linearizability (later acks reflect later state on read-back after restart).
- `phase-5-concurrency.md` recommends one of the two approaches with reasoning, including the case where Ray someday spawns multiple GCS-internal threads.

**Evidence:** TSAN/ASAN clean; no duplicates over 1M iterations; parallel writes maintain ordering.

**Pivot trigger:** Merge operator has subtle correctness issues (e.g., compaction merges silently produce wrong results); fall back to mutex RMW unconditionally.

**Effort:** ~3 days.

---

### Phase 6 — API completeness + StoreClientTestBase parity

**Claim addressed:** "RocksDbStoreClient passes all existing StoreClient test cases."

**Deliverables:**

- All remaining methods implemented: `AsyncGetAll`, `AsyncMultiGet`, `AsyncDelete`, `AsyncBatchDelete`, `AsyncGetKeys` (prefix scan via RocksDB iterator), `AsyncExists`.
- `rocksdb_store_client_test.cc` extending `StoreClientTestBase` (subclass pattern, same as `redis_store_client_test.cc`). All inherited tests pass.
- All GCS integration tests under `src/ray/gcs/gcs_server/test/` pass with `RAY_GCS_STORAGE=rocksdb` set.
- A `chaos_rocksdb_store_client_test.cc` mirroring the existing chaos test (env-driven failure injection on disk operations).
- `phase-6-api-parity.md` lists each method, the RocksDB primitive used, and any deviations in semantics.

**Evidence:** test suite runs green on Linux x86_64 with `--config=ci`, `--config=asan-clang`, `--config=tsan` (if Ray has one).

**Pivot trigger:** none — by this point the architecture is settled.

**Effort:** ~5 days.

---

### Phase 7 — Side-by-side microbenchmarks

**Claim addressed:** the REP's "Performance Characteristics (Expected)" table — write 0.5–2ms (Redis) vs 0.01–0.1ms (RocksDB), read same range, recovery faster on RocksDB.

**Deliverables:**

- C++ microbenchmark as a `cc_test` target (Ray's existing benchmark convention): write latency p50/p99, read latency, AsyncGetAll throughput, AsyncGetKeys prefix-scan throughput. Runs the same workload against InMemory, Redis, and RocksDB. Emits structured JSON to `TEST_OUTPUT_JSON`. This is the per-commit / fast-feedback layer.
- Docker Compose harness (`harness/docker-compose/`) running the same workload at the Python level, against a Redis container vs a RocksDB-on-volume head pod. One-command repro for laptop reviewers.
- A Python wrapper around the Docker Compose harness suitable for `release_tests.yaml` (the cluster-scale nightly tier — separate from the cc_test). Schema and threshold-assertion docs included so the Ray team can decide where to land it.
- `phase-7-microbench.md` reports numbers with confidence intervals, hardware spec, OS, kernel, filesystem, and a "what would invalidate these numbers" section (cache warm-up, NUMA effects, sync vs async storage stack).

**Evidence:** numbers vs the REP's claimed ranges; explicit pass/fail vs the claims.

**Pivot trigger:** if RocksDB is materially slower than the REP claims (e.g., write p99 > 1ms on SSD), profile and tune; if no tuning recovers parity, the REP's performance argument needs revision.

**What would invalidate this result:** measurement on warm OS page cache that wouldn't reflect cold-recovery; mitigate with explicit `drop_caches` between phases.

**Effort:** ~5 days.

---

### Phase 8 — Recovery time + K8s/cloud validation

**Claim addressed:** "Recovery time is equal to or better than Redis-based FT" + the K8s deployment story (PVC re-attach, head-pod restart) + cloud fsync sanity.

**Deliverables:**

- Python harness that spawns a Ray cluster, populates state (configurable: 1k actors, 100 placement groups, 10k jobs), kills GCS, and times the recovery. Run side-by-side against Redis and RocksDB.
- Hand-written K8s manifests (`harness/kind/`): a StatefulSet for the Ray head with a PVC mounted at `/data/gcs-state`, `RAY_GCS_STORAGE=rocksdb` env var, and a one-line `make kind-up` reproducer. Same harness produces a Redis-backed comparison setup.
- Cloud fsync sanity script (`harness/cloud/`): one-shot scripts targeting EBS and GCE PD that validate the durability claim on real cloud block storage.
- `phase-8-k8s-cloud.md` reports recovery times across substrates and includes a "head-pod failure modes table" mapping each REP-claimed failure mode to whether we observed correct recovery.

**Evidence:** recovery time numbers; manifest-driven recovery actually works on kind; cloud script exits 0.

**Pivot trigger:** if recovery time is materially slower than Redis (the REP claims faster), investigate whether RocksDB cold-open is the culprit — possibly mitigate with `max_open_files` or warm-up reads.

**Effort:** ~5 days.

---

### Phase 9 — Final dossier + production-hardening backlog

**Claim addressed:** coherence of the whole.

**Deliverables:**

- `EVIDENCE.md` — a single-page table mapping each REP claim to the phase that tested it and the verdict (verified / partially verified / contradicted).
- `README.md` rewritten as the orientation document for reviewers: how to navigate, how to reproduce, what's in scope and out.
- `reproducers/README.md` — explicit step-by-step for each layer (bazel / docker-compose / kind / cloud), with expected outputs and rough wall-clock times.
- `RISKS.md` finalized with what's still open.
- A "production hardening backlog" section listing exactly what is NOT done: comprehensive error paths, observability/metrics, KubeRay operator automation, state compaction/TTL, HA, doc updates in `doc/source/`. Each with a one-line rationale for why it's deferred.

**Evidence:** the dossier itself.

**Effort:** ~3 days.

---

## Cross-cutting evidence model (per phase report shape)

Every `reports/phase-N-*.md` follows the same template:

1. **Claim addressed** — verbatim from the REP, or "n/a (foundational)".
2. **Method** — what we built, what we measured, parameters.
3. **Result** — numbers, table, terse summary.
4. **Skepticism** — what could invalidate this; what we did to control it; what residual uncertainty remains.
5. **Reproducer** — one-line command + expected output.
6. **Pivot decision** — proceed / revise / pivot, with reasoning.

This template is the load-bearing thing. It forces us to be honest in writing about every phase.

## Reproducibility kit (cross-phase)

Every benchmark must work with one command on each layer:

- **bazel:** `bazel test //rep-64-poc/harness/cpp:storage_microbench --test_output=streamed`
- **docker-compose:** `cd rep-64-poc/harness/docker-compose && ./run.sh redis|rocksdb`
- **kind:** `cd rep-64-poc/harness/kind && ./up.sh && ./bench.sh`
- **cloud:** `cd rep-64-poc/harness/cloud && ./fsync-sanity.sh ebs|gcepd` (manual, one-shot — not for CI)

All emit JSON. A small `harness/compare.py` produces a side-by-side table from any pair of result files.

## CI-ready harness for the Ray team

The bazel-layer microbench is structured so it can be added to nightly CI without us being involved:

- `cc_test` target tagged `team:core` (matches existing convention).
- `release_tests.yaml` entry under `core-daily-test`.
- JSON output schema documented in `harness/cpp/README.md`.
- Threshold assertions defined separately from the benchmark itself, so the Ray team can adjust thresholds without rewriting the test.

## Open decisions (deferred to first time the choice matters)

- Custom merge operator vs mutex-RMW for `GetNextJobID` — Phase 5 chooses with evidence.
- Column families vs key-prefix data model — start with column families (REP recommendation); revisit only if Phase 6 finds an API-fit problem.
- Whether to expose `RAY_GCS_ROCKSDB_WAL_DIR` in the POC — defer; not on the critical path for proving the design.
- Cluster-ID stale-data marker (REP §"Stale data protection") — implement minimally in Phase 3 (write a marker on first open, fail-fast on mismatch); full design in production hardening.

## Verification (how we know the POC succeeded)

End-to-end checks at the close of Phase 9:

1. `bazel test //src/ray/gcs/store_client/test:rocksdb_store_client_test` passes.
2. `bazel test --config=asan-clang //src/ray/gcs/store_client/test:rocksdb_store_client_test` passes.
3. The full GCS integration suite passes with `RAY_GCS_STORAGE=rocksdb`.
4. `rep-64-poc/harness/docker-compose/run.sh rocksdb` produces a JSON file with all three claim-relevant metrics (write p99, read p99, recovery time).
5. `rep-64-poc/harness/kind/up.sh && rep-64-poc/harness/kind/bench.sh` produces the same JSON shape with comparable numbers.
6. `EVIDENCE.md` contains a row per REP claim, each with status (verified / partial / contradicted) and a link to the supporting phase report.
7. A reviewer who has never seen this work can follow `reproducers/README.md` and reproduce at least the bazel and docker-compose layers in under one hour.

## Total estimated effort

Roughly 7–8 weeks at 1.0 FTE, with Phase 4 (durability) and Phase 7 (microbench) as the two highest-risk slots. Phases are sequential except Phase 1 baseline measurement can start in parallel with Phase 2 (independent work).
