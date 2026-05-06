# REP-64 POC: Embedded RocksDB Storage Backend for Ray GCS

**Branch:** `jhasm/rep-64-poc-1` &nbsp;|&nbsp; **REP:** `2026-02-23-gcs-embedded-storage.md` (in `ray-project/enhancements`) &nbsp;|&nbsp; **Author:** Santosh Jha &lt;santosh.m.jha@gmail.com&gt;

## What this is

A proof-of-concept for replacing Ray's external-Redis fault-tolerance backend with a local-RocksDB-on-PVC backend, as proposed in REP-64. **It is not a feature PR.** Its job is to surface the design's risks, prove or disprove the REP's concrete claims, and ship a self-contained dossier reviewers can read without needing the author present.

## Where to start (depending on who you are)

- **Maintainer evaluating the REP:** read `EVIDENCE.md` first. It maps every REP claim to evidence + a verdict in one page.
- **Reviewer checking the implementation:** the production-bound code changes are scoped to:
  - `bazel/BUILD.rocksdb`, `bazel/ray_deps_setup.bzl` — RocksDB dep wiring
  - `src/ray/gcs/store_client/rocksdb_store_client.{h,cc}` — the new `StoreClient`
  - `src/ray/gcs/store_client/test/rocksdb_store_client_test.cc` — unit tests
  - `src/ray/gcs/gcs_server/gcs_server.{h,cc}`, `src/ray/common/ray_config_def.h` — GCS wiring + `RAY_GCS_STORAGE=rocksdb` config
  - `BUILD.bazel` — three new build entries

  POC harness code lives entirely under `rep-64-poc/harness/` and is not on the production-bound path.

- **Reproducing the numbers:** `reproducers/README.md` has one-command repros for every phase. Quick sanity sweep is:
  ```bash
  bazel test --config=ci \
    //:rocksdb_smoke_test \
    //:rocksdb_store_client_test \
    //rep-64-poc/harness/concurrency:concurrency_test \
    //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
  ```

## Layout

```
rep-64-poc/
├── README.md          (this file — orientation)
├── EVIDENCE.md        (maintainer-facing claim → evidence map; the load-bearing doc)
├── PLAN.md            (the up-front 9-phase plan; phases 1–8 are now reports, see reports/)
├── RISKS.md           (live risk register, updated each phase)
├── reports/           (one report per phase — claim, method, result, skepticism, repro)
│   ├── phase-1-foundations.md
│   ├── phase-2-bazel.md
│   ├── phase-3-skeleton.md
│   ├── phase-4-durability.md
│   ├── phase-5-concurrency.md
│   ├── phase-6-api-parity.md
│   ├── phase-7-microbench.md
│   └── phase-8-recovery-k8s.md
├── harness/           (every test / benchmark / reproducer the POC produced)
│   ├── docker-compose/    (Phase 1 Redis baseline harness)
│   ├── durability/        (Phase 1 fsync probe + Phase 4 kill-9 harness + results)
│   ├── concurrency/       (Phase 5 N-thread stress)
│   ├── store_client_parity/ (Phase 6 StoreClientTestBase parity)
│   ├── microbench/        (Phase 7 InMemory-vs-RocksDB latencies + results)
│   ├── recovery/          (Phase 8 storage-layer recovery time + results)
│   ├── kind/              (Phase 8 reference K8s manifest + repro README)
│   ├── cloud/             (Phase 8 cloud-volume sanity wrapper script)
│   └── integration/       (Phase 3 actor-survival end-to-end test, gated on RAY_REP64_RUN_E2E=1)
└── reproducers/
    └── README.md      (one-command repro per phase, for reviewers)
```

## Three things maintainers should know upfront

1. **The REP's "0.01–0.1 ms RocksDB write" claim is misleading and should be revised.** That number is for memtable-only / `sync=false` writes. The durability claim the REP elsewhere requires is `sync=true`, which is fsync-bounded: **3.81 ms p50** on probe-verified ext4 on this VM. See `reports/phase-7-microbench.md`. (Reads remain 0.01–0.1 ms class — the REP's read claim is confirmed.)
2. **The cluster-ID-mismatch fail-fast story (REP "Stale data protection") cannot be implemented as drawn.** Ray's GCS init order has `InitKVManager()` before `GetOrGenerateClusterId()`, and the persisted ID *is* the cluster ID — there's nothing to mismatch against. This branch defers the fail-fast to a follow-on PR with K8s downward API plumbing where an external authoritative ID can come from. The deferral is honestly documented in `reports/phase-3-skeleton.md`.
3. **Binary size impact is well under the PLAN's 50 MB pivot trigger.** `gcs_server` grows from 16.5 MB → 23.1 MB stripped (+6.6 MB, +40%) on x86_64 Linux. Numbers + apples-to-apples master baseline are in `reports/phase-3-skeleton.md` and `EVIDENCE.md`.

## A note on porting

Phases 1–9 of the POC dossier (every report, every harness) were authored against an earlier Ray master and the production code adapted to upstream's current `StoreClient` API as a final integration step. The phase reports' numbers are still valid (the per-phase test runs they describe were green at their time of writing); the harness code in `rep-64-poc/harness/` and the production code in `src/ray/gcs/store_client/rocksdb_store_client.{h,cc}` are now against upstream's `Postable<>`-based API. The dossier captures the journey; the diff in this PR is the integrated outcome.

## What's *not* in this POC (deferred, per the PLAN)

- Production hardening of `RocksDbStoreClient` (most failure modes RAY_CHECK).
- Observability / metrics: per-method latency histograms, fsync stalls.
- KubeRay operator changes for PVC lifecycle automation.
- HA / active-standby GCS — out of scope; the REP itself does not propose HA here.
- State compaction, TTL, GCS OOM mitigation.
- Migration tooling for existing Redis-backed Ray clusters.
- `doc/source/` user docs.

## Reading order

If you have ten minutes: `EVIDENCE.md`, then skim `RISKS.md`. Both are one page.

If you have an hour: read each phase report's "Result", "Skepticism", and "Pivot decision" sections. Skip the methodology bodies on the first pass — they are there for someone re-running the bench.

If you're going to run the repros: `reproducers/README.md` is the one-stop shop.
