# Phase 9 — Final dossier + production-hardening backlog

**Status:** dossier shipped (`README.md`, `EVIDENCE.md`, `reproducers/README.md`, `PR_BODY.md`); risk register finalized; production-hardening backlog enumerated.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

Coherence of the whole. Phase 9 isn't testing anything new; it's making sure a maintainer who lands on this branch cold can find the load-bearing artifacts in five minutes and re-run the numerical claims in an hour.

## What landed

### Maintainer-facing dossier

- **`rep-64-poc/EVIDENCE.md`** — single-page claim-to-evidence map. Lists each REP claim, the phase that tested it, and the verdict (verified / partially verified / contradicted). Carries the substrate matrix and the open follow-up list.
- **`rep-64-poc/README.md`** — orientation document. "I just landed on this branch — what should I read first?"
- **`rep-64-poc/reproducers/README.md`** — one-command repros for every phase, in order. Includes toolchain notes and the substrate-honesty caveat.
- **`rep-64-poc/PR_BODY.md`** — pull-request description, ready to paste into the GitHub PR body. Includes the open questions for maintainers.

### Live risk register

`rep-64-poc/RISKS.md` was updated each phase as evidence landed. Final state:

| | Closed | Partially closed | Open |
|---|---|---|---|
| Count | R1, R2 (modulo TSAN/ASAN), R5 (modulo TSAN/ASAN), R7 (storage layer) | R3, R4, R6, R8 | R9 (meta), R10 (process), R11 (one instance caught — methodology pinned) |

Detail in `RISKS.md` itself; cross-referenced by every phase report.

### Production-hardening backlog (out of POC scope, listed for the maintainers)

These are explicitly deferred from the POC per the PLAN's "non-goals" section. They are real follow-on work, but proving them was not the POC's job:

| Item | Why deferred | Where it lives |
|---|---|---|
| Comprehensive error paths in `RocksDbStoreClient` | Most failure modes today RAY_CHECK. Production-grade error handling needs design choices (which errors retry vs propagate vs fail-fast) that should be made with maintainer input. | Phase 6 follow-on. |
| Observability / metrics | Per-method latency histograms, fsync stalls, compaction time series, memtable-flush count. None of this is in the POC. | Phase 7 follow-on. |
| KubeRay operator changes for PVC lifecycle | The POC ships a hand-written manifest; KubeRay operator integration is a separate, larger effort with its own design considerations. | Out of scope. |
| HA / active-standby GCS | The REP itself does not propose HA for the embedded backend; out of scope. | Out of scope. |
| State compaction / TTL / GCS OOM mitigation | Long-running clusters accumulate state. Compaction tuning (the REP's level-by-level table) is meaningful work; the POC uses RocksDB defaults. | Phase 7 follow-on. |
| Migration tooling for existing Redis-backed Ray | No migration path is implemented. The POC's design is opt-in via `RAY_GCS_STORAGE=rocksdb`. | Out of scope. |
| `doc/source/` user docs | The POC's docs target maintainers (this dossier), not end users. | Out of scope. |
| `chaos_rocksdb_store_client_test.cc` | Mirror of `chaos_redis_store_client_test.cc`. Mechanical work; moderate value because Phase 4 + 5 already exercise the most important chaos vectors. | Phase 6 follow-on. |

### Open follow-ups (in scope but unblocked by external dependencies)

These are *not* deferred — they're things we'd run if we had the host / image / collaborator. Listed in priority order:

1. **Build a Ray container image from this branch.** Highest-impact follow-on. Unblocks: actor-survival end-to-end test (`harness/integration/test_rocksdb_recovery.py`), K8s pod-delete recovery timing, Docker Compose Redis-vs-RocksDB head-to-head, full Phase 8.
2. **TSAN / ASAN runs** of every test target on a Ray-CI-toolchain host. Same targets, no code change.
3. **Cloud-volume substrate sweep** (EBS, GCE PD) via `cloud_fsync_check.sh` + collaborator. Closes R4 fully.
4. **NFS loopback substrate.** Quick-win local test.
5. **`drop_caches`-equipped variants** of Phases 4 and 8 on a host with `sudo`. Closes the OS-level fsync honesty argument.
6. **macOS+F_FULLFSYNC** runs on the user's MacBook for substrate diversity at zero infrastructure cost.
7. **Multi-threaded-writer microbench variant** (Phase 7 v2) to capture group-commit aggregate-throughput benefit.
8. **Cluster-ID-mismatch fail-fast** with K8s downward API plumbing (Phase 8 follow-on).

## Skepticism

### What this phase does NOT do

- Add new evidence beyond what Phases 1–8 already shipped. Phase 9 is consolidation, not measurement.
- Resolve the open questions for maintainers in `PR_BODY.md`. Those need maintainer input.
- Make policy decisions about what should be production-hardened first. The backlog is listed by topic, not prioritized — sequencing belongs to whoever owns the eventual feature work.

### What would invalidate this dossier

- A maintainer trying to follow `reproducers/README.md` and getting stuck. The repros were exercised on this branch but only on this VM's substrate; a different toolchain / kernel / filesystem could surface a path that wasn't tested. **Mitigation:** every repro emits structured JSON; failures or unexpected numbers are easy to diff against the committed baselines.
- A maintainer reading `EVIDENCE.md` and finding a claim listed as "verified" that is contradicted by the linked phase report. **Mitigation:** every verdict cell links to the phase report; the table summarises but does not summarise differently from the phase reports.
- A reader concluding "the POC says everything is fine" because the dossier is well-organized. **Mitigation:** the three findings in the executive summary (REP perf claim wrong, cluster-ID fail-fast undeliverable as drawn, two Ray-core regressions that needed fixing) are highlighted up-front in `README.md` and `EVIDENCE.md` precisely to prevent that.

## Pivot decision

**Open the PR.** Phase 9 IS the close-out; the dossier is the artifact.
