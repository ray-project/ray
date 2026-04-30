# Phase 5 — Concurrency: atomic GetNextJobID and parallel writes

**Status:** mutex-RMW path verified under N-thread contention; the harness caught a real TOCTOU race in Phase 3's `GetOrCreateColumnFamily` (now fixed). 4/4 tests pass, 10/10 runs stable.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> "Option 1: Read-modify-write under a mutex (simple, GCS is single-process)."

The REP picks mutex-RMW as the recommended approach. Phase 5 demonstrates that this is correct under contention, and looks at whether a Merge-operator alternative is worth the added complexity.

## Method

`rep-64-poc/harness/concurrency/concurrency_test.cc` — a Phase 5–owned gtest target separate from the upstream-bound `//:rocksdb_store_client_test`. Four tests:

1. **`GetNextJobIDNoDuplicatesAcrossThreads`** — 16 threads × 1000 calls each. Asserts every returned ID is unique and that the union is exactly the contiguous range `[1, 16000]`. The "no skips" half of this check is what catches lost increments under racy RMW.
2. **`GetNextJobIDMonotonicPerThread`** — 8 threads × 500 calls. Each thread's own observed sequence must be strictly increasing. Catches reordering bugs (a thread's later call seeing an earlier ID).
3. **`ParallelAsyncPutAllSurviveWithCorrectValues`** — 8 producer threads × 500 distinct keys, all `AsyncPut` against a single `io_context`. After all callbacks fire, every key must be readable with the value its producer wrote. Catches dropped writes, torn writes, cross-key value mix-ups.
4. **`JobIdSurvivesRestartAfterConcurrentLoad`** — heavy concurrent burst of `GetNextJobID`, then close+reopen the client at the same path. Next ID must be strictly greater than the largest ID issued in the previous lifetime — no roll-back, no reuse.

The tests use `instrumented_io_context` + a dedicated I/O thread, mirroring how `GcsServer` configures the StoreClient in production. The lambda-captured `std::atomic<int>` ack counters and a 60s WaitFor timeout structure the wait for callback completion deterministically.

## Result

`bazel test //rep-64-poc/harness/concurrency:concurrency_test --runs_per_test=10`:

| Test | Outcome | Notes |
|---|---|---|
| `GetNextJobIDNoDuplicatesAcrossThreads` | **PASS** | 16,000 IDs, exact contiguous range `[1, 16000]`, 0 duplicates. ~130 ms per run. |
| `GetNextJobIDMonotonicPerThread` | **PASS** | Per-thread sequences strictly increasing. ~65 ms per run. |
| `ParallelAsyncPutAllSurviveWithCorrectValues` | **PASS** | 4,000 keys, 0 missing, 0 mismatched, 0 cross-key value bleed. ~250 ms per run. |
| `JobIdSurvivesRestartAfterConcurrentLoad` | **PASS** | 1,600-ID burst, close+reopen, next ID > 1600. ~80 ms per run. |
| **Aggregate over 10 runs/test** | **40/40 PASS** | Wall-clock 0.6 – 1.4 s, no flakiness. |

### Bug caught and fixed by this phase

The first run of test #3 above immediately RAY_CHECK-failed inside RocksDB's `CreateColumnFamily`:

```
Check failed: status.ok() Failed to create column family 'phase5_table':
  Invalid argument: Column family already exists
```

Root cause: `RocksDbStoreClient::GetOrCreateColumnFamily` had a TOCTOU race between the "lookup under lock" and "create outside lock" steps. Two threads each saw the table missing, both released the lock, both called `db_->CreateColumnFamily`, the second got `Status::InvalidArgument`. The old code released the lock before the create call to avoid blocking other lookups during the disk I/O — the comment "Re-take the lock; another caller may have raced us" handled the cf_handles_ map race but missed that RocksDB itself rejects the duplicate.

Fix: hold the lock through `CreateColumnFamily`. The fast path (already-cached handle) is the steady state and remains uncontended; only the very first touch per table per process pays the briefly-held-lock cost. Diff in the same commit as this phase's tests.

This is exactly the kind of bug the Phase 5 stress harness exists to catch — silent in unit tests that hit each table once, fatal in any real concurrent workload.

## Skepticism

### What this phase does NOT prove

- **TSAN / ASAN cleanliness.** Both bazel test runs above were under `--config=ci` (default toolchain), not `--config=tsan` or `--config=asan-clang`. The dev VM ships GCC 11 in `~/.local/`; the LLVM toolchain Ray's ASAN config wants is not installed. A run on a properly-tooled CI host is the right next step. Failure modes a TSAN run would surface that the deterministic-assertion harness here might miss: data-race-but-not-corruption-yet on speculative-execution, missed memory-fence around `cf_handles_`, etc.
- **Merge-operator alternative.** The PLAN flags a RocksDB Merge operator with uint64 addition as the alternative atomic-counter implementation. We have not implemented or tested it. **Recommendation: defer indefinitely.** Reasons: (a) GCS is single-process, so the mutex-RMW path is unambiguously simple and correct; (b) Merge operators have subtle compaction-time correctness considerations (idempotency under merge re-execution, partial-merge semantics) that warrant their own design review; (c) the mutex-RMW path's contention does not appear to be on the critical path of GCS workloads — Phase 7's microbenchmarks will surface it if it is. Reflected in the recommended approach in this report's body.
- **Cross-process atomicity.** GCS is single-process today. If Ray ever spawned multiple GCS-internal processes that share a RocksDB, mutex-RMW would not protect them. Out of scope for this phase; flagged for future hardening.
- **Failure during `CreateColumnFamily`.** The fix above holds the lock across a disk-I/O call. A failure in that I/O while the lock is held would currently `RAY_CHECK` (process-exit). For Phase 6's full StoreClient parity, error paths get more scrutiny.
- **Random kill-points within the concurrent burst.** Phase 4's kill-9 harness covers durability under SIGKILL on a single-writer workload. The Phase 5 tests run to completion. Combining the two — concurrent writers + SIGKILL — is properly Phase 6's chaos-test pattern (the existing redis-store-client chaos test gives the template).

### What would invalidate this result

- A TSAN run flagging a data race the deterministic assertions silently rode through. None observed yet on this toolchain.
- A 1000+-thread or 100k+-iteration stress that exposes a tail-latency or contention edge the 16×1000 / 8×500 grid did not. This is a Phase 7/release-test concern, not a correctness one.
- A real GCS workload pattern (many tables, sparse touches, concurrent lazy-creates of *different* tables) trips a different cf-mutex contention pattern. The fix here is correct for "create-once-per-table"; if creates need to be parallel-across-tables, a per-table once-flag would be the upgrade.

### What R-register status changes

- **R5 (atomic GetNextJobID correctness).** **Closed for the mutex-RMW path on GCC 11.** Reopen to "open under TSAN/ASAN" once a properly-tooled host is available. The Merge-operator alternative is recommended deferred; rationale captured here.

## Reproducer

```bash
bazel test --config=ci //rep-64-poc/harness/concurrency:concurrency_test \
  --test_output=streamed --runs_per_test=10
```

To inspect per-test wall-clock and stability:

```bash
bazel test --config=ci //rep-64-poc/harness/concurrency:concurrency_test \
  --test_output=summary --runs_per_test=10
# Each test reports its own wall-clock; runs_per_test=10 surfaces flake.
```

For TSAN / ASAN once the LLVM toolchain is available:

```bash
bazel test --config=tsan       //rep-64-poc/harness/concurrency:concurrency_test
bazel test --config=asan-clang //rep-64-poc/harness/concurrency:concurrency_test
```

## Pivot decision

**Proceed.** Mutex-RMW under N-thread contention is verified correct, the harness caught a real bug in the Phase 3 implementation, and the Merge-operator alternative is recommended deferred. Phase 6 (API completeness) is the next step.

## Next concrete actions

1. **TSAN / ASAN run** on a host with the LLVM toolchain Ray's CI uses. Same test target.
2. **Per-table-create parallelism** if Phase 7 microbenchmarks show many-table-create contention is a real workload pattern. Today's serialised create on the very-first touch is fine for the steady state.
3. **Combine Phase 4 + Phase 5** into a chaos-test variant that mirrors `chaos_redis_store_client_test.cc`. Properly Phase 6 work.
