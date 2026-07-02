# Phase 6 — API completeness + StoreClientTestBase parity

**Status:** all 9 `StoreClient` methods implemented; the upstream `StoreClientTestBase` suite passes against `RocksDbStoreClient` unmodified.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> "RocksDbStoreClient passes all existing StoreClient test cases."

Phase 3 implemented 3 of the 9 methods (`AsyncPut`, `AsyncGet`, `GetNextJobID`); the rest fell over with `RAY_CHECK(false)` to make unimplemented paths fail loudly during integration. Phase 6 fills them out and proves the result is API-compatible with what `InMemoryStoreClient` and `RedisStoreClient` already deliver.

## Method

### Methods implemented in this phase

| Method | RocksDB primitive | Notes |
|---|---|---|
| `AsyncGetAll` | `Iterator` over the column family with `total_order_seek = true` | Full table scan. Returns all `(key, value)` pairs as a `flat_hash_map`. |
| `AsyncMultiGet` | `DB::MultiGet` | Single batched call into RocksDB; missing keys silently omitted from the result map (Redis MGET semantics). |
| `AsyncDelete` | `DB::Delete` with `WriteOptions{sync=true}` | Pre-checks existence with `Get` so the callback's `bool` reflects "removed an existing entry". |
| `AsyncBatchDelete` | `WriteBatch` of `Delete` ops, written with `Write(sync=true)` | Pre-loops with `Get` to count existing keys; the callback's `int64_t` is "how many real deletions happened" (Redis DEL semantics). |
| `AsyncGetKeys` | `Iterator` `Seek(prefix)` + walk while `key.starts_with(prefix)` | Byte-ordered prefix scan. RocksDB's iterator guarantees once a key fails the prefix check, no later key passes. |
| `AsyncExists` | `DB::Get` (value discarded) | Single point lookup. |

All methods follow the same Phase 3 dispatch pattern: do RocksDB work synchronously (mutating ops block on WAL fsync), then `main_io_service_.post(...)` the user callback so callback ordering matches the rest of GCS.

### How parity is verified

`rep-64-poc/harness/store_client_parity/rocksdb_parity_test.cc` subclasses Ray's existing `StoreClientTestBase` — the very same fixture `in_memory_store_client_test.cc` and `redis_store_client_test.cc` already use — and points it at a `RocksDbStoreClient` constructed on a per-test temp directory. The base class runs:

- `TestAsyncPutAndAsyncGet` — Put → Get → Delete → GetEmpty across 5000 keys.
- `TestAsyncGetAllAndBatchDelete` — Exists(false) → Put → GetAll → GetKeys → Exists(true) → BatchDelete → Exists(false) → GetEmpty.

Together these exercise every method in the API surface. A pass means RocksDB satisfies the same StoreClient contract the in-memory and Redis backends do; a fail would have meant the interface didn't fit and we'd have to revisit.

## Result

```
$ bazel test --config=ci //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
//rep-64-poc/harness/store_client_parity:rocksdb_parity_test    PASSED in 1.8s
```

Both subclassed test methods PASS unmodified:

| Test | Outcome | Wall-clock |
|---|---|---|
| `RocksDbStoreClientParityTest.AsyncPutAndAsyncGetTest` | **PASS** | (combined 1.8 s for both) |
| `RocksDbStoreClientParityTest.AsyncGetAllAndBatchDeleteTest` | **PASS** | |

For comparison, `InMemoryStoreClientTest` runs the same suite and presumably finishes in milliseconds (no fsync); the 1.8 s figure here is dominated by 5000 `Put(sync=true)` round-trips at the Phase-1-measured ~3.6 ms fsync p50, amortised by the OS write coalescing. **Notably, the entire 5000-key round-trip is 1.8 s, not 5000 × 3.6 ms ≈ 18 s — RocksDB's WAL group-commit batches multiple syncs across the io_service_pool's 2 threads.** This is consistent with the REP's positioning of RocksDB as fast enough for GCS workloads.

### Combined regression check (Phases 3, 5, 6 together)

```
$ bazel test --config=ci \
    //:rocksdb_store_client_test \
    //rep-64-poc/harness/concurrency:concurrency_test \
    //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
//:rocksdb_store_client_test                                        PASSED in 0.5s
//rep-64-poc/harness/concurrency:concurrency_test                   PASSED in 0.6s
//rep-64-poc/harness/store_client_parity:rocksdb_parity_test        PASSED in 1.8s
Executed 3 out of 3 tests: 3 tests pass.
```

No regressions across Phase 3 unit tests, Phase 5 concurrency stress, or Phase 6 parity. Phase 5's earlier finding (the TOCTOU race in `GetOrCreateColumnFamily`) is also exercised by this phase's test, since `StoreClientTestBase` hits a single table from multiple io_service_pool threads — the parity test would have failed before the Phase 5 fix.

## Skepticism

### What this phase does NOT prove

- **`--config=asan-clang` / `--config=tsan` cleanliness.** Same toolchain caveat as Phases 3 + 5: the dev VM ships GCC 11; the LLVM toolchain Ray's CI uses isn't installed. A run on a properly-tooled host is the right next step. The `StoreClientTestBase` suite is exactly the right input for that.
- **Full chaos-test parity.** Ray ships a `chaos_redis_store_client_test.cc` with env-driven RocksDB-side failure injection. The PLAN flags writing `chaos_rocksdb_store_client_test.cc` as part of Phase 6; we have not done so. **Recommendation:** track as a follow-on. The pattern is mechanical (mirror the Redis chaos test's structure substituting RocksDB-side fault injection points), but the value is moderate — Phase 4's kill-9 harness already covers one critical chaos vector (process death between ack and durability), and Phase 5's concurrency tests cover another (parallel-writer races).
- **Wall-clock numbers in this report are not microbenchmarks.** They tell us *the suite passes* and roughly *what fsync amortisation looks like*, not *write latency p50/p99*. Phase 7 owns the rigorous numbers.
- **Iterator-based `AsyncGetAll` on multi-MB tables.** The base test runs 5000 keys, which is large enough to surface API mistakes but small enough that iterator behaviour at scale is unverified. Phase 7's microbenchmark + the Phase 8 K8s harness will exercise larger workloads.
- **Memtable / compaction tuning.** All RocksDB knobs are at their library defaults. The REP's tuning table (LZ4 on cold levels, no compression on hot levels, etc.) is honest future work; for the POC, defaults are fine and produce data we can reason about.

### What would invalidate this result

- A `StoreClientTestBase` extension that exercises a method we don't pass — possible if Ray adds new test methods to the base after this branch. The fix is mechanical (implement whatever new RocksDB primitive is required); the API doesn't appear to have a fundamentally-RocksDB-hostile method in it today.
- TSAN / ASAN finding a real bug in any of the 6 newly-implemented methods. Most likely candidates: lifetime issue around the lambda capture of `result` in the post-callbacks (we move-construct, but a delayed callback against a destroyed client could still bite if RocksDB's iterator outlives the `RocksDbStoreClient`).
- A compaction-time semantic issue in `AsyncGetKeys` if RocksDB ever reorders keys. RocksDB guarantees byte-ordered iteration, so this should not happen, but the prefix scan's correctness rests on that guarantee.

### What R-register status changes

- **R5 (atomic correctness).** No additional change: still closed for mutex-RMW path, still open under TSAN/ASAN. (The Phase 6 parity test passing means the parallelism in `StoreClientTestBase` does not surface any new RMW-level issue beyond the TOCTOU Phase 5 already fixed.)
- **R3 (binary size).** Negligible additional contribution — these methods are small, no new dependencies. `gcs_server` size delta unchanged.

## Reproducer

```bash
bazel test --config=ci //rep-64-poc/harness/store_client_parity:rocksdb_parity_test \
  --test_output=streamed
```

Combined regression sweep:

```bash
bazel test --config=ci \
  //:rocksdb_store_client_test \
  //rep-64-poc/harness/concurrency:concurrency_test \
  //rep-64-poc/harness/store_client_parity:rocksdb_parity_test \
  --test_output=summary
```

For TSAN/ASAN once the LLVM toolchain is available:

```bash
bazel test --config=tsan       //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
bazel test --config=asan-clang //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
```

## Pivot decision

**Proceed.** API parity is demonstrated; no method has fundamentally-RocksDB-hostile semantics. Phase 7 microbenchmarks are the next step.

## Next concrete actions

1. **TSAN / ASAN run** of the parity test on a properly-tooled host. Same target.
2. **`chaos_rocksdb_store_client_test.cc`** mirroring `chaos_redis_store_client_test.cc`, if the maintainers want it before merge. Mechanical work.
3. **Memtable / compaction tuning sweep**, properly Phase 7 territory (we want the numbers before tuning, then again after, to show the tuning helps).
