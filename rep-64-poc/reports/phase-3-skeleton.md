# Phase 3 — Walking skeleton: minimal RocksDbStoreClient + GCS integration

**Status:** code shipped, all 4 storage-layer recovery tests PASS (`bazel test //:rocksdb_store_client_test`); cluster-level (Python) recovery test scaffolded but not executed yet.
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> The `StoreClient` interface fits RocksDB cleanly enough that the GCS recovery model is identical to today.

## Method

Five changes turn the dep added in Phase 2 into a backend the GCS server can be configured to use:

1. **`src/ray/gcs/store_client/rocksdb_store_client.{h,cc}`** — new. Implements `StoreClient` with the three methods Phase 3 owes: `AsyncPut`, `AsyncGet`, `GetNextJobID`. The remaining six methods abort via `RAY_CHECK(false)` so unimplemented paths fail loudly during integration testing rather than silently returning empty data. Storage-side bookkeeping in the constructor:
   - Lists existing column families and opens them all.
   - Reads (or creates) the cluster-ID marker — REP §"Stale data protection". Mismatch → fail-fast.
   - Recovers the persisted job counter so a restart doesn't reissue old job IDs.
   - Lazily creates a column family on first `AsyncPut`/`AsyncGet` for an unseen table.

2. **`src/ray/common/ray_config_def.h`** — adds `RAY_CONFIG(std::string, gcs_storage_path, "")`. `gcs_storage` already existed; the new field carries the `RAY_GCS_STORAGE_PATH` env var.

3. **`src/ray/gcs/gcs_server/gcs_server.{h,cc}`** — adds `StorageType::ROCKSDB_PERSIST` and the `kRocksDbStorage = "rocksdb"` constant, extends `GetStorageType()` with a third branch (with a `RAY_CHECK` that the storage path is non-empty), and adds a fourth case to `InitKVManager` that wraps `RocksDbStoreClient` in `ObservableStoreClient` exactly like the in-memory path. The cluster ID is plumbed in from `GetClusterId().Hex()` so the marker check is meaningful in production.

4. **`BUILD.bazel`** — new `cc_library :gcs_rocksdb_store_client` (mirrors the in-memory pattern), added to `gcs_server_lib` deps. New `cc_test :rocksdb_store_client_test` for the storage-layer recovery tests.

5. **`src/ray/gcs/store_client/test/rocksdb_store_client_test.cc`** — new. Four tests:
   - `PutGetRoundtrip`: `AsyncPut` → `AsyncGet` returns the value within a single client.
   - `RecoverAcrossReopen`: writes 3 keys across 2 tables, destroys the client (closes the DB), constructs a new client at the same path with the same cluster ID, and reads all 3 keys back. **This is the headline Phase 3 proof point.**
   - `JobIdMonotonicAndPersists`: counter is monotonic within a process and a fresh process opens to a value strictly greater than the previous lifetime's last issued ID.
   - `ClusterIdMarkerWritesOnFirstOpen`: marker write on first open, accept on second open with same ID.

6. **`rep-64-poc/harness/integration/test_rocksdb_recovery.py`** — new. End-to-end Python scaffold for the cluster-level claim ("detached actor survives GCS kill"). Skipped unless `RAY_REP64_RUN_E2E=1` and a Ray build with this branch is available.

## Recovery sequence (actual vs REP)

The REP draws this for the recovery flow:

```
Head pod crashes → KubeRay restarts head pod → PVC re-mounted →
GCS opens RocksDB at RAY_GCS_STORAGE_PATH → reads state → workers reconnect
```

In the Phase 3 implementation, the C++ layer corresponds as follows:

```
new RocksDbStoreClient(io, db_path, cluster_id)
  ↓
DB::ListColumnFamilies(db_path)               # discover tables
  ↓
DB::Open(db_path, descriptors)                # open all CFs at once
  ↓
ValidateOrWriteClusterIdMarker(cluster_id)    # fail-fast on PVC mismatch
  ↓
db_->Get(default_cf, kJobCounterKey)          # restore job counter
  ↓
ready: AsyncPut/AsyncGet served from cf_handles_
```

The state-loading half of the REP's diagram is unchanged from today — that's `GcsInitData::AsyncLoad()` calling `AsyncGetAll` per table on whichever `StoreClient` is configured. Phase 3 doesn't implement `AsyncGetAll` yet (it stubs), so the *full* recovery path (load actor table, load node table, …) doesn't run end-to-end until Phase 6 ships `AsyncGetAll`. What Phase 3 does prove is the *storage* layer's recovery contract: data written before close is readable after reopen.

## Result

### What this run verifies

- **`RocksDbStoreClient` source compiles against real RocksDB 9.11.2 headers** (GCC 11, `--std=c++17 -fsyntax-only`).
- **`rocksdb_store_client_test.cc` source compiles against the same headers.**
- **`gcs_server.cc` edit is internally consistent** — the new switch case calls into `RocksDbStoreClient` with arguments that match its constructor.
- **API fit, by inspection.** The three implemented methods follow the same callback-on-`main_io_service_` pattern as `InMemoryStoreClient`. No new asio primitives were needed. No `StoreClient` method has surfaced as un-implementable on RocksDB.

### What this run does NOT verify

The dev VM still lacks `bazel build`-capable tooling (see `phase-2-bazel.md`), so:

- `bazel test //:rocksdb_store_client_test` has not run.
- `bazel build //:gcs_server` with the new code path has not run.
- The end-to-end Python recovery test has not run.

These all unblock as soon as a Bazel-capable host is available. The C++ unit test is structured so the recovery proof is one `bazel test` command; the Python test runs under `pytest` with `RAY_REP64_RUN_E2E=1`.

### Provisional answer

| Question | Phase 3 answer |
|---|---|
| Does the StoreClient interface fit RocksDB? | **Yes** for the three methods exercised here. No interface changes were needed; the InMemoryStoreClient pattern transfers directly. |
| Does state survive close+reopen at the storage layer? | **Yes** by construction (RocksDB durability + sync writes), tested via `RecoverAcrossReopen`, but not yet executed. |
| Does the API fit cleanly for *all* methods? | **Pending.** `AsyncGetAll`/`AsyncMultiGet`/`AsyncGetKeys`/etc. stubs are RAY_CHECK(false). Phase 6 implements them and validates with the full `StoreClientTestBase`. |

## Skepticism

### What this provisional pass does *not* prove

- **The walking skeleton actually works end-to-end inside Ray.** Until `bazel test` runs, we have a strong static-analysis confidence (compiles, types match, follows existing patterns) but not a single integration data point.
- **The ObservableStoreClient wrapper composes correctly.** We added `RocksDbStoreClient` inside `ObservableStoreClient` mirroring the InMemory path. If the observable layer assumes anything specific about the inner client's threading, we'll discover it on first build.
- **The cluster ID we plumb (`GetClusterId().Hex()`) is non-empty by the time `InitKVManager()` runs.** The PLAN puts cluster-ID generation *after* `InitKVManager()` is called in `GcsServer::Start()`. We may need to defer the marker check, or plumb a different identifier. Rephrasing this honestly: **the cluster-ID wiring is plausible, not yet correct on first build.** Slot in `phase-3-skeleton.md` "Next concrete actions" to verify and possibly rework.
- **Test for cluster-ID-mismatch fail-fast is missing.** The C++ test file calls this out: gtest death tests + RocksDB open file handles fork-poorly. Phase 8 covers this at the K8s level, where the right machinery exists.

### What R-register status changes

- **R8 (stale data on re-used PVC).** Mitigation in place: marker mechanism implemented and unit-tested at the storage layer. Closes the storage-layer half. Cluster-ID plumbing into the marker may need rework once we see real init order — open as caveat.
- **R3 (binary size + memory overhead).** Still open. We added a static lib whose size delta is unknown until a real build.

### What would invalidate Phase 3's claim

- A method on `StoreClient` whose semantics we cannot satisfy without invasive workarounds. None observed for the three implemented methods; remains a residual risk for the Phase 6 fill-out.
- The first build reveals that `ObservableStoreClient` requires a primitive `RocksDbStoreClient` doesn't expose.
- Cluster-ID timing forces us to defer the marker check past the constructor — survivable but worth documenting.

## Reproducer

C++ unit tests, on a Bazel-capable host:

```bash
bazel test --config=ci //:rocksdb_store_client_test --test_output=streamed
bazel build --config=ci --config=asan-clang //:rocksdb_store_client_test
```

End-to-end Python (requires `bazel build //:gcs_server` to have produced a binary that uses the new code path):

```bash
RAY_REP64_RUN_E2E=1 \
  pytest -s rep-64-poc/harness/integration/test_rocksdb_recovery.py
```

## Pivot decision

**Proceed.** No interface mis-fit observed. The three implemented methods follow the existing pattern cleanly, the recovery test is targeted at the headline Phase 3 claim, and the GCS-server integration is a surgical addition to two switch statements and one `RAY_CONFIG` line. Phase 4 (durability under crashes) can begin; the test substrate concern flagged in Phase 1 (R4) makes Phase 4 the next place real evidence will land.

## Next concrete actions before closing Phase 3

1. Run `bazel test //:rocksdb_store_client_test` on a Bazel-capable host. Capture pass/fail, wall-clock, ASAN status. Expected outcome: green.
2. Verify cluster-ID plumbing (`GetClusterId().Hex()` at `InitKVManager()` time) — if non-empty, fine; if empty, defer the marker check or pull the ID from a different source.
3. Run `pytest rep-64-poc/harness/integration/test_rocksdb_recovery.py` end-to-end and capture the actor-survival result.
4. Update RISKS.md with the actual close-out for R8.

## Built on (2026-04-30)

Re-ran on the same dev VM after the Phase 2 lib-path fix and the boost URL fix landed (commits `b043e4622d` and `07c65c84bf`).

**Build:**
- `bazel test //:rocksdb_store_client_test` cold build: **494 s** (8m 14s). 4298 actions, dominated by first-time @boost + protobuf + gRPC compilation. Subsequent rebuilds will reuse the cache.
- Binary: `bazel-bin/rocksdb_store_client_test` = **8.5 MB** unstripped.

**Test result:**
```
[==========] Running 4 tests from 1 test suite.
[ RUN      ] RocksDbStoreClient.PutGetRoundtrip                  OK ( 67 ms)
[ RUN      ] RocksDbStoreClient.RecoverAcrossReopen              OK ( 75 ms)
[ RUN      ] RocksDbStoreClient.JobIdMonotonicAndPersists        OK ( 83 ms)
[ RUN      ] RocksDbStoreClient.ClusterIdMarkerWritesOnFirstOpen OK ( 48 ms)
[  PASSED  ] 4 tests. (273 ms total)
```

**What this verifies:**

| Phase 3 claim | Evidence |
|---|---|
| `StoreClient` interface fits RocksDB cleanly for `AsyncPut` / `AsyncGet` / `GetNextJobID`. | **Yes.** Compiled, linked, ran. No interface change needed. |
| State survives close+reopen at the storage layer. | **Yes — RecoverAcrossReopen passes.** This is the headline Phase 3 proof point that the REP claim relies on, and it now has real data. |
| JobID counter persists across process restart and is monotonic. | **Yes — JobIdMonotonicAndPersists passes.** |
| Cluster-ID marker is written on first open and validated on subsequent opens. | **Yes at the storage layer — ClusterIdMarkerWritesOnFirstOpen passes.** Cluster-ID *plumbing into `GcsServer`* is a separate concern; see "Open items" below. |

**Cluster-ID timing in `GcsServer::Start()` — confirmed bug, fix landed:**

Static analysis of `gcs_server.cc:141-152` and `grpc_server.h:132-134` confirmed the suspected runtime crash:

```cpp
void GcsServer::Start() {
  ...
  InitKVManager();                                // RocksDbStoreClient constructed here
  gcs_init_data->AsyncLoad([this, gcs_init_data] {
    GetOrGenerateClusterId([this, gcs_init_data](ClusterID cluster_id) {
      rpc_server_.SetClusterId(cluster_id);       // cluster_id set HERE — too late
      DoStart(*gcs_init_data);
    });
  });
}

// In rpc/grpc_server.h:
const ClusterID &GetClusterId() const {
  RAY_CHECK(!cluster_id_.IsNil()) << "Cannot fetch cluster ID before it is set.";
  ...
}
```

So the original `gcs_server.cc:593` `GetClusterId().Hex()` would `RAY_CHECK`-fail at every cold start with `RAY_GCS_STORAGE=rocksdb`. Unit tests didn't catch it because they construct `RocksDbStoreClient` directly, bypassing GcsServer.

A second, deeper finding: the cluster-ID marker mechanism as currently designed is partially redundant with Ray's existing `kv_manager.Put("cluster", "ClusterId", …)` (gcs_server.cc:167-170). With RocksDbStoreClient as the StoreClient, that persistence already provides cross-restart cluster-ID continuity. **For PVC-mismatch fail-fast (REP "Stale data protection") to work, GcsServer needs an *external* authoritative cluster_id source — K8s downward API or env var — to compare against the persisted one. There is no such source today.** The mismatch test path is not yet wired.

**Fix shipped in this iteration:** `gcs_server.cc:593` now passes `""` (empty string) instead of `GetClusterId().Hex()`. `RocksDbStoreClient::ValidateOrWriteClusterIdMarker` already handles empty cluster_id gracefully (skips both write and verify), so the storage layer is unchanged and unit tests continue to pass. The mismatch fail-fast capability is honestly deferred to Phase 8, where K8s integration provides the external authoritative cluster_id source.

**Other open items:**

- **End-to-end actor-survival test.** `rep-64-poc/harness/integration/test_rocksdb_recovery.py` requires a `gcs_server` binary built with this branch, then `RAY_REP64_RUN_E2E=1 pytest …`. Not yet attempted.
- **`--config=asan-clang`.** Not attempted on this host (LLVM toolchain not installed in `~/.local/`).
- **Mismatch fail-fast test (cluster-ID drift).** Re-scoped to Phase 8 K8s-level testing — needs an external authoritative cluster_id source that the current architecture lacks.

**R-register update:**

- **R8 (stale data on re-used PVC).** Re-scoped: the marker mechanism at the storage layer works (unit tests pass), but it cannot fail-fast on cluster mismatch in production today because the GcsServer integration has no external cluster_id to compare against. **Phase 3 partially closes R8** (storage layer correct, integration deferred). Full close requires Phase 8.
- **R3 (binary size).** **Captured with apples-to-apples master delta** (built with the @boost URL fix applied to a worktree at master so master itself was buildable):

  | | unstripped | stripped |
  |---|---|---|
  | Master `gcs_server` (no RocksDB) | 23.8 MB | **16.5 MB** |
  | This branch (with RocksDB) | 32.0 MB | **23.1 MB** |
  | **Delta** | **+8.2 MB (+34%)** | **+6.6 MB (+40%)** |

  Well under the PLAN's 50 MB pivot trigger. The smoke-test stripped binary is 6.8 MB (essentially RocksDB + gtest); the +6.6 MB delta on `gcs_server` is consistent — RocksDB itself accounts for almost all of it after linker dead-code elimination. `gcs_server --help` exits 0 — binary loads cleanly with the cluster-ID fix in place.

**`gcs_server` build numbers:**

| Run | Wall-clock | Notes |
|---|---|---|
| Cold build of `//:gcs_server` | 697 s (11m 37s) | 1986 actions; reused the boost + gcs lib cache from Phase 3 unit-test build, so dominated by the gcs_server-specific .cc files + final link. |
| Incremental rebuild after cluster-ID fix | 0.6 s ("up-to-date") | The in-flight cold build had not yet started compiling `gcs_server.cc` when the edit landed, so it picked up the fix transparently. |
