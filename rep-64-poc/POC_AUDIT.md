# RocksDbStoreClient — POC self-audit (post per-key strand fix)

**Triggered by:** Cursor Bugbot comment #3175800831 (TOCTOU race in
`!overwrite` on offload path) revealed that the offload path silently
broke a contract — per-key submission-order execution — that the inline
path, `InMemoryStoreClient`, and `RedisStoreClient` all give for free
via single-threaded execution. The TOCTOU was *one symptom* of a
broader hole. This audit checks the rest of the surface for the same
class of bug and any other correctness/safety gaps.

**Method:** Walk every public StoreClient method, every private
helper, the constructor, and the destructor. For each: identify the
shared state it touches, the threads that can hit it, and whether
ordering, atomicity, or sequencing is required. Cross-check against
the InMemory/Redis reference semantics.

## Verdict

The strand fix closes the broader ordering bug. The audit found two
additional issues worth fixing in this PR (both addressed below) and
several intentional design choices worth flagging so reviewers can
push back if they disagree.

---

## Fixed in this commit (the strand commit)

### 1. Per-key ordering on the offload path — **fixed**

Symptom Cursor found: TOCTOU on `AsyncPut !overwrite`. Underlying bug:
the offload pool reorders any same-key sequence. Affected:

| Sequence | Inline path | Offload (no strand) | Offload (strand) |
|---|---|---|---|
| `Put(K, !overwrite)` × 2 racing | One inserted | Both can `inserted=true` | One inserted ✓ |
| `Put(K, V1); Put(K, V2)` | V2 final | Either can be final | V2 final ✓ |
| `Delete(K); Put(K, V)` | V present | Either result | V present ✓ |
| `Put(K, V); Get(K)` | Sees V | May see old | Sees V ✓ |

**Fix:** Single-key ops (`AsyncPut`, `AsyncGet`, `AsyncDelete`,
`AsyncExists`) dispatch through `RunIoForKey(table, key, work)`, which
posts to a `boost::asio::strand` bucketed by
`absl::HashOf(table, key) % gcs_rocksdb_strand_buckets`. Same-bucket
posts execute FIFO; different buckets parallelize up to pool size.
Multi-key/scan ops (`AsyncMultiGet`, `AsyncGetAll`, `AsyncGetKeys`,
`AsyncBatchDelete`) and the global counter (`AsyncGetNextJobID`) use
`RunIoUnordered`, posting straight to the pool.

**Tests:** `OffloadStrandSerializesOverwriteFalseRace` (128 issuer
threads racing same key, exactly 1 inserted=true), `LWW`, `Delete-Put`,
`CrossKeyParallelism` — all in
`src/ray/gcs/store_client/tests/rocksdb_store_client_test.cc`.

### 2. `AsyncBatchDelete` tombstone churn — **fixed**

Same shape as the AsyncDelete optimization Cursor flagged: the previous
code `WriteBatch.Delete(K)`'d every key in the batch even if the
existence probe found it absent. That writes tombstones (with fsync)
for no-op deletes, costs a wasted ~3.8 ms write per batch fsync, and
adds compaction debt. Now skips the WriteBatch entry when the probe
found the key absent; the entire `db_->Write()` call is skipped if
`deleted_count == 0`.

---

## Confirmed correct (reviewed, no change)

### Constructor

- **Cluster-ID validation** — runs on the constructor's thread before
  the object is reachable from other code. No race.
- **Counter recovery via `std::stoll`** — explicit range-check against
  `[0, INT_MAX]` (Gemini fix from earlier in the session). Corrupted
  counter produces clear `RAY_LOG(FATAL)` instead of
  `std::out_of_range`.
- **Column-family open with full descriptor list** —
  `RocksDB::ListColumnFamilies` followed by `Open` with descriptors
  ensures every existing CF is registered in `cf_handles_`. Fresh DBs
  get just `default`. ✓

### Destructor

- **Pool drain before DB close** — `io_pool_->stop(); io_pool_->join();`
  runs before `cf_handles_` is walked and `db_` destructs. Strands
  wrap the pool's executor, so joining the pool drains them; the
  subsequent `strands_.clear()` is then safe.
- **`DestroyColumnFamilyHandle` under cf_mutex_** — already correct;
  no other thread can hold the mutex once the pool is joined, so the
  lock is precautionary against a non-existent race, but cheap.

### `GetOrCreateColumnFamily`

- Holds `cf_mutex_` across `db_->CreateColumnFamily` to prevent two
  first-touches both calling RocksDB (which would reject the second
  as `InvalidArgument`). Phase 5 stress-tested this; the existing code
  comment documents the "real TOCTOU race we caught and fixed" history.
  Cursor flagged contention on this mutex as a perf concern earlier in
  the session — kept as-is because CFs are created at most once per
  table over process lifetime.

### `GetNextJobIDSync`

- Internal `job_id_mutex_` covers BOTH the in-memory increment and the
  fsynced Put. Sequencing is `lock; ++; Put(sync); unlock`, so:
  - Two threads racing → serialized at the mutex; each gets a unique ID.
  - Crash between `++` and the Put → next-on-restart re-reads the
    persisted value, which is one less than the in-memory one was; the
    just-incremented in-memory value is forgotten. Caller A's request
    that crashed gets retried by the GCS RPC layer with a fresh ID.
  - In-memory counter is always ≥ persisted counter, so we never
    re-issue a previously-issued ID. ✓

### Snapshot semantics for scans

- `db_->NewIterator(ReadOptions, cf)` takes an implicit snapshot at
  iterator-create time. `AsyncGetAll` and `AsyncGetKeys` walk that
  snapshot to completion, so concurrent writes during the scan don't
  corrupt the result. Same as the InMemoryStoreClient's "lock the map
  for the duration of the scan" semantics.
- `db_->MultiGet(ReadOptions, ...)` reads from a single snapshot
  internally. Per-key results are mutually consistent within the call.

### `WriteOptions::sync = true`

- Set at every mutating call site (`AsyncPut`, `AsyncDelete`'s real
  delete, `AsyncBatchDelete`'s `db_->Write`, `GetNextJobIDSync`'s Put,
  cluster-marker write). REP-64 §"Durability" requires this. ✓

### Default-CF key namespace collision

- `kClusterIdKey` and `kJobCounterKey` live in the default CF and are
  prefixed `__ray_rep64_`. User tables go through
  `GetOrCreateColumnFamily(table_name)`, which creates a per-table CF
  — user keys are never written to the default CF. No collision risk
  unless a future change starts writing user data to the default CF
  (it shouldn't, and there's no code path that does).

---

## Intentional design choices (flagged for reviewer pushback)

### Multi-key/scan ordering is intentionally loose

`AsyncMultiGet` / `AsyncGetAll` / `AsyncGetKeys` / `AsyncBatchDelete`
post to the base pool with no strand, so:
- A scan that races a single-key `Put(K, V)` may or may not see V.
- `MultiGet([K1, K2])` that races `Put(K1)` may see either old or new.
- `BatchDelete([K1, K2])`'s returned count reflects "what was there at
  the moment of the probe", not the moment of the actual delete.

**Rationale:** A global ordering guarantee would funnel all ops
through a single strand → kills throughput. This matches Redis
pipelining (per-key order, no global order) and what
InMemoryStoreClient gives under concurrent callers. Callers who need
scan-after-write ordering should serialize through the write callback,
which is the existing contract.

**If reviewers disagree:** the alternative is per-table strands for
scans, which serializes all scans on one table but preserves
write→scan ordering. Half-step compromise; happy to add if requested.

### Strand bucket count default = 64

- Pool default 4, buckets 64 → ~16× headroom. Different keys
  collision-share a bucket only when their hashes mod 64 collide,
  which for any realistic GCS key set is rare and only costs a small
  parallelism dip when it happens.
- `gcs_rocksdb_strand_buckets` config knob lets ops tune up if a
  specific deployment sees contention. No data-driven floor in this
  POC.

### `OptimisticTransactionDB` not used

- Strand gives single-process correctness. The GCS is single-process,
  so this is sufficient.
- If GCS ever ran multi-writer against the same DB (it doesn't, and
  RocksDB's LOCK file would block it anyway), only OptimisticTransactionDB
  would close cross-process races.

### `AsyncMultiGet` / `AsyncBatchDelete` FATAL on non-NotFound errors

- Both `RAY_LOG(FATAL)` if any per-key status is neither `ok()` nor
  `IsNotFound()`. This is loud but the GCS RPC layer retries above
  us, so the FATAL gives a clear "the DB is in a state we can't
  reason about" signal rather than silently losing data. Same posture
  as the rest of the StoreClient.

### `GetOrCreateColumnFamily` mutex held across `CreateColumnFamily`

- Cursor flagged this as a contention concern. We hold the mutex
  across an I/O call to prevent the duplicate-create race; a
  double-checked locking refactor would need a lot of care to avoid
  publishing a half-initialized handle. CFs are created at most once
  per table over process lifetime, so steady-state lookups hit the
  cache fast-path. Documented as not-pursued unless real contention
  appears.

---

## Open questions for reviewers

1. **Strand bucket count default.** 64 is a defensible pick; a
   maintainer with deeper knowledge of GCS workload patterns might
   want a different number, or might want it derived from
   `io_pool_size`.

2. **Should multi-key ops also strand?** I'd argue no (see "Intentional
   design choices" above), but a maintainer might prefer per-table
   strands for stronger scan semantics. Trade-off is throughput vs
   ordering.

3. **Should the inline path also use strands?** Currently
   `RunIoForKey` runs inline when `strands_.empty()`. The inline path
   has trivial submission-order=execution-order via the io_service
   thread, so strands would just add dispatch overhead. But if the GCS
   ever runs the io_service multi-threaded, this assumption breaks —
   worth a comment or assert if that's possible.

4. **Should `AsyncBatchDelete` count semantics be tightened?** Today
   the count reflects probe-time existence, which can drift from
   actual-delete-time existence under concurrent writes. Matches
   InMemoryStoreClient under same races. Could be tightened with
   per-key strand acquisition, at the cost of multi-strand coordination
   complexity.
