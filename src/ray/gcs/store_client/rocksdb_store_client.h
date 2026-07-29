// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "boost/asio/strand.hpp"
#include "boost/asio/thread_pool.hpp"
#include "ray/asio/instrumented_io_context.h"
#include "ray/gcs/postable/postable.h"
#include "ray/gcs/store_client/store_client.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace ray {
namespace gcs {

/// \class RocksDbStoreClient
/// Embedded-storage StoreClient backed by RocksDB on a local persistent
/// volume. Implements the GCS fault-tolerance contract proposed in
/// REP-64 (`enhancements/reps/2026-02-23-gcs-embedded-storage.md`).
///
/// **Execution model.** Each call posts its RocksDB work (including the
/// WAL fsync that dominates mutating-op latency) to a small
/// `boost::asio::thread_pool`; the user-supplied callback still runs on
/// the caller's executor via `Postable::Post`. Offloading keeps the WAL
/// fsync (~3.8 ms p50 on probe-verified ext4) off the GCS event loop, so
/// a slow fsync cannot stall other GCS RPCs. RocksDB's group-commit
/// aggregates concurrent in-flight writers into one fsync, so aggregate
/// write throughput scales with pool size while the event loop stays
/// responsive.
///
///   *Per-key ordering.* Single-key ops (Put/Get/Delete/Exists) dispatch
///   through a fixed array of `boost::asio::strand`s bucketed by
///   `hash(table, key) % gcs_rocksdb_strand_buckets`. This gives per-key
///   submission-order execution, matching the guarantee that
///   `InMemoryStoreClient` and `RedisStoreClient` provide via their own
///   single-threaded execution models. Without strands, the pool would
///   happily reorder a `Delete(K); Put(K, V)` pair, race two
///   `AsyncPut(K, !overwrite)` calls so both observe "not found", or let
///   an `AsyncGet(K)` see a value older than a Put that was submitted
///   before it.
///
///   Multi-key/scan ops (MultiGet, GetAll, GetKeys, BatchDelete) post
///   to the base pool without a strand. Their semantics are inherently
///   "snapshot from whenever they ran" and matches Redis pipelining:
///   per-key order is preserved among single-key ops, but a scan that
///   races a single-key write may or may not see the write. Callers
///   needing scan-after-write ordering must serialize via the write
///   callback, which is the same contract `InMemoryStoreClient`
///   imposes under the same races.
///
/// In both paths the callback is dispatched via `Postable::Post` to the
/// GCS event loop, which keeps callback ordering uniform with the rest
/// of GCS.
///
/// **Durability.** By default every mutating call uses
/// `WriteOptions::sync = true` so the WAL is fsynced before the callback
/// fires. This is the invariant Ray's GCS RPC layer relies on: a caller
/// that received an ack can assume the write survived a crash. Whether
/// `fsync` actually flushes to media is a property of the underlying
/// volume; operators should verify substrate honesty on their storage
/// class before relying on this contract.
///
/// *Soft-durability tables.* A small, hardcoded set of tables (see
/// `SoftDurableTables()` in the .cc) is written with `sync = false`
/// instead. GCS publishes death notifications (node down, actor dead)
/// from inside the write's completion callback, so the per-write fsync
/// delays those cluster-wide notifications and widens a pre-existing
/// Ray-core reconstruction race. Relaxing the fsync on the
/// death-notification tables removes that delay while keeping durability
/// at least on par with Ray's recommended Redis GCS, which runs
/// `appendfsync everysec` (periodic, not per-write). The affected state
/// (node liveness, actor state) is re-derived after a GCS restart anyway.
/// This set is a fixed design property rather than a config knob; see
/// `SoftDurableTables()` for the full rationale and the follow-up to
/// remove the workaround once the root-cause GCS-core race is fixed.
class RocksDbStoreClient : public StoreClient {
 public:
  /// Open or create a RocksDB at \p db_path and validate the cluster-ID
  /// marker.
  ///
  /// \param io_service The event loop for this client. Held only as a
  ///   reference so the Postable callbacks have a default I/O context.
  /// \param db_path Filesystem path on a persistent volume.
  /// \param expected_cluster_id If non-empty: enforce that any existing
  ///   marker matches; if there's no marker yet, write this value. If
  ///   empty: skip the marker entirely. The empty path is the production
  ///   wiring today (see `gcs_server.cc`'s ROCKSDB_PERSIST case): GCS
  ///   does not have an authoritative cluster_id at the moment
  ///   `InitKVManager()` runs. PVC-mismatch fail-fast is deferred until
  ///   the K8s downward API plumbs in an external authoritative ID.
  /// \param io_pool_size Worker-thread count for the RocksDB I/O offload
  ///   pool. Clamped to >= 1.
  /// \param strand_buckets Number of per-key `asio::strand` buckets used
  ///   for single-key op ordering. Clamped to >= 1. Default 64 gives
  ///   ~16x headroom over the typical pool size (4) so collision-
  ///   induced serialization is rare. See class docstring for the
  ///   ordering guarantees this controls.
  RocksDbStoreClient([[maybe_unused]] instrumented_io_context &io_service,
                     const std::string &db_path,
                     const std::string &expected_cluster_id,
                     std::size_t io_pool_size = 4,
                     std::size_t strand_buckets = 64);

  ~RocksDbStoreClient() override;

  RocksDbStoreClient(const RocksDbStoreClient &) = delete;
  RocksDbStoreClient &operator=(const RocksDbStoreClient &) = delete;

  void AsyncPut(const std::string &table_name,
                const std::string &key,
                std::string data,
                bool overwrite,
                Postable<void(bool)> callback) override;

  void AsyncGet(const std::string &table_name,
                const std::string &key,
                ToPostable<rpc::OptionalItemCallback<std::string>> callback) override;

  void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  void AsyncDelete(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

  void AsyncBatchDelete(const std::string &table_name,
                        const std::vector<std::string> &keys,
                        Postable<void(int64_t)> callback) override;

  void AsyncGetNextJobID(Postable<void(int)> callback) override;

  void AsyncGetKeys(const std::string &table_name,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) override;

  void AsyncExists(const std::string &table_name,
                   const std::string &key,
                   Postable<void(bool)> callback) override;

 private:
  /// Look up the column family for \p table_name, creating it lazily on
  /// first use. Steady-state lookups take only a shared reader lock so
  /// they don't serialize against each other. Creation upgrades to an
  /// exclusive lock and re-checks to defeat the race where two
  /// concurrent first-touches both miss the cache (RocksDB rejects
  /// duplicate creates on the same name).
  rocksdb::ColumnFamilyHandle *GetOrCreateColumnFamily(const std::string &table_name)
      ABSL_LOCKS_EXCLUDED(cf_mutex_);

  /// Write the cluster-ID marker on first open or validate it on
  /// subsequent opens. RAY_CHECK-fails on mismatch when
  /// `expected_cluster_id` is non-empty; no-op when empty.
  void ValidateOrWriteClusterIdMarker(const std::string &expected_cluster_id);

  /// WriteOptions for a mutating op on \p table_name. Uses `sync = true`
  /// (fsync-on-WAL before ack) unless \p table_name is in
  /// `SoftDurableTables()`, in which case it uses `sync = false`. Calls
  /// with no table (cluster-id marker, job counter) always fsync.
  rocksdb::WriteOptions SyncWriteOptions(const std::string &table_name = "") const;

  /// Increment and durably persist the job counter under
  /// `job_id_mutex_`, returning the new value. The internal
  /// implementation behind `AsyncGetNextJobID`; runs on the offload pool
  /// via `RunIoUnordered`. Not part of the `StoreClient` interface.
  int GetNextJobIDSync();

  /// Dispatch \p work for a single-key operation: posts to the strand
  /// bucketed by `hash(table_name, key)`, so two operations for the same
  /// key always execute in submission order.
  void RunIoForKey(const std::string &table_name,
                   const std::string &key,
                   std::function<void()> work);

  /// Dispatch \p work for an op whose ordering is intentionally loose
  /// (multi-key probes, scans, the global job-counter increment which
  /// uses its own internal mutex): posts to the base pool, no strand.
  void RunIoUnordered(std::function<void()> work);

  static constexpr char kClusterIdKey[] = "__ray_cluster_id__";
  static constexpr char kJobCounterKey[] = "__ray_job_counter__";

  /// Offload pool for RocksDB I/O.
  ///
  /// Safety on destruction depends on TWO things, not just declaration
  /// order:
  ///   1. The explicit destructor drains the pool via `io_pool_->wait()`
  ///      BEFORE any cf_handles_ / db_ teardown. `wait()` lets every
  ///      queued and running handler complete; `~thread_pool` would
  ///      instead call `stop()`, which cancels pending handlers and
  ///      silently drops their captured Postable callbacks (a caller
  ///      awaiting the ack would hang). The explicit drain is the
  ///      load-bearing piece — see the destructor body for the full
  ///      rationale.
  ///   2. Declaration order keeps `io_pool_` after `db_` so even in the
  ///      event of an exception during destruction or a future refactor
  ///      that loses the explicit drain, implicit member destruction
  ///      still tears down the pool before the DB. This is a defense-
  ///      in-depth fallback; it does NOT by itself prevent the
  ///      stop()-cancels-handlers problem above.
  /// Bounded ColumnFamilyOptions (shared block cache + small write buffer)
  /// applied to every column family, including those created lazily at
  /// runtime by GetOrCreateColumnFamily. Declared before `db_` so it
  /// outlives the DB; the shared_ptrs it holds (block cache) are also
  /// retained internally by `db_`.
  rocksdb::ColumnFamilyOptions cf_options_;

  std::unique_ptr<rocksdb::DB> db_;
  std::unique_ptr<boost::asio::thread_pool> io_pool_;

  /// Per-key strands bucketed by `hash(table, key)`. Each strand wraps
  /// `io_pool_`'s executor, so destruction order requires draining the
  /// pool before clearing this vector — the destructor handles that
  /// explicitly.
  using StrandT = boost::asio::strand<boost::asio::thread_pool::executor_type>;
  std::vector<std::unique_ptr<StrandT>> strands_;

  absl::Mutex cf_mutex_;
  absl::flat_hash_map<std::string, rocksdb::ColumnFamilyHandle *> cf_handles_
      ABSL_GUARDED_BY(cf_mutex_);

  absl::Mutex job_id_mutex_;
  int job_id_ ABSL_GUARDED_BY(job_id_mutex_) = 0;
};

}  // namespace gcs
}  // namespace ray
