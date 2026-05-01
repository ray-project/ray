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
#include "boost/asio/thread_pool.hpp"
#include "ray/common/asio/instrumented_io_context.h"
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
/// **Two execution paths, selected at construction:**
///
/// - *Inline (default).* Each call does its RocksDB work on the caller's
///   thread (mutating ops block on the WAL fsync). Mirrors
///   `InMemoryStoreClient` semantics and is the simplest to reason about.
///   Drawback flagged by code review: the WAL fsync (~3.8 ms p50 on
///   probe-verified ext4) blocks the GCS event loop, capping single-key
///   write throughput at ~250/s and adding tail latency to other GCS RPCs.
///
/// - *Offloaded* (`gcs_rocksdb_async_offload = true`). Each call posts
///   its RocksDB work to a small `boost::asio::thread_pool`; the
///   user-supplied callback still runs on `io_service_` via
///   `Postable::Post`. RocksDB's group-commit aggregates concurrent
///   in-flight writers into one fsync, so aggregate write throughput
///   scales with pool size while the event loop stays responsive.
///
/// In both paths the callback is dispatched via `Postable::Post` to the
/// GCS event loop, which keeps callback ordering uniform with the rest
/// of GCS.
///
/// **Durability.** Every mutating call uses `WriteOptions::sync = true`
/// so the WAL is fsynced before the callback fires. This is the
/// invariant Ray's GCS RPC layer relies on: a caller that received an
/// ack can assume the write survived a crash. Whether `fsync` actually
/// flushes to media is a property of the underlying volume; the POC's
/// Phase 1 fsync probe verifies the substrate before the durability
/// claim is trusted.
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
  ///   the K8s downward API plumbs in an external authoritative ID
  ///   (REP-64 Phase 8 follow-on; see `rep-64-poc/reports/phase-3-skeleton.md`).
  /// \param offload_io If true, RocksDB work runs on `io_pool_` (a
  ///   thread pool of \p io_pool_size threads) instead of the caller's
  ///   thread. The user callback always runs on \p io_service via
  ///   `Postable::Post` regardless. Production wiring is driven by the
  ///   `gcs_rocksdb_async_offload` config flag; tests pass it directly
  ///   so they can exercise both paths.
  /// \param io_pool_size Worker-thread count. Ignored when
  ///   `offload_io == false`. Clamped to >= 1 when active.
  RocksDbStoreClient(instrumented_io_context &io_service,
                     const std::string &db_path,
                     const std::string &expected_cluster_id,
                     bool offload_io = false,
                     std::size_t io_pool_size = 4);

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

  /// Synchronous helper retained for the POC concurrency tests, which
  /// exercise mutex-RMW correctness directly. Production code should
  /// use AsyncGetNextJobID via the StoreClient interface; this helper
  /// is not part of that interface and is not for use in GCS code.
  int GetNextJobIDSync();

 private:
  /// Look up the column family for \p table_name, creating it lazily on
  /// first use. The cf_mutex_ is held across the create call so two
  /// concurrent first-touches of the same table do not both attempt to
  /// create it (RocksDB rejects duplicate creates on the same name).
  rocksdb::ColumnFamilyHandle *GetOrCreateColumnFamily(const std::string &table_name)
      ABSL_LOCKS_EXCLUDED(cf_mutex_);

  /// Write the cluster-ID marker on first open or validate it on
  /// subsequent opens. RAY_CHECK-fails on mismatch when
  /// `expected_cluster_id` is non-empty; no-op when empty.
  void ValidateOrWriteClusterIdMarker(const std::string &expected_cluster_id);

  /// `WriteOptions{sync=true}` shorthand. fsync-on-WAL.
  rocksdb::WriteOptions SyncWriteOptions() const;

  /// Dispatch \p work either inline (when `io_pool_` is null) or onto
  /// the offload pool. The work closure runs the RocksDB op AND posts
  /// the user callback, so the pool-vs-inline branch lives only here.
  void RunIo(std::function<void()> work);

  static constexpr char kClusterIdKey[] = "__ray_rep64_cluster_id__";
  static constexpr char kJobCounterKey[] = "__ray_rep64_job_counter__";

  // Holds a ref so Postable's default-IO-context resolution still works,
  // matching how RedisStoreClient stores its io_service_.
  instrumented_io_context &io_service_;

  /// Offload pool for RocksDB I/O. Null when offload_io was false in
  /// the ctor. Joined and destroyed BEFORE `db_` so any in-flight RocksDB
  /// op completes against a still-live DB handle. (Hence declared after
  /// `db_` here — destructor walks members in reverse.)
  std::unique_ptr<rocksdb::DB> db_;
  std::unique_ptr<boost::asio::thread_pool> io_pool_;

  absl::Mutex cf_mutex_;
  absl::flat_hash_map<std::string, rocksdb::ColumnFamilyHandle *> cf_handles_
      ABSL_GUARDED_BY(cf_mutex_);

  absl::Mutex job_id_mutex_;
  int job_id_ ABSL_GUARDED_BY(job_id_mutex_) = 0;
};

}  // namespace gcs
}  // namespace ray
