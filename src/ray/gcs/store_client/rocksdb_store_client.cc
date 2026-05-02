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

#include "ray/gcs/store_client/rocksdb_store_client.h"

#include <algorithm>
#include <cstddef>
#include <functional>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "boost/asio/post.hpp"
#include "ray/util/logging.h"
#include "rocksdb/options.h"

namespace ray {
namespace gcs {

namespace {

constexpr char kDefaultCFName[] = "default";

rocksdb::Options BuildDbOptions() {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  // Per REP: hardcoded conservative settings sized for the GCS metadata
  // workload (10–100 MB). Phase 7 / Phase 8 follow-on will revisit some
  // of these (compression at L2+, write buffer sizing).
  options.IncreaseParallelism(2);
  options.OptimizeLevelStyleCompaction();
  return options;
}

}  // namespace

rocksdb::WriteOptions RocksDbStoreClient::SyncWriteOptions() const {
  rocksdb::WriteOptions wo;
  // REP §"Durability and Consistency Semantics": fsync per write is the
  // contract. Phase 4 verifies this on real substrates.
  wo.sync = true;
  return wo;
}

RocksDbStoreClient::RocksDbStoreClient(instrumented_io_context &io_service,
                                       const std::string &db_path,
                                       const std::string &expected_cluster_id,
                                       bool offload_io,
                                       std::size_t io_pool_size)
    : io_service_(io_service) {
  if (offload_io) {
    // Boost requires >=1 thread; clamp here so pool_size=0 from a bad
    // config can't crash the GCS at startup.
    io_pool_ =
        std::make_unique<boost::asio::thread_pool>(io_pool_size > 0 ? io_pool_size : 1);
  }
  RAY_CHECK(!db_path.empty()) << "RAY_GCS_STORAGE_PATH must be set when "
                                 "RAY_GCS_STORAGE=rocksdb.";

  // List existing CFs so we can open them all. On a fresh DB this
  // returns just `default`.
  rocksdb::Options list_options;
  std::vector<std::string> cf_names;
  auto list_status = rocksdb::DB::ListColumnFamilies(list_options, db_path, &cf_names);
  if (!list_status.ok()) {
    cf_names = {kDefaultCFName};
  }
  if (std::find(cf_names.begin(), cf_names.end(), kDefaultCFName) == cf_names.end()) {
    cf_names.push_back(kDefaultCFName);
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.reserve(cf_names.size());
  for (const auto &name : cf_names) {
    descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }

  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  rocksdb::DB *raw_db = nullptr;
  auto open_status =
      rocksdb::DB::Open(BuildDbOptions(), db_path, descriptors, &handles, &raw_db);
  RAY_CHECK(open_status.ok()) << "Failed to open RocksDB at " << db_path << ": "
                              << open_status.ToString();
  db_.reset(raw_db);

  RAY_CHECK_EQ(handles.size(), descriptors.size());
  {
    absl::MutexLock lock(&cf_mutex_);
    for (size_t i = 0; i < handles.size(); ++i) {
      cf_handles_[descriptors[i].name] = handles[i];
    }
  }

  ValidateOrWriteClusterIdMarker(expected_cluster_id);

  // Recover the persisted job counter. Default 0 if the DB is fresh.
  // No mutex needed: the constructor runs before any other thread can
  // observe `this`. Use std::stoll + an explicit range check so a
  // corrupted-or-overflowed counter on disk produces a clear FATAL
  // rather than an unrecoverable std::out_of_range.
  std::string counter_value;
  auto get_status = db_->Get(
      rocksdb::ReadOptions(), db_->DefaultColumnFamily(), kJobCounterKey, &counter_value);
  if (get_status.ok()) {
    long long parsed = 0;
    try {
      parsed = std::stoll(counter_value);
    } catch (const std::exception &e) {
      RAY_LOG(FATAL) << "RocksDB job counter is corrupt at " << db_path << ": "
                     << counter_value << " (" << e.what() << ")";
    }
    if (parsed < 0 || parsed > std::numeric_limits<int>::max()) {
      RAY_LOG(FATAL) << "RocksDB job counter at " << db_path
                     << " is out of int range: " << counter_value;
    }
    job_id_ = static_cast<int>(parsed);
  } else {
    RAY_CHECK(get_status.IsNotFound())
        << "Unexpected RocksDB Get error for job counter: " << get_status.ToString();
  }
}

RocksDbStoreClient::~RocksDbStoreClient() {
  // Drain any in-flight offloaded work BEFORE we touch db_/cf handles —
  // a pool task that ran after DestroyColumnFamilyHandle would dereference
  // a freed handle. boost::asio::thread_pool::~thread_pool also joins,
  // but we need the join to happen before the cf-handle-destroy block
  // below, not after the body returns.
  if (io_pool_) {
    io_pool_->stop();
    io_pool_->join();
  }

  absl::MutexLock lock(&cf_mutex_);
  for (auto &[_, handle] : cf_handles_) {
    db_->DestroyColumnFamilyHandle(handle);
  }
  cf_handles_.clear();
}

void RocksDbStoreClient::RunIo(std::function<void()> work) {
  if (io_pool_) {
    boost::asio::post(*io_pool_, std::move(work));
  } else {
    work();
  }
}

void RocksDbStoreClient::ValidateOrWriteClusterIdMarker(
    const std::string &expected_cluster_id) {
  std::string existing;
  auto get_status = db_->Get(
      rocksdb::ReadOptions(), db_->DefaultColumnFamily(), kClusterIdKey, &existing);

  if (get_status.IsNotFound()) {
    if (!expected_cluster_id.empty()) {
      auto put_status = db_->Put(SyncWriteOptions(),
                                 db_->DefaultColumnFamily(),
                                 kClusterIdKey,
                                 expected_cluster_id);
      RAY_CHECK(put_status.ok())
          << "Failed to write cluster ID marker: " << put_status.ToString();
    }
    return;
  }

  RAY_CHECK(get_status.ok()) << "Unexpected RocksDB Get error for cluster marker: "
                             << get_status.ToString();

  // REP §"Stale data protection": fail-fast if the marker disagrees,
  // rather than silently loading another cluster's state. No-op when
  // expected_cluster_id is empty (the production wiring today).
  if (!expected_cluster_id.empty() && existing != expected_cluster_id) {
    RAY_LOG(FATAL) << "RocksDB at this path belongs to cluster '" << existing
                   << "' but this GCS expected cluster '" << expected_cluster_id
                   << "'. Refusing to load stale state.";
  }
}

rocksdb::ColumnFamilyHandle *RocksDbStoreClient::GetOrCreateColumnFamily(
    const std::string &table_name) {
  // Hold cf_mutex_ across CreateColumnFamily so concurrent first-touches
  // of the same table don't both call into rocksdb (which would reject
  // the second create as InvalidArgument). Steady-state lookups hit the
  // cache fast-path before the create is needed.
  absl::MutexLock lock(&cf_mutex_);
  auto it = cf_handles_.find(table_name);
  if (it != cf_handles_.end()) {
    return it->second;
  }
  rocksdb::ColumnFamilyHandle *new_handle = nullptr;
  auto status =
      db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), table_name, &new_handle);
  RAY_CHECK(status.ok()) << "Failed to create column family '" << table_name
                         << "': " << status.ToString();
  cf_handles_[table_name] = new_handle;
  return new_handle;
}

void RocksDbStoreClient::AsyncPut(const std::string &table_name,
                                  const std::string &key,
                                  std::string data,
                                  bool overwrite,
                                  Postable<void(bool)> callback) {
  RunIo([this,
         table_name,
         key,
         data = std::move(data),
         overwrite,
         callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    bool inserted = true;
    if (!overwrite) {
      // Honour the !overwrite contract: only write if the key is absent.
      //
      // Inline path (offload_io=false, the default): RunIo executes
      // synchronously on the issuer's thread, so the GCS event loop's
      // single-writer model gives Get-then-Put atomicity for free.
      //
      // Offload path (offload_io=true): the Get and the Put run on a
      // pool thread, so two concurrent AsyncPut calls for the same key
      // can both observe "not found" and both write. The window is real
      // but bounded — only !overwrite callers see it, only when offload
      // is enabled, and only for the same key. Production hardening
      // would use either RocksDB's OptimisticTransactionDB or a per-key
      // strand keyed by hash(table, key); tracked as an open question
      // in the PR description.
      std::string existing;
      auto get_status = db_->Get(rocksdb::ReadOptions(), cf, key, &existing);
      if (get_status.ok()) {
        std::move(callback).Post("GcsRocksDb.PutSkip", false);
        return;
      }
      RAY_CHECK(get_status.IsNotFound())
          << "RocksDB Get failed: " << get_status.ToString();
    } else {
      std::string existing;
      auto get_status = db_->Get(rocksdb::ReadOptions(), cf, key, &existing);
      if (get_status.ok()) {
        inserted = false;
      } else {
        RAY_CHECK(get_status.IsNotFound())
            << "RocksDB Get failed: " << get_status.ToString();
      }
    }

    auto put_status = db_->Put(SyncWriteOptions(), cf, key, std::move(data));
    RAY_CHECK(put_status.ok()) << "RocksDB Put failed for table=" << table_name
                               << " key=" << key << ": " << put_status.ToString();

    std::move(callback).Post("GcsRocksDb.Put", inserted);
  });
}

void RocksDbStoreClient::AsyncGet(
    const std::string &table_name,
    const std::string &key,
    ToPostable<rpc::OptionalItemCallback<std::string>> callback) {
  RunIo([this, table_name, key, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    std::string raw_value;
    auto status = db_->Get(rocksdb::ReadOptions(), cf, key, &raw_value);

    std::optional<std::string> data;
    if (status.ok()) {
      data = std::move(raw_value);
    } else if (!status.IsNotFound()) {
      RAY_LOG(FATAL) << "RocksDB Get failed for table=" << table_name << " key=" << key
                     << ": " << status.ToString();
    }
    std::move(callback).Post("GcsRocksDb.Get", Status::OK(), std::move(data));
  });
}

void RocksDbStoreClient::AsyncGetAll(
    const std::string &table_name,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  RunIo([this, table_name, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    rocksdb::ReadOptions read_opts;
    read_opts.total_order_seek = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_opts, cf));

    absl::flat_hash_map<std::string, std::string> result;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      result.emplace(it->key().ToString(), it->value().ToString());
    }
    RAY_CHECK(it->status().ok())
        << "RocksDB iterator failed during GetAll: " << it->status().ToString();

    std::move(callback).Post("GcsRocksDb.GetAll", std::move(result));
  });
}

void RocksDbStoreClient::AsyncMultiGet(
    const std::string &table_name,
    const std::vector<std::string> &keys,
    Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) {
  if (keys.empty()) {
    absl::flat_hash_map<std::string, std::string> result;
    std::move(callback).Post("GcsRocksDb.MultiGet", std::move(result));
    return;
  }

  RunIo([this, table_name, keys, callback = std::move(callback)]() mutable {
    absl::flat_hash_map<std::string, std::string> result;
    auto *cf = GetOrCreateColumnFamily(table_name);
    std::vector<rocksdb::ColumnFamilyHandle *> cfs(keys.size(), cf);
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto &k : keys) key_slices.emplace_back(k);

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses =
        db_->MultiGet(rocksdb::ReadOptions(), cfs, key_slices, &values);
    RAY_CHECK_EQ(statuses.size(), keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
      if (statuses[i].ok()) {
        result.emplace(keys[i], std::move(values[i]));
      } else if (!statuses[i].IsNotFound()) {
        RAY_LOG(FATAL) << "RocksDB MultiGet failed for key=" << keys[i] << ": "
                       << statuses[i].ToString();
      }
    }

    std::move(callback).Post("GcsRocksDb.MultiGet", std::move(result));
  });
}

void RocksDbStoreClient::AsyncDelete(const std::string &table_name,
                                     const std::string &key,
                                     Postable<void(bool)> callback) {
  RunIo([this, table_name, key, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    std::string existing;
    auto get_status = db_->Get(rocksdb::ReadOptions(), cf, key, &existing);
    bool existed = get_status.ok();
    if (!existed && !get_status.IsNotFound()) {
      RAY_LOG(FATAL) << "RocksDB Get during Delete failed: " << get_status.ToString();
    }

    // Skip the synchronous Delete (and its fsync) when the key was
    // already absent. RocksDB would otherwise write a tombstone that
    // costs a full ~3.8 ms fsync on durable storage and adds a stale
    // entry that compaction has to reap later. Callback contract is
    // unchanged: the bool reports whether the key existed at the
    // moment of the read.
    if (existed) {
      auto del_status = db_->Delete(SyncWriteOptions(), cf, key);
      RAY_CHECK(del_status.ok()) << "RocksDB Delete failed for table=" << table_name
                                 << " key=" << key << ": " << del_status.ToString();
    }

    std::move(callback).Post("GcsRocksDb.Delete", existed);
  });
}

void RocksDbStoreClient::AsyncBatchDelete(const std::string &table_name,
                                          const std::vector<std::string> &keys,
                                          Postable<void(int64_t)> callback) {
  if (keys.empty()) {
    std::move(callback).Post("GcsRocksDb.BatchDelete", static_cast<int64_t>(0));
    return;
  }

  RunIo([this, table_name, keys, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    // Batch the existence probe via MultiGet so we issue one I/O round
    // instead of N sequential Gets. Mirrors AsyncMultiGet's structure.
    std::vector<rocksdb::ColumnFamilyHandle *> cfs(keys.size(), cf);
    std::vector<rocksdb::Slice> key_slices;
    key_slices.reserve(keys.size());
    for (const auto &k : keys) key_slices.emplace_back(k);

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses =
        db_->MultiGet(rocksdb::ReadOptions(), cfs, key_slices, &values);
    RAY_CHECK_EQ(statuses.size(), keys.size());

    rocksdb::WriteBatch batch;
    int64_t deleted_count = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
      if (statuses[i].ok()) {
        ++deleted_count;
      } else if (!statuses[i].IsNotFound()) {
        RAY_LOG(FATAL) << "RocksDB Get during BatchDelete failed for key=" << keys[i]
                       << ": " << statuses[i].ToString();
      }
      auto bs = batch.Delete(cf, keys[i]);
      RAY_CHECK(bs.ok()) << "WriteBatch Delete failed: " << bs.ToString();
    }
    auto write_status = db_->Write(SyncWriteOptions(), &batch);
    RAY_CHECK(write_status.ok())
        << "RocksDB BatchDelete write failed: " << write_status.ToString();

    std::move(callback).Post("GcsRocksDb.BatchDelete", deleted_count);
  });
}

void RocksDbStoreClient::AsyncGetNextJobID(Postable<void(int)> callback) {
  RunIo([this, callback = std::move(callback)]() mutable {
    int next = GetNextJobIDSync();
    std::move(callback).Post("GcsRocksDb.GetNextJobID", next);
  });
}

void RocksDbStoreClient::AsyncGetKeys(const std::string &table_name,
                                      const std::string &prefix,
                                      Postable<void(std::vector<std::string>)> callback) {
  RunIo([this, table_name, prefix, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    // Byte-ordered prefix scan: Seek to the prefix and walk forward while
    // keys still share the prefix. Once a key fails the prefix check, no
    // later key can pass, so we stop.
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions(), cf));
    std::vector<std::string> result;
    for (it->Seek(prefix); it->Valid(); it->Next()) {
      if (!it->key().starts_with(rocksdb::Slice(prefix))) break;
      result.emplace_back(it->key().ToString());
    }
    RAY_CHECK(it->status().ok())
        << "RocksDB iterator failed during GetKeys: " << it->status().ToString();

    std::move(callback).Post("GcsRocksDb.GetKeys", std::move(result));
  });
}

void RocksDbStoreClient::AsyncExists(const std::string &table_name,
                                     const std::string &key,
                                     Postable<void(bool)> callback) {
  RunIo([this, table_name, key, callback = std::move(callback)]() mutable {
    auto *cf = GetOrCreateColumnFamily(table_name);

    std::string v;
    auto status = db_->Get(rocksdb::ReadOptions(), cf, key, &v);
    bool exists = status.ok();
    if (!exists && !status.IsNotFound()) {
      RAY_LOG(FATAL) << "RocksDB Get for AsyncExists failed: " << status.ToString();
    }

    std::move(callback).Post("GcsRocksDb.Exists", exists);
  });
}

int RocksDbStoreClient::GetNextJobIDSync() {
  absl::MutexLock lock(&job_id_mutex_);
  job_id_ += 1;
  // Persist so the counter survives restart. fsync semantics match the
  // rest of the StoreClient writes.
  auto status = db_->Put(SyncWriteOptions(),
                         db_->DefaultColumnFamily(),
                         kJobCounterKey,
                         std::to_string(job_id_));
  RAY_CHECK(status.ok()) << "RocksDB Put for job counter failed: " << status.ToString();
  return job_id_;
}

}  // namespace gcs
}  // namespace ray
