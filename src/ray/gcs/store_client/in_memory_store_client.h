// Copyright 2017 The Ray Authors.
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

#include <functional>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/node_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray::gcs {

/// \class InMemoryStoreClient
/// Please refer to StoreClient for API semantics.
///
/// This class is thread safe.
class InMemoryStoreClient : public StoreClient {
 public:
  explicit InMemoryStoreClient() = default;

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  std::string data,
                  bool overwrite,
                  Postable<void(bool)> callback) override;

  Status AsyncGet(const std::string &table_name,
                  const std::string &key,
                  ToPostable<OptionalItemCallback<std::string>> callback) override;

  Status AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  Status AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) override;

  Status AsyncDelete(const std::string &table_name,
                     const std::string &key,
                     Postable<void(bool)> callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          Postable<void(int64_t)> callback) override;

  Status AsyncGetNextJobID(Postable<void(int)> callback) override;

  Status AsyncGetKeys(const std::string &table_name,
                      const std::string &prefix,
                      Postable<void(std::vector<std::string>)> callback) override;

  Status AsyncExists(const std::string &table_name,
                     const std::string &key,
                     Postable<void(bool)> callback) override;

 private:
  struct InMemoryTable {
    /// Mutex to protect the records_ field and the index_keys_ field.
    mutable absl::Mutex mutex_;
    // Mapping from key to data.
    // TODO(dayshah): benchmark reader/writer locks against boost::concurrent_flat_map
    absl::flat_hash_map<std::string, std::string> records_ ABSL_GUARDED_BY(mutex_);
  };

  // The returned reference is valid as long as the InMemoryStoreClient is alive and
  // as long as no other thread erases the InMemoryTable from tables_.
  InMemoryTable &GetOrCreateMutableTable(const std::string &table_name);

  // 1) Will return nullptr if the table does not exist.
  // 2) The returned pointer is valid as long as the InMemoryStoreClient is alive and
  //    as long as no other thread erases the InMemoryTable from tables_.
  const InMemoryTable *GetTable(const std::string &table_name);

  /// Mutex to protect the tables_ field.
  absl::Mutex mutex_;
  // node_hash_map to keep pointer validity and allow for InMemoryTable which holds a
  // mutex to be held without extra heap alloation, most operations done on this are just
  // find, for which performance is almost identical to flat_hash_map.
  // Note: Do not erase from this as it will invalidate pointer from GetTable.
  absl::node_hash_map<std::string, InMemoryTable> tables_ ABSL_GUARDED_BY(mutex_);

  /// Current job id, auto-increment when request next-id.
  std::atomic<int> job_id_ = 1;
};

}  // namespace ray::gcs
