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

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/store_client/store_client.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class InMemoryStoreClient
///
/// This class is thread safe.
class InMemoryStoreClient : public StoreClient {
 public:
  explicit InMemoryStoreClient(instrumented_io_context &main_io_service)
      : main_io_service_(main_io_service) {}

  Status AsyncPut(const std::string &table_name,
                  const std::string &key,
                  const std::string &data,
                  const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name,
                           const std::string &key,
                           const std::string &index_key,
                           const std::string &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name,
                  const std::string &key,
                  const OptionalItemCallback<std::string> &callback) override;

  Status AsyncGetByIndex(const std::string &table_name,
                         const std::string &index_key,
                         const MapCallback<std::string, std::string> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const MapCallback<std::string, std::string> &callback) override;

  Status AsyncDelete(const std::string &table_name,
                     const std::string &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteWithIndex(const std::string &table_name,
                              const std::string &key,
                              const std::string &index_key,
                              const StatusCallback &callback) override;

  Status AsyncBatchDelete(const std::string &table_name,
                          const std::vector<std::string> &keys,
                          const StatusCallback &callback) override;

  Status AsyncBatchDeleteWithIndex(const std::string &table_name,
                                   const std::vector<std::string> &keys,
                                   const std::vector<std::string> &index_keys,
                                   const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name,
                            const std::string &index_key,
                            const StatusCallback &callback) override;

  int GetNextJobID() override;

 private:
  struct InMemoryTable {
    /// Mutex to protect the records_ field and the index_keys_ field.
    absl::Mutex mutex_;
    // Mapping from key to data.
    absl::flat_hash_map<std::string, std::string> records_ GUARDED_BY(mutex_);
    // Mapping from index key to keys.
    absl::flat_hash_map<std::string, std::vector<std::string>> index_keys_
        GUARDED_BY(mutex_);
  };

  std::shared_ptr<InMemoryStoreClient::InMemoryTable> GetOrCreateTable(
      const std::string &table_name);

  /// Mutex to protect the tables_ field.
  absl::Mutex mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<InMemoryTable>> tables_
      GUARDED_BY(mutex_);

  /// Async API Callback needs to post to main_io_service_ to ensure the orderly execution
  /// of the callback.
  instrumented_io_context &main_io_service_;

  int job_id_ = 0;
};

}  // namespace gcs

}  // namespace ray
