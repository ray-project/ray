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

#ifndef RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H

#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class InMemoryTable
///
/// Non-thread safe.
class InMemoryTable {
 public:
  /// Write data to the table synchronously.
  ///
  /// \param key The key that will be written to the table.
  /// \param data The value of the key that will be written to the table.
  void Put(const std::string &key, const std::string &data);

  /// Write data to the table synchronously.
  ///
  /// \param key The key that will be written to the table.
  /// \param index_key A secondary key that will be used for indexing the data.
  /// \param data The value of the key that will be written to the table.
  void PutWithIndex(const std::string &key, const std::string &index_key,
                    const std::string &data);

  /// Get data from the table synchronously.
  ///
  /// \param key The key to lookup from the table.
  /// \return The value of the key.
  boost::optional<std::string> Get(const std::string &key);

  /// Get all data from the table synchronously.
  ///
  /// \return All data of the table.
  absl::flat_hash_map<std::string, std::string> GetAll();

  /// Delete data from the table synchronously.
  ///
  /// \param key The key that will be deleted from the table.
  void Delete(const std::string &key);

  /// Delete by index from the table synchronously.
  ///
  /// \param index_key The secondary key that will be used to delete the indexed data from
  /// the table.
  void DeleteByIndex(const std::string &index_key);

 private:
  // Mapping from key to data.
  absl::flat_hash_map<std::string, std::string> records_;

  // Mapping from index key to keys.
  absl::flat_hash_map<std::string, std::vector<std::string>> index_keys_;
};

/// \class InMemoryStoreClient
///
/// This class is thread safe.
template <typename Key, typename Data, typename IndexKey>
class InMemoryStoreClient : public StoreClient<Key, Data, IndexKey> {
 public:
  explicit InMemoryStoreClient(boost::asio::io_service &main_io_service,
                               size_t io_service_num = 1)
      : main_io_service_(main_io_service) {
    io_service_pool_.reset(new IOServicePool(io_service_num));
    io_service_pool_->Run();
  }

  virtual ~InMemoryStoreClient() { io_service_pool_->Stop(); }

  Status AsyncPut(const std::string &table_name, const Key &key, const Data &data,
                  const StatusCallback &callback) override;

  Status AsyncPutWithIndex(const std::string &table_name, const Key &key,
                           const IndexKey &index_key, const Data &data,
                           const StatusCallback &callback) override;

  Status AsyncGet(const std::string &table_name, const Key &key,
                  const OptionalItemCallback<Data> &callback) override;

  Status AsyncGetAll(const std::string &table_name,
                     const SegmentedCallback<std::pair<Key, Data>> &callback) override;

  Status AsyncDelete(const std::string &table_name, const Key &key,
                     const StatusCallback &callback) override;

  Status AsyncDeleteByIndex(const std::string &table_name, const IndexKey &index_key,
                            const StatusCallback &callback) override;

 private:
  std::shared_ptr<InMemoryTable> GetOrCreateTable(const std::string &table_name);

  /// Mutex to protect the tables_ field.
  absl::Mutex mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<InMemoryTable>> tables_
      GUARDED_BY(mutex_);

  std::unique_ptr<IOServicePool> io_service_pool_;

  /// Async API Callback needs to post to main_io_service_ to ensure the orderly execution
  /// of the callback.
  boost::asio::io_service &main_io_service_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H
