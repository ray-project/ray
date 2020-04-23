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

#include "absl/synchronization/mutex.h"
#include "ray/gcs/store_client/store_client.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class InMemoryTable
///
/// This class is thread safe.
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
  std::unordered_map<std::string, std::string> GetAll();

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
  absl::Mutex mutex_;
  std::unordered_map<std::string, std::string> records_ GUARDED_BY(mutex_);
  std::unordered_map<std::string, std::vector<std::string>> index_keys_
      GUARDED_BY(mutex_);
};

/// \class InMemoryStoreClient
///
/// This class is thread safe.
template <typename Key, typename Data, typename IndexKey>
class InMemoryStoreClient : public StoreClient<Key, Data, IndexKey> {
 public:
  InMemoryStoreClient() {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));
  }

  virtual ~InMemoryStoreClient() {
    io_service_.stop();
    thread_io_service_->join();
  }

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

  absl::Mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<InMemoryTable>> tables_
      GUARDED_BY(mutex_);
  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_IN_MEMORY_STORE_CLIENT_H
