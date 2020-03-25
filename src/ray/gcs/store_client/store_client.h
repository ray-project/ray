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

#ifndef RAY_GCS_STORE_CLIENT_STORE_CLIENT_H
#define RAY_GCS_STORE_CLIENT_STORE_CLIENT_H

#include <memory>
#include <string>
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

/// \class StoreClient
/// Abstract interface of the storage client.
///
/// To read and write from the storage, `Connect()` must be called and return Status::OK.
/// Before exit, `Disconnect()` must be called.
template <typename Key, typename Data, typename SecondaryKey>
class StoreClient {
 public:
  virtual ~StoreClient() {}

  /// Connect to storage. Non-thread safe.
  ///
  /// \return Status
  virtual Status Connect(std::shared_ptr<IOServicePool> io_service_pool) = 0;

  /// Disconnect with storage. Non-thread safe.
  virtual void Disconnect() = 0;

  /// Write data to the given table asynchronously.
  ///
  /// \param table_name The name of the table to be write.
  /// \param key The key that will be write to the table.
  /// \param value The value of the key that will be write to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPut(const std::string &table_name, const Key &key,
                          const Data &value, const StatusCallback &callback) = 0;

  /// Write data to the given table asynchronously.
  ///
  /// \param table_name The name of the table to be written.
  /// \param key The key that will be write to the table.
  /// \param index_key A secondary key that will be used for indexing the data.
  /// \param value The value of the key that will be write to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPutWithIndex(const std::string &table_name, const Key &key,
                                   const SecondaryKey &index_key, const Data &value,
                                   const StatusCallback &callback) = 0;

  /// Get data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param key The key that will be read from the table.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  virtual Status AsyncGet(const std::string &table_name, const Key &key,
                          const OptionalItemCallback<Data> &callback) = 0;

  /// Get all data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param callback Callback that will be called when receives data of the table.
  /// If the callback return `has_more == true` mean there's more data will be received.
  /// \return Status
  virtual Status AsyncGetAll(const std::string &table_name,
                             const SegmentedCallback<std::pair<Key, Data>> &callback) = 0;

  /// Delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table which data is to be deleted.
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDelete(const std::string &table_name, const Key &key,
                             const StatusCallback &callback) = 0;

  /// Delete by index from the given table asynchronously.
  ///
  /// \param table_name The name of the table which data is to be deleted.
  /// \param index_key The secondary key that will be used to delete the indexed data.
  /// from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDeleteByIndex(const std::string &table_name,
                                    const SecondaryKey &index_key,
                                    const StatusCallback &callback) = 0;

 protected:
  StoreClient() {}

  std::shared_ptr<IOServicePool> io_service_pool_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_STORE_CLIENT_H
