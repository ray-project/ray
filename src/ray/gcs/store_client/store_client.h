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

#include <memory>
#include <string>

#include "ray/common/asio/io_service_pool.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {

/// \class StoreClient
/// Abstract interface of the storage client.
class StoreClient {
 public:
  virtual ~StoreClient() = default;

  /// Write data to the given table asynchronously.
  ///
  /// \param table_name The name of the table to be written.
  /// \param key The key that will be written to the table.
  /// \param data The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPut(const std::string &table_name,
                          const std::string &key,
                          const std::string &data,
                          const StatusCallback &callback) = 0;

  /// Write data to the given table asynchronously.
  ///
  /// \param table_name The name of the table to be written.
  /// \param key The key that will be written to the table.
  /// \param index_key A secondary key that will be used for indexing the data.
  /// \param data The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPutWithIndex(const std::string &table_name,
                                   const std::string &key,
                                   const std::string &index_key,
                                   const std::string &data,
                                   const StatusCallback &callback) = 0;

  /// Get data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param key The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  virtual Status AsyncGet(const std::string &table_name,
                          const std::string &key,
                          const OptionalItemCallback<std::string> &callback) = 0;

  /// Get data by index from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param index_key The secondary key that will be used to get the indexed data.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  virtual Status AsyncGetByIndex(
      const std::string &table_name,
      const std::string &index_key,
      const MapCallback<std::string, std::string> &callback) = 0;

  /// Get all data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param callback Callback that will be called after data has been received.
  /// \return Status
  virtual Status AsyncGetAll(const std::string &table_name,
                             const MapCallback<std::string, std::string> &callback) = 0;

  /// Delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDelete(const std::string &table_name,
                             const std::string &key,
                             const StatusCallback &callback) = 0;

  /// Delete data from the given table asynchronously, this can delete
  /// key--value and index--key.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param key The key that will be deleted from the table.
  /// \param index_key The index key of the given key.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDeleteWithIndex(const std::string &table_name,
                                      const std::string &key,
                                      const std::string &index_key,
                                      const StatusCallback &callback) = 0;

  /// Batch delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param keys The keys that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncBatchDelete(const std::string &table_name,
                                  const std::vector<std::string> &keys,
                                  const StatusCallback &callback) = 0;

  /// Batch delete data from the given table asynchronously, this can delete all
  /// key--value data and index--key data.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param keys The keys that will be deleted from the table.
  /// \param index_keys The index keys of the given keys, they are in one-to-one
  ///                   correspondence
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncBatchDeleteWithIndex(const std::string &table_name,
                                           const std::vector<std::string> &keys,
                                           const std::vector<std::string> &index_keys,
                                           const StatusCallback &callback) = 0;

  /// Delete by index from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param index_key The secondary key that will be used to delete the indexed data.
  /// from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDeleteByIndex(const std::string &table_name,
                                    const std::string &index_key,
                                    const StatusCallback &callback) = 0;

  /// Get next job id by `INCR` "JobCounter" key synchronously.
  ///
  /// \return Next job id in integer representation.
  virtual int GetNextJobID() = 0;

 protected:
  StoreClient() = default;
};

}  // namespace gcs

}  // namespace ray
