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
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/io_service_pool.h"
#include "ray/util/logging.h"

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
  virtual Status AsyncPut(const std::string &table_name, const std::string &key,
                          const std::string &data, const StatusCallback &callback) = 0;

  /// Write data to the given table asynchronously.
  ///
  /// \param table_name The name of the table to be written.
  /// \param key The key that will be written to the table.
  /// \param index_key A secondary key that will be used for indexing the data.
  /// \param data The value of the key that will be written to the table.
  /// \param callback Callback that will be called after write finishes.
  /// \return Status
  virtual Status AsyncPutWithIndex(const std::string &table_name, const std::string &key,
                                   const std::string &index_key, const std::string &data,
                                   const StatusCallback &callback) = 0;

  /// Get data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param key The key to lookup from the table.
  /// \param callback Callback that will be called after read finishes.
  /// \return Status
  virtual Status AsyncGet(const std::string &table_name, const std::string &key,
                          const OptionalItemCallback<std::string> &callback) = 0;

  /// Get all data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param callback Callback that will be called after data has been received.
  /// If the callback return `has_more == true` mean there's more data to be received.
  /// \return Status
  virtual Status AsyncGetAll(
      const std::string &table_name,
      const SegmentedCallback<std::pair<std::string, std::string>> &callback) = 0;

  /// Delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param key The key that will be deleted from the table.
  /// \param callback Callback that will be called after delete finishes.
  /// \return Status
  virtual Status AsyncDelete(const std::string &table_name, const std::string &key,
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

 protected:
  StoreClient() = default;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_STORE_CLIENT_STORE_CLIENT_H
