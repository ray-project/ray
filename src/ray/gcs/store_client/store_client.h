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
#include <vector>

#include "ray/common/asio/io_service_pool.h"
#include "ray/common/asio/postable.h"
#include "ray/common/gcs_callback_types.h"
#include "ray/common/id.h"
#include "ray/common/status.h"

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
  /// \param overwrite Whether to overwrite existing values. Otherwise, the update
  ///   will be ignored.
  /// \param callback WARNING: it returns true if and only if A NEW ENTRY is added.
  /// Overwritten return false.
  virtual void AsyncPut(const std::string &table_name,
                        const std::string &key,
                        std::string data,
                        bool overwrite,
                        Postable<void(bool)> callback) = 0;

  /// Get data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param key The key to lookup from the table.
  /// \param callback returns the value or null.
  virtual void AsyncGet(const std::string &table_name,
                        const std::string &key,
                        ToPostable<OptionalItemCallback<std::string>> callback) = 0;

  /// Get all data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param callback returns the key value pairs in a map.
  virtual void AsyncGetAll(
      const std::string &table_name,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) = 0;

  /// Get all data from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param keys The keys to look up from the table.
  /// \param callback returns the key value pairs in a map for those keys that exist.
  virtual void AsyncMultiGet(
      const std::string &table_name,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) = 0;

  /// Delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param key The key that will be deleted from the table.
  /// \param callback returns true if an entry with matching key is deleted.
  virtual void AsyncDelete(const std::string &table_name,
                           const std::string &key,
                           Postable<void(bool)> callback) = 0;

  /// Batch delete data from the given table asynchronously.
  ///
  /// \param table_name The name of the table from which data is to be deleted.
  /// \param keys The keys that will be deleted from the table.
  /// \param callback returns the number of deleted entries.
  virtual void AsyncBatchDelete(const std::string &table_name,
                                const std::vector<std::string> &keys,
                                Postable<void(int64_t)> callback) = 0;

  /// Get next job id by `INCR` "JobCounter" key asynchronously.
  ///
  /// \param callback returns the next job id in integer representation.
  virtual void AsyncGetNextJobID(Postable<void(int)> callback) = 0;

  /// Get all the keys match the prefix from the given table asynchronously.
  ///
  /// \param table_name The name of the table to be read.
  /// \param prefix The prefix to be scaned.
  /// \param callback returns all matching keys in a vector.
  virtual void AsyncGetKeys(const std::string &table_name,
                            const std::string &prefix,
                            Postable<void(std::vector<std::string>)> callback) = 0;

  /// Check whether the key exists in the table.
  ///
  /// \param table_name The name of the table to be read.
  /// \param key The key to be checked.
  /// \param callback Returns true if such key exists.
  virtual void AsyncExists(const std::string &table_name,
                           const std::string &key,
                           Postable<void(bool)> callback) = 0;

 protected:
  StoreClient() = default;
};

}  // namespace gcs

}  // namespace ray
