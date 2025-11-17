// Copyright 2025 The Ray Authors.
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

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "ray/common/gcs_callback_types.h"
#include "ray/common/status.h"

namespace ray {
namespace gcs {

/**
  @interface InternalKVAccessorInterface

  Interface for accessing the internal control plane key-value store
 */
class InternalKVAccessorInterface {
 public:
  virtual ~InternalKVAccessorInterface() = default;

  /**
    Asynchronously list keys with prefix stored in internal kv

    @param  ns The namespace to scan.
    @param  prefix The prefix to scan.
    @param  timeout_ms -1 means infinite.
    @param callback Callback that will be called after scanning.
   */
  virtual void AsyncInternalKVKeys(
      const std::string &ns,
      const std::string &prefix,
      const int64_t timeout_ms,
      const OptionalItemCallback<std::vector<std::string>> &callback) = 0;

  /**
    Asynchronously get the value for a given key.

    @param ns The namespace to lookup.
    @param key The key to lookup.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after get the value.
   */
  virtual void AsyncInternalKVGet(const std::string &ns,
                                  const std::string &key,
                                  const int64_t timeout_ms,
                                  const OptionalItemCallback<std::string> &callback) = 0;

  /**
    Asynchronously get the value for multiple keys.

    @param ns The namespace to lookup.
    @param keys The keys to lookup.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after get the values.
   */
  virtual void AsyncInternalKVMultiGet(
      const std::string &ns,
      const std::vector<std::string> &keys,
      const int64_t timeout_ms,
      const OptionalItemCallback<std::unordered_map<std::string, std::string>>
          &callback) = 0;

  /**
    Asynchronously set the value for a given key.

    @param ns The namespace to put the key.
    @param key The key in <key, value> pair
    @param value The value associated with the key
    @param overwrite If it's true, it'll overwrite existing <key, value> if it exists.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation.
   */
  virtual void AsyncInternalKVPut(const std::string &ns,
                                  const std::string &key,
                                  const std::string &value,
                                  bool overwrite,
                                  const int64_t timeout_ms,
                                  const OptionalItemCallback<bool> &callback) = 0;

  /**
    Asynchronously check the existence of a given key

    @param ns The namespace to check.
    @param key The key to check.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation. Called with `true`
                    if the key is deleted; `false` if it doesn't exist.
   */
  virtual void AsyncInternalKVExists(const std::string &ns,
                                     const std::string &key,
                                     const int64_t timeout_ms,
                                     const OptionalItemCallback<bool> &callback) = 0;

  /**
    Asynchronously delete a key

    @param ns The namespace to delete from.
    @param key The key to delete.
    @param del_by_prefix If set to be true, delete all keys with prefix as `key`.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation. Called with number
                    of keys deleted.
   */
  virtual void AsyncInternalKVDel(const std::string &ns,
                                  const std::string &key,
                                  bool del_by_prefix,
                                  const int64_t timeout_ms,
                                  const OptionalItemCallback<int> &callback) = 0;

  /**
    List keys with prefix stored in internal kv

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to scan.
    @param prefix The prefix to scan.
    @param timeout_ms -1 means infinite.
    @param[out] value It's an output parameter. It'll be set to the keys with `prefix`
    @return Status
   */
  virtual Status Keys(const std::string &ns,
                      const std::string &prefix,
                      const int64_t timeout_ms,
                      std::vector<std::string> &value) = 0;

  /**
    Set the <key, value> in the store

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to put the key.
    @param key The key of the pair
    @param value The value of the pair
    @param overwrite If it's true, it'll overwrite existing <key, value> if it exists.
    @param timeout_ms -1 means infinite.
    @param[out] added It's an output parameter. It'll be set to be true if any row is
    added.
    @return Status
    @todo change the out parameter type to `int` just like AsyncInternalKVPut.
   */
  virtual Status Put(const std::string &ns,
                     const std::string &key,
                     const std::string &value,
                     bool overwrite,
                     const int64_t timeout_ms,
                     bool &added) = 0;

  /**
    Retrieve the value associated with a key

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to lookup.
    @param key The key to lookup.
    @param timeout_ms -1 means infinite.
    @param[out] value It's an output parameter. It'll be set to the value of the key
    @return Status
   */
  virtual Status Get(const std::string &ns,
                     const std::string &key,
                     const int64_t timeout_ms,
                     std::string &value) = 0;

  /**
    Retrieve the values associated with some keys

    @param ns The namespace to lookup.
    @param keys The keys to lookup.
    @param timeout_ms -1 means infinite.
    @param[out] values It's an output parameter. It'll be set to the values of the keys.
    @return Status
   */
  virtual Status MultiGet(const std::string &ns,
                          const std::vector<std::string> &keys,
                          const int64_t timeout_ms,
                          std::unordered_map<std::string, std::string> &values) = 0;

  /**
    Delete the key

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to delete from.
    @param key The key to delete
    @param del_by_prefix If set to be true, delete all keys with prefix as `key`.
    @param timeout_ms -1 means infinite.
    @param[out] num_deleted It's an output parameter. It'll be set to be number of keys
    deleted.
    @return Status
   */
  virtual Status Del(const std::string &ns,
                     const std::string &key,
                     bool del_by_prefix,
                     const int64_t timeout_ms,
                     int &num_deleted) = 0;

  /**
    Check existence of a key in the store

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to check.
    @param key The key to check
    @param timeout_ms -1 means infinite.
    @param[out] exists It's an output parameter. It'll be true if the key exists in the
                       system. Otherwise, it'll be set to be false.
    @return Status
   */
  virtual Status Exists(const std::string &ns,
                        const std::string &key,
                        const int64_t timeout_ms,
                        bool &exists) = 0;

  /**
    Get the internal config string from GCS.

    @param callback Processes a map of config options
   */
  virtual void AsyncGetInternalConfig(
      const OptionalItemCallback<std::string> &callback) = 0;
};

}  // namespace gcs
}  // namespace ray
