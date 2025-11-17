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

#include <functional>
#include <memory>

#include "ray/common/gcs_callback_types.h"
#include "ray/gcs_rpc_client/accessors/internal_kv_accessor_interface.h"
#include "ray/gcs_rpc_client/gcs_client_context.h"
#include "ray/rpc/rpc_callback_types.h"

namespace ray {
namespace gcs {

/**
  @class InternalKVAccessor

  Implementation of InternalKVAccessorInterface that accesses internal key-value store
  by querying the GCS.
 */
class InternalKVAccessor : public InternalKVAccessorInterface {
 public:
  InternalKVAccessor() = default;
  explicit InternalKVAccessor(GcsClientContext *context);
  virtual ~InternalKVAccessor() = default;

  /**
    Asynchronously list keys with prefix stored in internal kv

    @param  ns The namespace to scan.
    @param  prefix The prefix to scan.
    @param  timeout_ms -1 means infinite.
    @param callback Callback that will be called after scanning.
   */
  void AsyncInternalKVKeys(
      const std::string &ns,
      const std::string &prefix,
      const int64_t timeout_ms,
      const OptionalItemCallback<std::vector<std::string>> &callback) override;

  /**
    Asynchronously get the value for a given key.

    @param ns The namespace to lookup.
    @param key The key to lookup.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after get the value.
   */
  void AsyncInternalKVGet(const std::string &ns,
                          const std::string &key,
                          const int64_t timeout_ms,
                          const OptionalItemCallback<std::string> &callback) override;

  /**
    Asynchronously get the value for multiple keys.

    @param ns The namespace to lookup.
    @param keys The keys to lookup.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after get the values.
   */
  void AsyncInternalKVMultiGet(
      const std::string &ns,
      const std::vector<std::string> &keys,
      const int64_t timeout_ms,
      const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback)
      override;

  /**
    Asynchronously set the value for a given key.

    @param ns The namespace to put the key.
    @param key The key in <key, value> pair
    @param value The value associated with the key
    @param overwrite If it's true, it'll overwrite existing <key, value> if it exists.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation.
   */
  void AsyncInternalKVPut(const std::string &ns,
                          const std::string &key,
                          const std::string &value,
                          bool overwrite,
                          const int64_t timeout_ms,
                          const OptionalItemCallback<bool> &callback) override;

  /**
    Asynchronously check the existence of a given key

    @param ns The namespace to check.
    @param key The key to check.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation. Called with `true`
                    if the key is deleted; `false` if it doesn't exist.
   */
  void AsyncInternalKVExists(const std::string &ns,
                             const std::string &key,
                             const int64_t timeout_ms,
                             const OptionalItemCallback<bool> &callback) override;

  /**
    Asynchronously delete a key

    @param ns The namespace to delete from.
    @param key The key to delete.
    @param del_by_prefix If set to be true, delete all keys with prefix as `key`.
    @param timeout_ms -1 means infinite.
    @param callback Callback that will be called after the operation. Called with number
                    of keys deleted.
   */
  void AsyncInternalKVDel(const std::string &ns,
                          const std::string &key,
                          bool del_by_prefix,
                          const int64_t timeout_ms,
                          const OptionalItemCallback<int> &callback) override;

  /**
    List keys with prefix stored in internal kv

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to scan.
    @param prefix The prefix to scan.
    @param timeout_ms -1 means infinite.
    @param[out] value It's an output parameter. It'll be set to the keys with `prefix`
    @return Status
   */
  Status Keys(const std::string &ns,
              const std::string &prefix,
              const int64_t timeout_ms,
              std::vector<std::string> &value) override;

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
  Status Put(const std::string &ns,
             const std::string &key,
             const std::string &value,
             bool overwrite,
             const int64_t timeout_ms,
             bool &added) override;

  /**
    Retrieve the value associated with a key

    The RPC will timeout after the timeout_ms, or wait infinitely if timeout_ms is -1.

    @param ns The namespace to lookup.
    @param key The key to lookup.
    @param timeout_ms -1 means infinite.
    @param[out] value It's an output parameter. It'll be set to the value of the key
    @return Status
   */
  Status Get(const std::string &ns,
             const std::string &key,
             const int64_t timeout_ms,
             std::string &value) override;

  /**
    Retrieve the values associated with some keys

    @param ns The namespace to lookup.
    @param keys The keys to lookup.
    @param timeout_ms -1 means infinite.
    @param[out] values It's an output parameter. It'll be set to the values of the keys.
    @return Status
   */
  Status MultiGet(const std::string &ns,
                  const std::vector<std::string> &keys,
                  const int64_t timeout_ms,
                  std::unordered_map<std::string, std::string> &values) override;

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
  Status Del(const std::string &ns,
             const std::string &key,
             bool del_by_prefix,
             const int64_t timeout_ms,
             int &num_deleted) override;

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
  Status Exists(const std::string &ns,
                const std::string &key,
                const int64_t timeout_ms,
                bool &exists) override;

  /**
    Get the internal config string from GCS.

    @param callback Processes a map of config options
   */
  void AsyncGetInternalConfig(const OptionalItemCallback<std::string> &callback) override;

 private:
  GcsClientContext *context_;
};

}  // namespace gcs
}  // namespace ray
