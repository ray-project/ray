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

#include "absl/container/btree_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// \class InternalKVInterface
/// The interface for internal kv implementation. Ideally we should merge this
/// with store client, but due to compatibility issue, we keep them separated
/// right now.
class InternalKVInterface {
 public:
  /// Get the value associated with `key`.
  ///
  /// \param ns The namespace of the key.
  /// \param key The key to fetch.
  /// \param callback Returns the value or null if the key doesn't exist.
  virtual void Get(const std::string &ns,
                   const std::string &key,
                   std::function<void(std::optional<std::string>)> callback) = 0;

  /// Associate a key with the specified value.
  ///
  /// \param ns The namespace of the key.
  /// \param key The key for the pair.
  /// \param value The value for the pair.
  /// \param overwrite Whether to overwrite existing values. Otherwise, the update
  ///   will be ignored.
  /// \param callback WARNING: it returns true if and only if A NEW ENTRY is added.
  /// Overwritten return false.
  virtual void Put(const std::string &ns,
                   const std::string &key,
                   const std::string &value,
                   bool overwrite,
                   std::function<void(bool)> callback) = 0;

  /// Delete the key from the store.
  ///
  /// \param ns The namespace of the key.
  /// \param key The key to be deleted.
  /// \param del_by_prefix Whether to treat the key as prefix. If true, it'll
  ///     delete all keys with `key` as the prefix.
  /// \param callback returns the number of entries deleted.
  virtual void Del(const std::string &ns,
                   const std::string &key,
                   bool del_by_prefix,
                   std::function<void(int64_t)> callback) = 0;

  /// Check whether the key exists in the store.
  ///
  /// \param ns The namespace of the key.
  /// \param key The key to be checked.
  /// \param callback Callback function.
  virtual void Exists(const std::string &ns,
                      const std::string &key,
                      std::function<void(bool)> callback) = 0;

  /// Get the keys for a given prefix.
  ///
  /// \param ns The namespace of the prefix.
  /// \param prefix The prefix to be scaned.
  /// \param callback return all the keys matching the prefix.
  virtual void Keys(const std::string &ns,
                    const std::string &prefix,
                    std::function<void(std::vector<std::string>)> callback) = 0;

  virtual ~InternalKVInterface(){};
};

/// This implementation class of `InternalKVHandler`.
class GcsInternalKVManager : public rpc::InternalKVHandler {
 public:
  explicit GcsInternalKVManager(std::unique_ptr<InternalKVInterface> kv_instance)
      : kv_instance_(std::move(kv_instance)) {}

  void HandleInternalKVGet(rpc::InternalKVGetRequest request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVPut(rpc::InternalKVPutRequest request,
                           rpc::InternalKVPutReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVDel(rpc::InternalKVDelRequest request,
                           rpc::InternalKVDelReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVExists(rpc::InternalKVExistsRequest request,
                              rpc::InternalKVExistsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVKeys(rpc::InternalKVKeysRequest request,
                            rpc::InternalKVKeysReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  InternalKVInterface &GetInstance() { return *kv_instance_; }

 private:
  std::unique_ptr<InternalKVInterface> kv_instance_;
  Status ValidateKey(const std::string &key) const;
};

}  // namespace gcs
}  // namespace ray
