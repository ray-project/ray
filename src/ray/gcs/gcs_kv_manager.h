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
#include <utility>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/postable.h"
#include "ray/common/status.h"
#include "ray/gcs/grpc_service_interfaces.h"

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
                   Postable<void(std::optional<std::string>)> callback) = 0;

  /// Get the values associated with `keys`.
  ///
  /// \param ns The namespace of the key.
  /// \param keys The keys to fetch.
  /// \param callback Returns the values for those keys that exist.
  virtual void MultiGet(
      const std::string &ns,
      const std::vector<std::string> &keys,
      Postable<void(absl::flat_hash_map<std::string, std::string>)> callback) = 0;

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
                   std::string value,
                   bool overwrite,
                   Postable<void(bool)> callback) = 0;

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
                   Postable<void(int64_t)> callback) = 0;

  /// Check whether the key exists in the store.
  ///
  /// \param ns The namespace of the key.
  /// \param key The key to be checked.
  /// \param callback Callback function.
  virtual void Exists(const std::string &ns,
                      const std::string &key,
                      Postable<void(bool)> callback) = 0;

  /// Get the keys for a given prefix.
  ///
  /// \param ns The namespace of the prefix.
  /// \param prefix The prefix to be scaned.
  /// \param callback return all the keys matching the prefix.
  virtual void Keys(const std::string &ns,
                    const std::string &prefix,
                    Postable<void(std::vector<std::string>)> callback) = 0;

  virtual ~InternalKVInterface() = default;
};

class GcsInternalKVManager : public rpc::InternalKVGcsServiceHandler {
 public:
  explicit GcsInternalKVManager(std::unique_ptr<InternalKVInterface> kv_instance,
                                std::string raylet_config_list,
                                instrumented_io_context &io_context)
      : kv_instance_(std::move(kv_instance)),
        raylet_config_list_(std::move(raylet_config_list)),
        io_context_(io_context) {}

  void HandleInternalKVGet(rpc::InternalKVGetRequest request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVMultiGet(rpc::InternalKVMultiGetRequest request,
                                rpc::InternalKVMultiGetReply *reply,
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

  /// Handle get internal config.
  void HandleGetInternalConfig(rpc::GetInternalConfigRequest request,
                               rpc::GetInternalConfigReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  InternalKVInterface &GetInstance() { return *kv_instance_; }

 private:
  std::unique_ptr<InternalKVInterface> kv_instance_;
  const std::string raylet_config_list_;
  instrumented_io_context &io_context_;
  Status ValidateKey(const std::string &key) const;
};

}  // namespace gcs
}  // namespace ray
