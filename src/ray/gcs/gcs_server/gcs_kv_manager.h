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
#include "absl/synchronization/mutex.h"
#include "absl/container/btree_map.h"
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
  /// Get the value associated with `key`
  ///
  /// \param key The key to fetch
  /// \param cb Callback function.
  virtual void Get(const std::string& key,
                   std::function<void(std::optional<std::string>)> cb) = 0;

  /// Associate key with value.
  ///
  /// \param key The key for the pair
  /// \param value The value for the pair
  /// \param overwrite If the key has existed before this op, nothing will happen
  ///    unless `overwrite` is set to be true.
  /// \param cb Callback function.
  virtual void Put(const std::string& key,
                   const std::string& value,
                   bool overwrite,
                   std::function<void(bool)> cb) = 0;

  /// Delete the key from the store
  ///
  /// \param key The key to be deleted
  /// \param cb Callback function.
  virtual void Del(const std::string& key, std::function<void(bool)> cb) = 0;

  /// Check whether the key exists in the store
  ///
  /// \param key The key to be checked.
  /// \param cb Callback function.
  virtual void Exists(const std::string& key, std::function<void(bool)> cb) = 0;

  /// Get the keys for a given prefix.
  ///
  /// \param prefix The prefix to be scaned.
  /// \param cb Callback function.
  virtual void Keys(const std::string& prefix, std::function<void(std::vector<std::string>)> cb) = 0;

  virtual ~InternalKVInterface() {};
};


class RedisInternalKV : public InternalKVInterface {
 public:
  RedisInternalKV(RedisClient* redis_client)
      : redis_client_(redis_client) {}

  void Get(const std::string& key, std::function<void(std::optional<std::string>)> cb) override;

  void Put(const std::string& key, const std::string& value, bool overwrite, std::function<void(bool)> cb) override;

  void Del(const std::string& key, std::function<void(bool)> cb) override;

  void Exists(const std::string& key, std::function<void(bool)> cb) override;

  void Keys(const std::string& prefix, std::function<void(std::vector<std::string>)> cb) override;


 private:
  RedisClient* redis_client_;
};

class MemoryInternalKV : public InternalKVInterface {
 public:
  MemoryInternalKV(instrumented_io_context* io_context)
      : io_context_(io_context) {
    RAY_CHECK(io_context != nullptr);
  }
  void Get(const std::string& key, std::function<void(std::optional<std::string>)> cb) override;

  void Put(const std::string& key, const std::string& value, bool overwrite, std::function<void(bool)> cb) override;

  void Del(const std::string& key, std::function<void(bool)> cb) override;

  void Exists(const std::string& key, std::function<void(bool)> cb) override;

  void Keys(const std::string& prefix, std::function<void(std::vector<std::string>)> cb) override;

 private:
  void RunCB(std::function<void()> cb) {
    io_context_->post(std::move(cb));
  }
  instrumented_io_context* io_context_;
  absl::btree_map<std::string, std::string> map_;
  absl::Mutex mu_;
};

/// This implementation class of `InternalKVHandler`.
class GcsInternalKVManager : public rpc::InternalKVHandler {
 public:
  explicit GcsInternalKVManager(std::unique_ptr<InternalKVInterface> kv_instance)
      : kv_instance_(std::move(kv_instance)) {}

  void HandleInternalKVGet(const rpc::InternalKVGetRequest &request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVPut(const rpc::InternalKVPutRequest &request,
                           rpc::InternalKVPutReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVDel(const rpc::InternalKVDelRequest &request,
                           rpc::InternalKVDelReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;
  void HandleInternalKVExists(const rpc::InternalKVExistsRequest &request,
                              rpc::InternalKVExistsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleInternalKVKeys(const rpc::InternalKVKeysRequest &request,
                            rpc::InternalKVKeysReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  InternalKVInterface& GetInstance() {
    return *kv_instance_;
  }
 private:
  std::unique_ptr<InternalKVInterface> kv_instance_;
};



}  // namespace gcs
}  // namespace ray
