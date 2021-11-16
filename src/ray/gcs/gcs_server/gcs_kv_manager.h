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
#include "ray/gcs/redis_client.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `InternalKVHandler`.
class GcsInternalKVManager : public rpc::InternalKVHandler {
 public:
  explicit GcsInternalKVManager(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(redis_client) {}

  void HandleInternalKVGet(const rpc::InternalKVGetRequest &request,
                           rpc::InternalKVGetReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  void HandleInternalKVPut(const rpc::InternalKVPutRequest &request,
                           rpc::InternalKVPutReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  void HandleInternalKVDel(const rpc::InternalKVDelRequest &request,
                           rpc::InternalKVDelReply *reply,
                           rpc::SendReplyCallback send_reply_callback);

  void InternalKVDelAsync(const std::string &key, std::function<void(int)> cb);

  void HandleInternalKVExists(const rpc::InternalKVExistsRequest &request,
                              rpc::InternalKVExistsReply *reply,
                              rpc::SendReplyCallback send_reply_callback);

  void HandleInternalKVKeys(const rpc::InternalKVKeysRequest &request,
                            rpc::InternalKVKeysReply *reply,
                            rpc::SendReplyCallback send_reply_callback);

 private:
  std::shared_ptr<RedisClient> redis_client_;
};

}  // namespace gcs
}  // namespace ray
