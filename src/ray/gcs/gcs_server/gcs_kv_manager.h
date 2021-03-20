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

/// This implementation class of `KVHandler`.
class GcsKVManager : public rpc::KVHandler {
 public:
  explicit GcsKVManager(std::shared_ptr<RedisClient> redis_client)
      : redis_client_(redis_client) {}

  void HandleGet(const rpc::GetRequest &request, rpc::GetReply *reply,
                 rpc::SendReplyCallback send_reply_callback);

  void HandlePut(const rpc::PutRequest &request, rpc::PutReply *reply,
                 rpc::SendReplyCallback send_reply_callback);

  void HandleDel(const rpc::DelRequest &request, rpc::DelReply *reply,
                 rpc::SendReplyCallback send_reply_callback);

  void HandleExists(const rpc::ExistsRequest &request, rpc::ExistsReply *reply,
                    rpc::SendReplyCallback send_reply_callback);

  void HandleKeys(const rpc::KeysRequest &request, rpc::KeysReply *reply,
                  rpc::SendReplyCallback send_reply_callback);

 private:
  std::shared_ptr<RedisClient> redis_client_;
};

}  // namespace gcs
}  // namespace ray
