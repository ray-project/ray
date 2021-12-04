// Copyright 2021 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_kv_manager.h"

namespace ray {
namespace gcs {

void GcsInternalKVManager::HandleInternalKVGet(
    const rpc::InternalKVGetRequest &request, rpc::InternalKVGetReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  kv_instance_->Get(request.key(),
                    [reply, send_reply_callback](std::optional<std::string> val) {
                      if(val) {
                        reply->set_value(*val);
                        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
                      } else {
                        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::NotFound("Failed to find the key"));
                      }
                    });
}

void GcsInternalKVManager::HandleInternalKVPut(
    const rpc::InternalKVPutRequest &request, rpc::InternalKVPutReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  kv_instance_->Put(
      request.key(),
      request.value(),
      request.overwrite(),
      [reply, send_reply_callback](bool newly_added) {
        reply->set_added_num(newly_added ? 1 : 0);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
}

void GcsInternalKVManager::HandleInternalKVDel(
    const rpc::InternalKVDelRequest &request, rpc::InternalKVDelReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  kv_instance_->Del(request.key(), [reply, send_reply_callback](bool deleted) {
                                     reply->set_deleted_num(deleted ? 1 : 0);
                                     GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
                                   });
}

void GcsInternalKVManager::HandleInternalKVExists(
    const rpc::InternalKVExistsRequest &request, rpc::InternalKVExistsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  kv_instance_->Exists(key, [reply, send_reply_callback](bool exists) {
                              reply->set_exists(exists);
                              GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
                            });
}

void GcsInternalKVManager::HandleInternalKVKeys(
    const rpc::InternalKVKeysRequest &request, rpc::InternalKVKeysReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  kv_instance_->Keys(request.prefix(), [reply, send_reply_callback] (std::vector<std::string> keys) {
                                         for (auto &result : keys) {
                                           reply->add_results(std::move(result));
                                         }
                                         GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());

                                       });
}

}  // namespace gcs
}  // namespace ray
