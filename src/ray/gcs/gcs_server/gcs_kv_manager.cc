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

#include <string_view>

#include "absl/strings/match.h"
#include "absl/strings/str_split.h"

namespace ray {
namespace gcs {

void GcsInternalKVManager::HandleInternalKVGet(
    rpc::InternalKVGetRequest request,
    rpc::InternalKVGetReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = ValidateKey(request.key());
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  } else {
    auto callback = [reply, send_reply_callback](std::optional<std::string> val) {
      if (val) {
        reply->set_value(*val);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      } else {
        GCS_RPC_SEND_REPLY(
            send_reply_callback, reply, Status::NotFound("Failed to find the key"));
      }
    };
    kv_instance_->Get(request.namespace_(), request.key(), std::move(callback));
  }
}

void GcsInternalKVManager::HandleInternalKVPut(
    rpc::InternalKVPutRequest request,
    rpc::InternalKVPutReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = ValidateKey(request.key());
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  } else {
    auto callback = [reply, send_reply_callback](bool newly_added) {
      reply->set_added_num(newly_added ? 1 : 0);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    };
    kv_instance_->Put(request.namespace_(),
                      request.key(),
                      request.value(),
                      request.overwrite(),
                      std::move(callback));
  }
}

void GcsInternalKVManager::HandleInternalKVDel(
    rpc::InternalKVDelRequest request,
    rpc::InternalKVDelReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = ValidateKey(request.key());
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  } else {
    auto callback = [reply, send_reply_callback](int64_t del_num) {
      reply->set_deleted_num(del_num);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    };
    kv_instance_->Del(request.namespace_(),
                      request.key(),
                      request.del_by_prefix(),
                      std::move(callback));
  }
}

void GcsInternalKVManager::HandleInternalKVExists(
    rpc::InternalKVExistsRequest request,
    rpc::InternalKVExistsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = ValidateKey(request.key());
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  {
    auto callback = [reply, send_reply_callback](bool exists) {
      reply->set_exists(exists);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    };
    kv_instance_->Exists(request.namespace_(), request.key(), std::move(callback));
  }
}

void GcsInternalKVManager::HandleInternalKVKeys(
    rpc::InternalKVKeysRequest request,
    rpc::InternalKVKeysReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = ValidateKey(request.prefix());
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  } else {
    auto callback = [reply, send_reply_callback](std::vector<std::string> keys) {
      for (auto &result : keys) {
        reply->add_results(std::move(result));
      }
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    };
    kv_instance_->Keys(request.namespace_(), request.prefix(), std::move(callback));
  }
}

Status GcsInternalKVManager::ValidateKey(const std::string &key) const {
  if (absl::StartsWith(key, kNamespacePrefix)) {
    return Status::KeyError(absl::StrCat("Key can't start with ", kNamespacePrefix));
  }
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
