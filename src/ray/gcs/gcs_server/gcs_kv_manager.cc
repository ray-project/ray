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

#include "absl/strings/match.h"

namespace ray {
namespace gcs {

void RedisInternalKV::Get(const std::string &key,
                          std::function<void(std::optional<std::string>)> cb) {
  std::vector<std::string> cmd = {"HGET", key, "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [cb = std::move(cb)](auto redis_reply) {
        if (!redis_reply->IsNil()) {
          cb(redis_reply->ReadAsString());
        } else {
          cb(std::nullopt);
        }
      }));
}

void RedisInternalKV::Put(const std::string &key, const std::string &value,
                          bool overwrite, std::function<void(bool)> cb) {
  std::vector<std::string> cmd = {overwrite ? "HSET" : "HSETNX", key, "value", value};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [cb = std::move(cb)](auto redis_reply) {
        auto added_num = redis_reply->ReadAsInteger();
        cb(added_num != 0);
      }));
}

void RedisInternalKV::Del(const std::string &key, std::function<void(bool)> cb) {
  std::vector<std::string> cmd = {"HDEL", key, "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd,
      [cb = std::move(cb)](auto redis_reply) { cb(redis_reply->ReadAsInteger() != 0); }));
}

void RedisInternalKV::Exists(const std::string &key, std::function<void(bool)> cb) {
  std::vector<std::string> cmd = {"HEXISTS", key, "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [cb = std::move(cb)](auto redis_reply) {
        bool exists = redis_reply->ReadAsInteger() > 0;
        cb(exists);
      }));
}

void RedisInternalKV::Keys(const std::string &prefix,
                           std::function<void(std::vector<std::string>)> cb) {
  std::vector<std::string> cmd = {"KEYS", prefix + "*"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [cb = std::move(cb)](auto redis_reply) {
        const auto &reply = redis_reply->ReadAsStringArray();
        std::vector<std::string> results;
        for (const auto &r : reply) {
          RAY_CHECK(r.has_value());
          results.emplace_back(*r);
        }
        cb(std::move(results));
      }));
}

void MemoryInternalKV::Get(const std::string &key,
                           std::function<void(std::optional<std::string>)> cb) {
  absl::ReaderMutexLock _(&mu_);
  auto it = map_.find(key);
  auto val = it == map_.end() ? std::nullopt : std::make_optional(it->second);
  if (cb != nullptr) {
    RunCB(std::bind(std::move(cb), std::move(val)));
  }
}

void MemoryInternalKV::Put(const std::string &key, const std::string &value,
                           bool overwrite, std::function<void(bool)> cb) {
  absl::WriterMutexLock _(&mu_);
  auto it = map_.find(key);
  bool inserted = false;
  if (it != map_.end()) {
    if (overwrite) {
      it->second = value;
    }
  } else {
    map_[key] = value;
    inserted = true;
  }
  if (cb != nullptr) {
    RunCB(std::bind(std::move(cb), inserted));
  }
}

void MemoryInternalKV::Del(const std::string &key, std::function<void(bool)> cb) {
  absl::WriterMutexLock _(&mu_);
  auto it = map_.find(key);
  bool deleted = true;
  if (it == map_.end()) {
    deleted = false;
  } else {
    map_.erase(it);
  }
  if (cb != nullptr) {
    RunCB(std::bind(std::move(cb), deleted));
  }
}

void MemoryInternalKV::Exists(const std::string &key, std::function<void(bool)> cb) {
  absl::ReaderMutexLock _(&mu_);
  bool existed = map_.find(key) != map_.end();
  if (cb != nullptr) {
    RunCB(std::bind(std::move(cb), existed));
  }
}

void MemoryInternalKV::Keys(const std::string &prefix,
                            std::function<void(std::vector<std::string>)> cb) {
  absl::ReaderMutexLock _(&mu_);
  std::vector<std::string> keys;
  auto iter = map_.lower_bound(prefix);
  while (iter != map_.end() && absl::StartsWith(iter->first, prefix)) {
    keys.push_back(iter->first);
    iter++;
  }
  if (cb != nullptr) {
    RunCB(std::bind(std::move(cb), std::move(keys)));
  }
}

void GcsInternalKVManager::HandleInternalKVGet(
    const rpc::InternalKVGetRequest &request, rpc::InternalKVGetReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto cb = [reply, send_reply_callback](std::optional<std::string> val) {
    if (val) {
      reply->set_value(*val);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    } else {
      GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                         Status::NotFound("Failed to find the key"));
    }
  };
  kv_instance_->Get(request.key(), std::move(cb));
}

void GcsInternalKVManager::HandleInternalKVPut(
    const rpc::InternalKVPutRequest &request, rpc::InternalKVPutReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto cb = [reply, send_reply_callback](bool newly_added) {
    reply->set_added_num(newly_added ? 1 : 0);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  kv_instance_->Put(request.key(), request.value(), request.overwrite(), std::move(cb));
}

void GcsInternalKVManager::HandleInternalKVDel(
    const rpc::InternalKVDelRequest &request, rpc::InternalKVDelReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto cb = [reply, send_reply_callback](bool deleted) {
    reply->set_deleted_num(deleted ? 1 : 0);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  kv_instance_->Del(request.key(), std::move(cb));
}

void GcsInternalKVManager::HandleInternalKVExists(
    const rpc::InternalKVExistsRequest &request, rpc::InternalKVExistsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto cb = [reply, send_reply_callback](bool exists) {
    reply->set_exists(exists);
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  kv_instance_->Exists(request.key(), std::move(cb));
}

void GcsInternalKVManager::HandleInternalKVKeys(
    const rpc::InternalKVKeysRequest &request, rpc::InternalKVKeysReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto cb = [reply, send_reply_callback](std::vector<std::string> keys) {
    for (auto &result : keys) {
      reply->add_results(std::move(result));
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  kv_instance_->Keys(request.prefix(), std::move(cb));
}

}  // namespace gcs
}  // namespace ray
