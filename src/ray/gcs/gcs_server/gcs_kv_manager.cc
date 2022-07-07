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
namespace {

constexpr std::string_view kNamespacePrefix = "@namespace_";
constexpr std::string_view kNamespaceSep = ":";
constexpr std::string_view kClusterSeparator = "@";

}  // namespace
std::string RedisInternalKV::MakeKey(const std::string &ns,
                                     const std::string &key) const {
  if (ns.empty()) {
    return absl::StrCat(external_storage_namespace_, kClusterSeparator, key);
  }
  return absl::StrCat(external_storage_namespace_,
                      kClusterSeparator,
                      kNamespacePrefix,
                      ns,
                      kNamespaceSep,
                      key);
}

Status RedisInternalKV::ValidateKey(const std::string &key) const {
  if (absl::StartsWith(key, kNamespacePrefix)) {
    return Status::KeyError(absl::StrCat("Key can't start with ", kNamespacePrefix));
  }
  return Status::OK();
}

std::string RedisInternalKV::ExtractKey(const std::string &key) const {
  auto view = std::string_view(key);
  RAY_CHECK(absl::StartsWith(view, external_storage_namespace_))
      << "Invalid key: " << view << ". It should start with "
      << external_storage_namespace_;
  view = view.substr(external_storage_namespace_.size() + kClusterSeparator.size());
  if (absl::StartsWith(view, kNamespacePrefix)) {
    std::vector<std::string> parts =
        absl::StrSplit(key, absl::MaxSplits(kNamespaceSep, 1));
    RAY_CHECK(parts.size() == 2) << "Invalid key: " << key;

    return parts[1];
  }
  return std::string(view.begin(), view.end());
}

RedisInternalKV::RedisInternalKV(const RedisClientOptions &redis_options)
    : redis_options_(redis_options),
      external_storage_namespace_(::RayConfig::instance().external_storage_namespace()),
      work_(io_service_) {
  RAY_CHECK(!absl::StrContains(external_storage_namespace_, kClusterSeparator))
      << "Storage namespace (" << external_storage_namespace_ << ") shouldn't contain "
      << kClusterSeparator << ".";
  io_thread_ = std::make_unique<std::thread>([this] {
    SetThreadName("InternalKV");
    io_service_.run();
  });
  redis_client_ = std::make_unique<RedisClient>(redis_options_);
  RAY_CHECK_OK(redis_client_->Connect(io_service_));
}

void RedisInternalKV::Get(const std::string &ns,
                          const std::string &key,
                          std::function<void(std::optional<std::string>)> callback) {
  auto true_key = MakeKey(ns, key);
  std::vector<std::string> cmd = {"HGET", true_key, "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [callback = std::move(callback)](auto redis_reply) {
        if (callback) {
          if (!redis_reply->IsNil()) {
            callback(redis_reply->ReadAsString());
          } else {
            callback(std::nullopt);
          }
        }
      }));
}

void RedisInternalKV::Put(const std::string &ns,
                          const std::string &key,
                          const std::string &value,
                          bool overwrite,
                          std::function<void(bool)> callback) {
  auto true_key = MakeKey(ns, key);
  std::vector<std::string> cmd = {
      overwrite ? "HSET" : "HSETNX", true_key, "value", value};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [callback = std::move(callback)](auto redis_reply) {
        if (callback) {
          auto added_num = redis_reply->ReadAsInteger();
          callback(added_num != 0);
        }
      }));
}

void RedisInternalKV::Del(const std::string &ns,
                          const std::string &key,
                          bool del_by_prefix,
                          std::function<void(int64_t)> callback) {
  auto true_key = MakeKey(ns, key);
  if (del_by_prefix) {
    std::vector<std::string> cmd = {"KEYS", true_key + "*"};
    RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
        cmd, [this, callback = std::move(callback)](auto redis_reply) {
          const auto &reply = redis_reply->ReadAsStringArray();
          // If there are no keys with this prefix, we don't need to send
          // another delete.
          if (reply.size() == 0) {
            if (callback) {
              callback(0);
            }
          } else {
            std::vector<std::string> del_cmd = {"DEL"};
            for (const auto &r : reply) {
              RAY_CHECK(r.has_value());
              del_cmd.emplace_back(*r);
            }
            RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
                del_cmd, [callback = std::move(callback)](auto redis_reply) {
                  if (callback) {
                    callback(redis_reply->ReadAsInteger());
                  }
                }));
          }
        }));
  } else {
    std::vector<std::string> cmd = {"DEL", true_key};
    RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
        cmd, [callback = std::move(callback)](auto redis_reply) {
          if (callback) {
            callback(redis_reply->ReadAsInteger());
          }
        }));
  }
}

void RedisInternalKV::Exists(const std::string &ns,
                             const std::string &key,
                             std::function<void(bool)> callback) {
  auto true_key = MakeKey(ns, key);
  std::vector<std::string> cmd = {"HEXISTS", true_key, "value"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [callback = std::move(callback)](auto redis_reply) {
        if (callback) {
          bool exists = redis_reply->ReadAsInteger() > 0;
          callback(exists);
        }
      }));
}

void RedisInternalKV::Keys(const std::string &ns,
                           const std::string &prefix,
                           std::function<void(std::vector<std::string>)> callback) {
  auto true_prefix = MakeKey(ns, prefix);
  std::vector<std::string> cmd = {"KEYS", true_prefix + "*"};
  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      cmd, [this, callback = std::move(callback)](auto redis_reply) {
        if (callback) {
          const auto &reply = redis_reply->ReadAsStringArray();
          std::vector<std::string> results;
          for (const auto &r : reply) {
            RAY_CHECK(r.has_value());
            results.emplace_back(ExtractKey(*r));
          }
          callback(std::move(results));
        }
      }));
}

void GcsInternalKVManager::HandleInternalKVGet(
    const rpc::InternalKVGetRequest &request,
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
    const rpc::InternalKVPutRequest &request,
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
    const rpc::InternalKVDelRequest &request,
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
    const rpc::InternalKVExistsRequest &request,
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
    const rpc::InternalKVKeysRequest &request,
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
