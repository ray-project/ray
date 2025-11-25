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

#include "ray/gcs_rpc_client/accessors/internal_kv_accessor.h"

#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ray/gcs_rpc_client/rpc_client.h"
#include "ray/util/container_util.h"

namespace ray {
namespace gcs {

InternalKVAccessor::InternalKVAccessor(GcsClientContext *context) : context_(context) {}

void InternalKVAccessor::AsyncInternalKVGet(
    const std::string &ns,
    const std::string &key,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::string> &callback) {
  rpc::InternalKVGetRequest req;
  req.set_key(key);
  req.set_namespace_(ns);
  context_->GetGcsRpcClient().InternalKVGet(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVGetReply &&reply) {
        if (reply.status().code() == static_cast<int>(StatusCode::NotFound)) {
          callback(status, std::nullopt);
        } else {
          callback(status, reply.value());
        }
      },
      timeout_ms);
}

void InternalKVAccessor::AsyncInternalKVMultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::unordered_map<std::string, std::string>> &callback) {
  rpc::InternalKVMultiGetRequest req;
  for (const auto &key : keys) {
    req.add_keys(key);
  }
  req.set_namespace_(ns);
  context_->GetGcsRpcClient().InternalKVMultiGet(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVMultiGetReply &&reply) {
        std::unordered_map<std::string, std::string> map;
        if (!status.ok()) {
          callback(status, map);
        } else {
          // TODO(ryw): reply.status() is not examined. It's never populated in
          // src/ray/gcs/gcs_kv_manager.cc either anyway so it's ok for now.
          // Investigate if we wanna remove that field.
          for (const auto &entry : reply.results()) {
            map[entry.key()] = entry.value();
          }
          callback(Status::OK(), map);
        }
      },
      timeout_ms);
}

void InternalKVAccessor::AsyncInternalKVPut(const std::string &ns,
                                            const std::string &key,
                                            const std::string &value,
                                            bool overwrite,
                                            const int64_t timeout_ms,
                                            const OptionalItemCallback<bool> &callback) {
  rpc::InternalKVPutRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  req.set_value(value);
  req.set_overwrite(overwrite);
  context_->GetGcsRpcClient().InternalKVPut(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVPutReply &&reply) {
        callback(status, reply.added());
      },
      timeout_ms);
}

void InternalKVAccessor::AsyncInternalKVExists(
    const std::string &ns,
    const std::string &key,
    const int64_t timeout_ms,
    const OptionalItemCallback<bool> &callback) {
  rpc::InternalKVExistsRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  context_->GetGcsRpcClient().InternalKVExists(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVExistsReply &&reply) {
        callback(status, reply.exists());
      },
      timeout_ms);
}

void InternalKVAccessor::AsyncInternalKVDel(const std::string &ns,
                                            const std::string &key,
                                            bool del_by_prefix,
                                            const int64_t timeout_ms,
                                            const OptionalItemCallback<int> &callback) {
  rpc::InternalKVDelRequest req;
  req.set_namespace_(ns);
  req.set_key(key);
  req.set_del_by_prefix(del_by_prefix);
  context_->GetGcsRpcClient().InternalKVDel(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVDelReply &&reply) {
        callback(status, reply.deleted_num());
      },
      timeout_ms);
}

void InternalKVAccessor::AsyncInternalKVKeys(
    const std::string &ns,
    const std::string &prefix,
    const int64_t timeout_ms,
    const OptionalItemCallback<std::vector<std::string>> &callback) {
  rpc::InternalKVKeysRequest req;
  req.set_namespace_(ns);
  req.set_prefix(prefix);
  context_->GetGcsRpcClient().InternalKVKeys(
      std::move(req),
      [callback](const Status &status, rpc::InternalKVKeysReply &&reply) {
        if (!status.ok()) {
          callback(status, std::nullopt);
        } else {
          callback(status, VectorFromProtobuf(std::move(*reply.mutable_results())));
        }
      },
      timeout_ms);
}

Status InternalKVAccessor::Put(const std::string &ns,
                               const std::string &key,
                               const std::string &value,
                               bool overwrite,
                               const int64_t timeout_ms,
                               bool &added) {
  std::promise<Status> ret_promise;
  AsyncInternalKVPut(
      ns,
      key,
      value,
      overwrite,
      timeout_ms,
      [&ret_promise, &added](Status status, std::optional<bool> was_added) {
        added = was_added.value_or(false);
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Keys(const std::string &ns,
                                const std::string &prefix,
                                const int64_t timeout_ms,
                                std::vector<std::string> &value) {
  std::promise<Status> ret_promise;
  AsyncInternalKVKeys(
      ns,
      prefix,
      timeout_ms,
      [&ret_promise, &value](Status status,
                             std::optional<std::vector<std::string>> &&values) {
        if (values) {
          value = std::move(*values);
        } else {
          value = std::vector<std::string>();
        }
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Get(const std::string &ns,
                               const std::string &key,
                               const int64_t timeout_ms,
                               std::string &value) {
  std::promise<Status> ret_promise;
  AsyncInternalKVGet(
      ns,
      key,
      timeout_ms,
      [&ret_promise, &value](Status status, std::optional<std::string> &&v) {
        if (v) {
          value = std::move(v.value());
        } else {
          value.clear();
        }
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::MultiGet(
    const std::string &ns,
    const std::vector<std::string> &keys,
    const int64_t timeout_ms,
    std::unordered_map<std::string, std::string> &values) {
  std::promise<Status> ret_promise;
  AsyncInternalKVMultiGet(
      ns,
      keys,
      timeout_ms,
      [&ret_promise, &values](
          Status status,
          std::optional<std::unordered_map<std::string, std::string>> &&vs) {
        values.clear();
        if (vs) {
          values = std::move(*vs);
        }
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Del(const std::string &ns,
                               const std::string &key,
                               bool del_by_prefix,
                               const int64_t timeout_ms,
                               int &num_deleted) {
  std::promise<Status> ret_promise;
  AsyncInternalKVDel(
      ns,
      key,
      del_by_prefix,
      timeout_ms,
      [&ret_promise, &num_deleted](Status status, std::optional<int> &&value) {
        num_deleted = value.value_or(0);
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

Status InternalKVAccessor::Exists(const std::string &ns,
                                  const std::string &key,
                                  const int64_t timeout_ms,
                                  bool &exists) {
  std::promise<Status> ret_promise;
  AsyncInternalKVExists(
      ns,
      key,
      timeout_ms,
      [&ret_promise, &exists](Status status, std::optional<bool> &&value) {
        exists = value.value_or(false);
        ret_promise.set_value(status);
      });
  return ret_promise.get_future().get();
}

void InternalKVAccessor::AsyncGetInternalConfig(
    const OptionalItemCallback<std::string> &callback) {
  rpc::GetInternalConfigRequest request;
  context_->GetGcsRpcClient().GetInternalConfig(
      std::move(request),
      [callback](const Status &status, rpc::GetInternalConfigReply &&reply) {
        if (status.ok()) {
          RAY_LOG(DEBUG) << "Fetched internal config: " << reply.config();
        } else {
          RAY_LOG(ERROR) << "Failed to get internal config: " << status;
        }
        callback(status, reply.config());
      });
}

}  // namespace gcs
}  // namespace ray
