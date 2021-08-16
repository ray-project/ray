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

#pragma once

#include <boost/optional/optional.hpp>
#include <exception>
#include <string>

#include "ray/common/constants.h"
#include "ray/common/status.h"
#include "ray/gcs/gcs_client.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {
typedef std::function<void(Status status, const boost::optional<std::string> &result)>
    GetAgentAddressCallback;
typedef std::function<void(Status status, const boost::optional<int> &result)>
    UpdateAgentAddressCallback;

static inline void FindAgentAddress(std::shared_ptr<gcs::RedisClient> redis_client,
                                    NodeID node_id, GetAgentAddressCallback callback) {
  auto key = std::string(kDashboardAgentAddressPrefix) + ":" + node_id.Hex();
  ray::Status status = redis_client->GetPrimaryContext()->RunArgvAsync(
      {"HGET", key, "value"}, [key, callback](auto redis_reply) {
        if (!redis_reply->IsNil()) {
          callback(Status::OK(), redis_reply->ReadAsString());
        } else {
          callback(Status::NotFound("Failed to find the key = " + key), std::string());
        }
      });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Get agent address failed, key = " << key;
    callback(status, std::string());
  }
}

static inline void FindAgentAddress(std::shared_ptr<gcs::GcsClient> gcs_client,
                                    NodeID node_id, GetAgentAddressCallback callback) {
  auto key = std::string(kDashboardAgentAddressPrefix) + ":" + node_id.Hex();
  ray::Status status = gcs_client->InternalKV().AsyncInternalKVGet(key, callback);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Get agent address failed, key = " << key;
    callback(status, std::string());
  }
}

static inline void UpdateAgentAddress(std::shared_ptr<gcs::GcsClient> gcs_client,
                                      NodeID node_id, const std::string &value,
                                      UpdateAgentAddressCallback callback) {
  auto key = std::string(kDashboardAgentAddressPrefix) + ":" + node_id.Hex();
  ray::Status status = gcs_client->InternalKV().AsyncInternalKVPut(
      key, value, /*overwrite=*/true, callback);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Update the agent address failed, key = " << key
                   << ", value = " << value;
    callback(status, 0);
  }
}
}  // namespace ray