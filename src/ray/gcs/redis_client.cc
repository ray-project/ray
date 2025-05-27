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

#include "ray/gcs/redis_client.h"

#include <memory>

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_context.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
RedisClient::RedisClient(const RedisClientOptions &options) : options_(options) {}

Status RedisClient::Connect(instrumented_io_context &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, redis server address is empty.";
    return Status::Invalid("Redis server address is invalid!");
  }

  primary_context_ = std::make_unique<RedisContext>(io_service);

  RAY_CHECK_OK(primary_context_->Connect(options_.server_ip_,
                                         options_.server_port_,
                                         /*username=*/options_.username_,
                                         /*password=*/options_.password_,
                                         /*enable_ssl=*/options_.enable_ssl_));

  is_connected_ = true;
  RAY_LOG(DEBUG) << "RedisClient connected.";

  return Status::OK();
}

void RedisClient::Disconnect() {
  RAY_CHECK(is_connected_);
  is_connected_ = false;
  RAY_LOG(DEBUG) << "RedisClient disconnected.";
}
}  // namespace gcs
}  // namespace ray
