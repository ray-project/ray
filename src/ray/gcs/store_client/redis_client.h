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

#include <map>
#include <memory>
#include <string>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/store_client/redis_context.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

/// \class RedisClient
/// This class is used to send commands to Redis.
class RedisClient {
 public:
  explicit RedisClient(const RedisClientOptions &options);

  RedisContext *GetPrimaryContext() { return primary_context_.get(); }

 protected:
  RedisClientOptions options_;

  /// Whether this client is connected to redis.
  bool is_connected_{false};

  // The following context writes everything to the primary shard
  std::unique_ptr<RedisContext> primary_context_;
};
}  // namespace gcs
}  // namespace ray
