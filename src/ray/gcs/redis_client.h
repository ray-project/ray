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
#include <string>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"
#include "ray/gcs/asio.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisClientOptions {
 public:
  RedisClientOptions(const std::string &ip,
                     int port,
                     const std::string &password,
                     bool enable_ssl = false)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        enable_ssl_(enable_ssl) {}

  // Redis server address
  std::string server_ip_;
  int server_port_;

  // Password of Redis.
  std::string password_;

  // Whether to use tls/ssl for redis connection
  bool enable_ssl_ = false;
};

/// \class RedisClient
/// This class is used to send commands to Redis.
class RedisClient {
 public:
  RedisClient(const RedisClientOptions &options);

  /// Connect to Redis. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_service The event loop for this client.
  /// This io_service must be single-threaded. Because `RedisAsioClient` is
  /// non-thread safe.
  /// \return Status
  Status Connect(instrumented_io_context &io_service);

  /// Disconnect with Redis. Non-thread safe.
  void Disconnect();

  std::shared_ptr<RedisContext> GetPrimaryContext() { return primary_context_; }

 protected:
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  void Attach();

  RedisClientOptions options_;

  /// Whether this client is connected to redis.
  bool is_connected_{false};

  // The following context writes everything to the primary shard
  std::unique_ptr<RedisAsioClient> asio_async_auxiliary_client_;
  std::shared_ptr<RedisContext> primary_context_;
};

}  // namespace gcs

}  // namespace ray
