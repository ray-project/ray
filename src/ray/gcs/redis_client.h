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
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

class RedisContext;

class RedisClientOptions {
 public:
  RedisClientOptions(const std::string &ip, int port, const std::string &password,
                     bool enable_sharding_conn = true, bool enable_sync_conn = true,
                     bool enable_async_conn = true, bool enable_subscribe_conn = true)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        enable_sharding_conn_(enable_sharding_conn),
        enable_sync_conn_(enable_sync_conn),
        enable_async_conn_(enable_async_conn),
        enable_subscribe_conn_(enable_subscribe_conn) {}

  // Redis server address
  std::string server_ip_;
  int server_port_;

  // Password of Redis.
  std::string password_;

  // Whether we enable sharding for accessing data.
  bool enable_sharding_conn_{true};

  // Whether to establish connection of contexts.
  bool enable_sync_conn_;
  bool enable_async_conn_;
  bool enable_subscribe_conn_;
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

  // TODO(micafan) Maybe it's not necessary to use multi threads.
  /// Connect to Redis. Non-thread safe.
  /// Call this function before calling other functions.
  ///
  /// \param io_services The event loops for this client. Each RedisContext bind to
  /// an event loop. Each io_service must be single-threaded. Because `RedisAsioClient`
  /// is non-thread safe.
  /// \return Status
  Status Connect(std::vector<instrumented_io_context *> io_services);

  /// Disconnect with Redis. Non-thread safe.
  void Disconnect();

  std::vector<std::shared_ptr<RedisContext>> GetShardContexts() {
    return shard_contexts_;
  }

  std::shared_ptr<RedisContext> GetShardContext(const std::string &shard_key);

  std::shared_ptr<RedisContext> GetPrimaryContext() { return primary_context_; }

 protected:
  /// Attach this client to an asio event loop. Note that only
  /// one event loop should be attached at a time.
  void Attach();

  RedisClientOptions options_;

  /// Whether this client is connected to redis.
  bool is_connected_{false};

  // The following contexts write to the data shard
  std::vector<std::shared_ptr<RedisContext>> shard_contexts_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_async_clients_;
  std::vector<std::unique_ptr<RedisAsioClient>> shard_asio_subscribe_clients_;
  // The following context writes everything to the primary shard
  std::shared_ptr<RedisContext> primary_context_;
  std::unique_ptr<RedisAsioClient> asio_async_auxiliary_client_;
  std::unique_ptr<RedisAsioClient> asio_subscribe_auxiliary_client_;
};

}  // namespace gcs

}  // namespace ray
