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

#include <boost/asio.hpp>

#include "ray/gcs/redis_context.h"
#include "ray/util/periodical_runner.h"

namespace ray {

namespace gcs {
class RedisGcsClient;

/// GcsRedisFailureDetector is responsible for monitoring redis and binding GCS server and
/// redis life cycle together. GCS client subscribes to redis messages and it cannot sense
/// whether the redis is inactive unless we go to ping redis voluntarily. But there are
/// many GCS clients, if they all Ping redis, the redis load will be high. So we ping
/// redis on GCS server and GCS client can sense whether redis is normal through RPC
/// connection with GCS server.
class GcsRedisFailureDetector {
 public:
  /// Create a GcsRedisFailureDetector.
  ///
  /// \param io_service The event loop to run the monitor on.
  /// \param redis_context The redis context is used to ping redis.
  /// \param callback Callback that will be called when redis is detected as not alive.
  explicit GcsRedisFailureDetector(boost::asio::io_service &io_service,
                                   std::shared_ptr<RedisContext> redis_context,
                                   std::function<void()> callback);

  /// Start detecting redis.
  void Start();

 protected:
  /// Check that if redis is inactive.
  void DetectRedis();

 private:
  /// A redis context is used to ping redis.
  /// TODO(ffbin): We will use redis client later.
  std::shared_ptr<RedisContext> redis_context_;

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;

  /// A function is called when redis is detected to be unavailable.
  std::function<void()> callback_;
};

}  // namespace gcs
}  // namespace ray
