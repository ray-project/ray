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

#include "ray/gcs/store_client/redis_context.h"

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/util/clock.h"

namespace ray {
namespace gcs {

// Regression test for the GCS crash on Redis connection loss
// (https://github.com/ray-project/ray/issues/53475).
//
// RedisContext::Connect used to RAY_CHECK / RAY_LOG(FATAL) on any connection
// failure, which aborts gcs_server. That made an in-place reconnect impossible:
// a reconnect attempt during a transient failover would simply move the crash
// into Connect(). This test pins the new contract: Connect() returns a non-OK
// Status when the endpoint is unreachable, instead of crashing the process.
// (Existing callers still RAY_CHECK_OK the result, so boot-time behavior is
// unchanged.)
TEST(RedisContextConnectTest, ConnectToUnreachableEndpointReturnsErrorInsteadOfCrashing) {
  // Fail fast: try once and give up instead of retrying for ~60s.
  RayConfig::instance().initialize(R"({"redis_db_connect_retries": 0})");

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  // Port 1 has nothing listening on it, so the TCP connection is refused.
  const Status status = context.Connect("127.0.0.1",
                                        /*port=*/1,
                                        /*username=*/"",
                                        /*password=*/"",
                                        /*enable_ssl=*/false);

  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.IsRedisError()) << status.ToString();
}

}  // namespace gcs
}  // namespace ray
