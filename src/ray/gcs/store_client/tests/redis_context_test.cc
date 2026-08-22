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

#ifndef _WIN32
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"
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

// The keepalive tests assert on the real sockets hiredis created, via
// getsockopt(), against a live local Redis. Windows is excluded: the test
// Redis harness does not run there and the socket option API differs.
#ifndef _WIN32

namespace {

int GetIntSockOpt(int fd, int level, int optname) {
  int value = -1;
  socklen_t len = sizeof(value);
  EXPECT_EQ(getsockopt(fd, level, optname, &value, &len), 0) << strerror(errno);
  return value;
}

}  // namespace

class RedisContextKeepaliveTest : public ::testing::Test {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  // RayConfig is process-global, so every value a test writes has to be put
  // back. Otherwise a test that shortens the connect-retry budget silently
  // reconfigures every test that runs after it, and the suite only passes for
  // the declaration order it happens to have today - not under --gtest_filter,
  // shuffling, or sharding.
  void SetUp() override {
    saved_interval_seconds_ =
        RayConfig::instance().redis_tcp_keepalive_interval_seconds();
    saved_probes_ = RayConfig::instance().redis_tcp_keepalive_probes();
    saved_connect_retries_ = RayConfig::instance().redis_db_connect_retries();
    saved_connect_wait_ms_ = RayConfig::instance().redis_db_connect_wait_milliseconds();
  }

  void TearDown() override {
    RayConfig::instance().redis_tcp_keepalive_interval_seconds() =
        saved_interval_seconds_;
    RayConfig::instance().redis_tcp_keepalive_probes() = saved_probes_;
    RayConfig::instance().redis_db_connect_retries() = saved_connect_retries_;
    RayConfig::instance().redis_db_connect_wait_milliseconds() = saved_connect_wait_ms_;
  }

  Status ConnectToLocalRedis(RedisContext &context) {
    return context.Connect("127.0.0.1",
                           TEST_REDIS_SERVER_PORTS.front(),
                           /*username=*/"",
                           /*password=*/"",
                           /*enable_ssl=*/false);
  }

 private:
  int64_t saved_interval_seconds_ = 0;
  int64_t saved_probes_ = 0;
  int64_t saved_connect_retries_ = 0;
  int64_t saved_connect_wait_ms_ = 0;
};

// Both the sync and the async hiredis sockets must carry the configured
// keepalive policy. Only glibc Linux exposes the per-connection timing:
// hiredis guards TCP_KEEPIDLE/TCP_KEEPINTVL/TCP_KEEPCNT on __GLIBC__, so on
// musl (and macOS/Windows) SO_KEEPALIVE is all that can be asserted.
TEST_F(RedisContextKeepaliveTest, EnablesKeepaliveOnSyncAndAsyncSockets) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 30;
  RayConfig::instance().redis_tcp_keepalive_probes() = 9;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);
  ASSERT_TRUE(ConnectToLocalRedis(context).ok());

  const int sync_fd = context.sync_context()->fd;
  const int async_fd = context.async_context().GetRawRedisAsyncContext()->c.fd;
  for (const int fd : {sync_fd, async_fd}) {
    EXPECT_EQ(GetIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE), 1) << "fd: " << fd;
#if defined(__linux__) && defined(__GLIBC__)
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPIDLE), 30) << "fd: " << fd;
    // Ray overrides hiredis' derived values (interval/3, 3 probes) so that
    // probe cadence and time-to-declare-dead can be tuned independently.
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPINTVL), 30) << "fd: " << fd;
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPCNT), 9) << "fd: " << fd;
#endif
  }
}

// Regression guard for the escape hatch: interval 0 must leave the sockets
// exactly as they were before this feature existed.
TEST_F(RedisContextKeepaliveTest, IntervalZeroLeavesKeepaliveDisabled) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 0;
  // Probes are not validated or applied when keepalive is off.
  RayConfig::instance().redis_tcp_keepalive_probes() = 0;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);
  ASSERT_TRUE(ConnectToLocalRedis(context).ok());

  const int sync_fd = context.sync_context()->fd;
  const int async_fd = context.async_context().GetRawRedisAsyncContext()->c.fd;
  for (const int fd : {sync_fd, async_fd}) {
    EXPECT_EQ(GetIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE), 0) << "fd: " << fd;
  }
}

// An invalid keepalive configuration is not a transient connection failure:
// Connect() must fail immediately with an actionable InvalidArgument instead
// of burning the redis_db_connect_retries budget and then reporting Redis as
// unreachable.
TEST_F(RedisContextKeepaliveTest, NegativeIntervalFailsFastWithInvalidArgument) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = -1;
  RayConfig::instance().redis_tcp_keepalive_probes() = 9;
  // Make an accidental retry loop visible: if the invalid config were treated
  // as retryable, Connect() would take >= 2 * 2000ms and trip the elapsed
  // assertion below.
  RayConfig::instance().redis_db_connect_retries() = 5;
  RayConfig::instance().redis_db_connect_wait_milliseconds() = 2000;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  const auto start = clock.SteadyNow();
  const Status status = ConnectToLocalRedis(context);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.SteadyNow() - start)
          .count();

  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_NE(status.message().find("redis_tcp_keepalive_interval_seconds"),
            std::string::npos)
      << status.ToString();
  EXPECT_LT(elapsed_ms, 2000) << "Connect() appears to have retried a "
                                 "non-retryable keepalive configuration error.";
}

// The configuration is rejected on its own terms, not as a side effect of a
// successful TCP connect: with an unreachable endpoint the invalid config must
// still surface as InvalidArgument rather than being masked by (and delayed
// behind) the Redis connect-retry budget.
TEST_F(RedisContextKeepaliveTest, InvalidConfigIsRejectedBeforeConnecting) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = -1;
  RayConfig::instance().redis_db_connect_retries() = 5;
  RayConfig::instance().redis_db_connect_wait_milliseconds() = 2000;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  const auto start = clock.SteadyNow();
  // Port 1 has nothing listening on it.
  const Status status = context.Connect("127.0.0.1",
                                        /*port=*/1,
                                        /*username=*/"",
                                        /*password=*/"",
                                        /*enable_ssl=*/false);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(clock.SteadyNow() - start)
          .count();

  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_LT(elapsed_ms, 2000) << "Connect() reached the network before validating "
                                 "the keepalive configuration.";
}

// Probe count is validated too, and only while keepalive is enabled.
TEST_F(RedisContextKeepaliveTest, InvalidProbeCountIsRejected) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 30;
  RayConfig::instance().redis_tcp_keepalive_probes() = 0;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  const Status status = ConnectToLocalRedis(context);
  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_NE(status.message().find("redis_tcp_keepalive_probes"), std::string::npos)
      << status.ToString();
}

#endif  // !defined(_WIN32)

}  // namespace gcs
}  // namespace ray
