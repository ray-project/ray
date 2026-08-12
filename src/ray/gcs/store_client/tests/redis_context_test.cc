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

class RedisContextConfigTest : public ::testing::Test {
 protected:
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

  Status ConnectToUnusedPort(RedisContext &context) {
    return context.Connect("127.0.0.1",
                           /*port=*/1,
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
TEST_F(RedisContextConfigTest,
       ConnectToUnreachableEndpointReturnsErrorInsteadOfCrashing) {
  // Fail fast: try once and give up instead of retrying for ~60s.
  RayConfig::instance().redis_db_connect_retries() = 0;
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 30;
  RayConfig::instance().redis_tcp_keepalive_probes() = 3;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  const Status status = ConnectToUnusedPort(context);

  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.IsRedisError()) << status.ToString();
}

// Validate before DNS or TCP connect so configuration errors are never masked
// by an unreachable Redis endpoint or delayed by connection retries.
TEST_F(RedisContextConfigTest, InvalidKeepaliveIntervalsAreRejectedBeforeConnecting) {
  RayConfig::instance().redis_tcp_keepalive_probes() = 3;
  RayConfig::instance().redis_db_connect_retries() = 0;

  for (const int64_t invalid_interval : {-1, 32768}) {
    SCOPED_TRACE(invalid_interval);
    RayConfig::instance().redis_tcp_keepalive_interval_seconds() = invalid_interval;

    instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                       /*running_on_single_thread=*/true};
    Clock clock;
    RedisContext context(io_service, clock);
    const Status status = ConnectToUnusedPort(context);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.message().find("redis_tcp_keepalive_interval_seconds"),
              std::string::npos)
        << status.ToString();
    EXPECT_NE(status.message().find(std::to_string(invalid_interval)), std::string::npos)
        << status.ToString();
  }
}

TEST_F(RedisContextConfigTest, InvalidKeepaliveProbeCountsAreRejectedBeforeConnecting) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 30;
  RayConfig::instance().redis_db_connect_retries() = 0;

  for (const int64_t invalid_probes : {0, 128}) {
    SCOPED_TRACE(invalid_probes);
    RayConfig::instance().redis_tcp_keepalive_probes() = invalid_probes;

    instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                       /*running_on_single_thread=*/true};
    Clock clock;
    RedisContext context(io_service, clock);
    const Status status = ConnectToUnusedPort(context);

    ASSERT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.message().find("redis_tcp_keepalive_probes"), std::string::npos)
        << status.ToString();
    EXPECT_NE(status.message().find(std::to_string(invalid_probes)), std::string::npos)
        << status.ToString();
  }
}

// The keepalive tests assert on the real sockets hiredis created, via
// getsockopt(), against a live local Redis. Windows is excluded because its
// socket option API differs.
#ifndef _WIN32

namespace {

int GetIntSockOpt(int fd, int level, int optname) {
  int value = -1;
  socklen_t len = sizeof(value);
  EXPECT_EQ(getsockopt(fd, level, optname, &value, &len), 0) << strerror(errno);
  return value;
}

}  // namespace

class RedisContextKeepaliveTest : public RedisContextConfigTest {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

 protected:
  Status ConnectToLocalRedis(RedisContext &context) {
    return context.Connect("127.0.0.1",
                           TEST_REDIS_SERVER_PORTS.front(),
                           /*username=*/"",
                           /*password=*/"",
                           /*enable_ssl=*/false);
  }
};

// Both the sync and the async hiredis sockets must carry the configured
// policy at the largest values accepted by Linux.
TEST_F(RedisContextKeepaliveTest, AppliesMaximumPolicyToSyncAndAsyncSockets) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 32767;
  RayConfig::instance().redis_tcp_keepalive_probes() = 127;

  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);
  const Status status = ConnectToLocalRedis(context);
  ASSERT_TRUE(status.ok()) << status.ToString();

  const int sync_fd = context.sync_context()->fd;
  const int async_fd = context.async_context().GetRawRedisAsyncContext()->c.fd;
  for (const int fd : {sync_fd, async_fd}) {
    EXPECT_EQ(GetIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE), 1) << "fd: " << fd;
#if defined(__linux__) && defined(__GLIBC__)
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPIDLE), 32767) << "fd: " << fd;
    // Ray overrides hiredis' derived values (interval/3, 3 probes) so that
    // probe cadence and time-to-declare-dead can be tuned independently.
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPINTVL), 32767) << "fd: " << fd;
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPCNT), 127) << "fd: " << fd;
#elif defined(__APPLE__) && defined(__MACH__)
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPALIVE), 32767) << "fd: " << fd;
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
  const Status status = ConnectToLocalRedis(context);
  ASSERT_TRUE(status.ok()) << status.ToString();

  const int sync_fd = context.sync_context()->fd;
  const int async_fd = context.async_context().GetRawRedisAsyncContext()->c.fd;
  for (const int fd : {sync_fd, async_fd}) {
    EXPECT_EQ(GetIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE), 0) << "fd: " << fd;
  }
}

#endif  // !defined(_WIN32)

}  // namespace gcs
}  // namespace ray
