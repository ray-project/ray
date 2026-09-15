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
#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif
#endif

#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/redis_tcp_keepalive.h"
#include "ray/util/clock.h"

#ifdef __linux__
namespace {

// Only this test binary wraps setsockopt. The override is scoped to the thread
// calling Connect; unrelated options and calls outside that scope reach libc.
thread_local const std::function<bool(int, int, int)> *setsockopt_failure = nullptr;

class ScopedSetsockoptFailure {
 public:
  explicit ScopedSetsockoptFailure(std::function<bool(int, int, int)> failure)
      : failure_(std::move(failure)),
        previous_(std::exchange(setsockopt_failure, &failure_)) {}

  ~ScopedSetsockoptFailure() { setsockopt_failure = previous_; }

  ScopedSetsockoptFailure(const ScopedSetsockoptFailure &) = delete;
  ScopedSetsockoptFailure &operator=(const ScopedSetsockoptFailure &) = delete;

 private:
  const std::function<bool(int, int, int)> failure_;
  const std::function<bool(int, int, int)> *previous_;
};

}  // namespace

#if defined(__GLIBC__)
#define RAY_TEST_SOCKET_NOEXCEPT noexcept
#else
#define RAY_TEST_SOCKET_NOEXCEPT
#endif
extern "C" int setsockopt(int fd,
                          int level,
                          int option,
                          const void *value,
                          socklen_t length) RAY_TEST_SOCKET_NOEXCEPT {
  if (setsockopt_failure != nullptr && (*setsockopt_failure)(fd, level, option)) {
    errno = EPERM;
    return -1;
  }
  return static_cast<int>(::syscall(SYS_setsockopt, fd, level, option, value, length));
}
#undef RAY_TEST_SOCKET_NOEXCEPT
#endif

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

  void CheckCommands(RedisContext &context) {
    auto *reply = static_cast<redisReply *>(redisCommand(context.sync_context(), "PING"));
    ASSERT_NE(reply, nullptr);
    EXPECT_EQ(reply->type, REDIS_REPLY_STATUS);
    EXPECT_STREQ(reply->str, "PONG");
    freeReplyObject(reply);

    auto completed = std::make_shared<bool>(false);
    context.RunArgvAsync(
        {"ECHO", "keepalive"},
        [&, completed](std::shared_ptr<CallbackReply> response) {
          *completed = true;
          EXPECT_EQ(response->ReadAsString(), "keepalive");
          context.io_service().stop();
        },
        kNoTable);
    context.io_service().restart();
    context.io_service().run_for(std::chrono::seconds(5));
    EXPECT_TRUE(*completed);
  }
};

class RedisContextKeepalivePolicyTest
    : public RedisContextKeepaliveTest,
      public ::testing::WithParamInterface<std::pair<int, int>> {};

// Cover the defaults, a custom policy, and Linux's accepted maximum values.
TEST_P(RedisContextKeepalivePolicyTest, AppliesPolicyToSyncAndAsyncSockets) {
  const auto [interval, probes] = GetParam();
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = interval;
  RayConfig::instance().redis_tcp_keepalive_probes() = probes;

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
#if defined(__linux__) && defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && \
    defined(TCP_KEEPCNT)
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPIDLE), interval) << "fd: " << fd;
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPINTVL), interval) << "fd: " << fd;
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPCNT), probes) << "fd: " << fd;
#elif defined(__APPLE__) && defined(__MACH__)
    EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, TCP_KEEPALIVE), interval) << "fd: " << fd;
#endif
  }
  CheckCommands(context);
}

INSTANTIATE_TEST_SUITE_P(Policies,
                         RedisContextKeepalivePolicyTest,
                         ::testing::Values(std::make_pair(30, 3),
                                           std::make_pair(7, 4),
                                           std::make_pair(32767, 127)));

#if (defined(__linux__) && defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && \
     defined(TCP_KEEPCNT)) ||                                                 \
    (defined(__APPLE__) && defined(__MACH__))
class RedisContextKeepaliveFailureTest : public RedisContextKeepaliveTest,
                                         public ::testing::WithParamInterface<int> {};

TEST_P(RedisContextKeepaliveFailureTest, TuningFailurePreservesUsableConnections) {
  // Establish fresh sockets with OS defaults, then inject failures only at the
  // keepalive helper's syscall boundary. Both hiredis contexts must remain usable.
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 0;
  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);
  ASSERT_TRUE(ConnectToLocalRedis(context).ok());

  const std::vector<std::pair<int, int>> timing_options = {
#if defined(__linux__)
    {TCP_KEEPIDLE, 7},
    {TCP_KEEPINTVL, 7},
    {TCP_KEEPCNT, 4},
#else
    {TCP_KEEPALIVE, 7},
#endif
  };
  for (auto *raw :
       {context.sync_context(), &context.async_context().GetRawRedisAsyncContext()->c}) {
    std::vector<int> previous_values;
    for (const auto &[option, value] : timing_options) {
      previous_values.push_back(GetIntSockOpt(raw->fd, IPPROTO_TCP, option));
    }
    const auto result = internal::SetRedisTcpKeepalive(
        7, 4, [&](int level, int option, const char *, int value) {
          if (level == IPPROTO_TCP && (GetParam() == -1 || option == GetParam())) {
            errno = EPERM;
            return false;
          }
          return setsockopt(raw->fd, level, option, &value, sizeof(value)) == 0;
        });
    EXPECT_EQ(result, internal::RedisTcpKeepaliveResult::kDegraded);
    EXPECT_EQ(raw->err, 0);
    EXPECT_EQ(GetIntSockOpt(raw->fd, SOL_SOCKET, SO_KEEPALIVE), 1);
    for (size_t i = 0; i < timing_options.size(); ++i) {
      const auto [option, value] = timing_options[i];
      const int expected =
          GetParam() == -1 || option == GetParam() ? previous_values[i] : value;
      EXPECT_EQ(GetIntSockOpt(raw->fd, IPPROTO_TCP, option), expected);
    }
  }
  CheckCommands(context);
}

INSTANTIATE_TEST_SUITE_P(RejectedOptions,
                         RedisContextKeepaliveFailureTest,
#if defined(__linux__)
                         ::testing::Values(TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT, -1)
#else
                         ::testing::Values(TCP_KEEPALIVE, -1)
#endif
);
#endif

TEST_F(RedisContextKeepaliveTest, EnableFailureStopsBeforeTuning) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 0;
  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);
  ASSERT_TRUE(ConnectToLocalRedis(context).ok());
  for (auto *raw :
       {context.sync_context(), &context.async_context().GetRawRedisAsyncContext()->c}) {
    int calls = 0;
    const auto result = internal::SetRedisTcpKeepalive(
        7, 4, [&](int level, int option, const char *, int) {
          ++calls;
          EXPECT_EQ(level, SOL_SOCKET);
          EXPECT_EQ(option, SO_KEEPALIVE);
          errno = EPERM;
          return false;
        });
    EXPECT_EQ(result, internal::RedisTcpKeepaliveResult::kFailed);
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(GetIntSockOpt(raw->fd, SOL_SOCKET, SO_KEEPALIVE), 0);
    EXPECT_EQ(raw->err, 0);
  }
}

#if defined(__linux__) && defined(TCP_KEEPIDLE) && defined(TCP_KEEPINTVL) && \
    defined(TCP_KEEPCNT)
class RedisContextKeepaliveConnectFailureTest
    : public RedisContextKeepaliveTest,
      public ::testing::WithParamInterface<int> {};

TEST_P(RedisContextKeepaliveConnectFailureTest, TuningFailureAllowsConnectAndCommands) {
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 7;
  RayConfig::instance().redis_tcp_keepalive_probes() = 4;
  RayConfig::instance().redis_db_connect_retries() = 3;
  RayConfig::instance().redis_db_connect_wait_milliseconds() = 0;
  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  const std::map<int, int> timing_options = {
      {TCP_KEEPIDLE, 7}, {TCP_KEEPINTVL, 7}, {TCP_KEEPCNT, 4}};
  std::map<std::pair<int, int>, int> rejected_values;
  std::vector<int> configured_fds;
  size_t timing_calls = 0;
  Status status;
  {
    ScopedSetsockoptFailure failure([&](int fd, int level, int option) {
      if (level == SOL_SOCKET && option == SO_KEEPALIVE) {
        configured_fds.push_back(fd);
      }
      if (level == IPPROTO_TCP && timing_options.count(option) != 0) {
        ++timing_calls;
        if (GetParam() == -1 || option == GetParam()) {
          rejected_values[{fd, option}] = GetIntSockOpt(fd, level, option);
          return true;
        }
      }
      return false;
    });
    status = ConnectToLocalRedis(context);
  }
  ASSERT_TRUE(status.ok()) << status.ToString();

  const std::vector<int> fds = {context.sync_context()->fd,
                                context.async_context().GetRawRedisAsyncContext()->c.fd};
  EXPECT_EQ(configured_fds, fds);
  EXPECT_EQ(timing_calls, fds.size() * timing_options.size());
  ASSERT_EQ(rejected_values.size(),
            fds.size() * (GetParam() == -1 ? timing_options.size() : 1));
  for (const int fd : fds) {
    EXPECT_EQ(GetIntSockOpt(fd, SOL_SOCKET, SO_KEEPALIVE), 1);
    for (const auto &[option, requested] : timing_options) {
      const auto rejected = rejected_values.find({fd, option});
      const int expected =
          rejected == rejected_values.end() ? requested : rejected->second;
      EXPECT_EQ(GetIntSockOpt(fd, IPPROTO_TCP, option), expected);
    }
  }
  EXPECT_EQ(context.sync_context()->err, 0);
  EXPECT_EQ(context.async_context().GetRawRedisAsyncContext()->c.err, 0);
  CheckCommands(context);
}

INSTANTIATE_TEST_SUITE_P(RejectedOptions,
                         RedisContextKeepaliveConnectFailureTest,
                         ::testing::Values(TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT, -1));

class RedisContextKeepaliveEnableFailureTest
    : public RedisContextKeepaliveTest,
      public ::testing::WithParamInterface<bool> {};

TEST_P(RedisContextKeepaliveEnableFailureTest, EnableFailureDoesNotRetryAndCleansUp) {
  const bool fail_async = GetParam();
  RayConfig::instance().redis_tcp_keepalive_interval_seconds() = 7;
  RayConfig::instance().redis_tcp_keepalive_probes() = 4;
  RayConfig::instance().redis_db_connect_retries() = 3;
  RayConfig::instance().redis_db_connect_wait_milliseconds() = 0;
  instrumented_io_context io_service{/*enable_lag_probe=*/false,
                                     /*running_on_single_thread=*/true};
  Clock clock;
  RedisContext context(io_service, clock);

  int enable_calls = 0;
  int timing_calls = 0;
  Status status;
  {
    ScopedSetsockoptFailure failure([&](int, int level, int option) {
      if (level == SOL_SOCKET && option == SO_KEEPALIVE) {
        ++enable_calls;
        // Let the sync context connect when exercising async setup failure.
        return !fail_async || enable_calls > 1;
      }
      if (level == IPPROTO_TCP &&
          (option == TCP_KEEPIDLE || option == TCP_KEEPINTVL || option == TCP_KEEPCNT)) {
        ++timing_calls;
      }
      return false;
    });
    status = ConnectToLocalRedis(context);
  }
  EXPECT_TRUE(status.IsIOError()) << status.ToString();
  // Count calls with a nonzero retry budget instead of relying on elapsed time.
  EXPECT_EQ(enable_calls, fail_async ? 2 : 1);
  EXPECT_EQ(timing_calls, fail_async ? 3 : 0);

  // The same object must be reusable after either partial setup failure.
  ASSERT_TRUE(ConnectToLocalRedis(context).ok());
  CheckCommands(context);
}

INSTANTIATE_TEST_SUITE_P(SyncAndAsync,
                         RedisContextKeepaliveEnableFailureTest,
                         ::testing::Bool());
#endif

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
