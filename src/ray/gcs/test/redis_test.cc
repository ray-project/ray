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

#include <iostream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_util.h"
#include "ray/gcs/redis_async_context.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context-inl.h"
#include "ray/util/logging.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

class MockRedisAsyncContext : public RedisAsyncContext {
 public:
  MockRedisAsyncContext()
      : RedisAsyncContext(std::unique_ptr<redisAsyncContext, RedisContextDeleter>(
            new redisAsyncContext())) {
    // Ultra hacky. Populate redis_async_context_->c
    // such that redisContextFree is a noop. Might leak, actually.
    redisContext *c = &(redis_async_context_->c);
    c->flags |= REDIS_IN_CALLBACK;
  }

  MOCK_METHOD(redisAsyncContext *, GetRawRedisAsyncContext, (), (override));

  MOCK_METHOD(Status,
              RedisAsyncCommandArgv,
              (redisCallbackFn * fn,
               void *privdata,
               int argc,
               const char **argv,
               const size_t *argvlen),
              (override));
};

class RedisContextTest : public ::testing::Test {
 public:
  RedisContextTest() {}
  virtual ~RedisContextTest() = default;

  static const std::string &GetTestAddress() {
    static const std::string ip = "127.0.0.1";
    return ip;
  }

  static const int &GetTestPort() {
    static const int port = 6379;
    return port;
  }

  static bool &GetMutableConnectTested() {
    static bool connect_tested = false;
    return connect_tested;
  }

  static MockRedisAsyncContext *GetGlobalMockRedisAsyncContext() {
    static auto mock_redis_context = std::make_unique<MockRedisAsyncContext>();
    return mock_redis_context.get();
  }
};

class MockRedisClient : public RedisClient {
 public:
  MockRedisClient(RedisClientOptions options) : RedisClient(options) {}

  MOCK_METHOD(void, ReattachContext, (RedisContext &), (override));
};

// Hacky glue for testing.
std::shared_ptr<RedisAsyncContext> CreateAsyncContext(
    std::unique_ptr<MockRedisAsyncContext, RedisContextDeleter> context) {
  return std::shared_ptr<RedisAsyncContext>(
      static_cast<RedisAsyncContext *>(context.release()));
}

// We specialize on the mock object to mock this function.
// Checks that we reconnect to the correct port, which is the most
// important part.
template <>
std::pair<Status, std::unique_ptr<MockRedisAsyncContext, RedisContextDeleter>>
ConnectWithRetries<MockRedisAsyncContext, decltype(redisAsyncConnect)>(
    const std::string &address, int port, decltype(redisAsyncConnect) &connect_function) {
  RAY_CHECK_EQ(address, RedisContextTest::GetTestAddress());
  RAY_CHECK_EQ(port, RedisContextTest::GetTestPort());

  // So we can assert on this at the end.
  auto &connect_tested = RedisContextTest::GetMutableConnectTested();
  connect_tested = true;

  return std::make_pair(Status::OK(),
                        std::unique_ptr<MockRedisAsyncContext, RedisContextDeleter>(
                            RedisContextTest::GetGlobalMockRedisAsyncContext()));
}

void SetAddress(redisAsyncContext &ctx,
                const std::string &host,
                const std::string &addr,
                int port) {
  ctx.c.tcp.host = const_cast<char *>(host.c_str());
  ctx.c.tcp.source_addr = const_cast<char *>(addr.c_str());
  ctx.c.tcp.port = port;
}

TEST_F(RedisContextTest, TestRedisMoved) {
  EXPECT_FALSE(RedisContextTest::GetMutableConnectTested());

  // Start IO service in a different thread.
  instrumented_io_context io_service;
  std::unique_ptr<std::thread> io_service_thread_ =
      std::make_unique<std::thread>([&io_service] {
        std::unique_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work(io_service));
        io_service.run_for(std::chrono::milliseconds(10000));
      });

  // Initialize the reply with MOVED error.
  auto base_context = new redisAsyncContext();
  const std::string hostname = "hostname";
  SetAddress(*base_context,
             hostname,
             RedisContextTest::GetTestAddress(),
             RedisContextTest::GetTestPort());

  // This will delete our created base context for us. We bypass redisAsyncFree
  // because it expects very specific values in the struct.
  struct ContextGuard {
    redisAsyncContext *raw_context_;
    ContextGuard(redisAsyncContext *raw_context) : raw_context_(raw_context) {
      // Setting this flag skips the free in redisAsyncFree.
      redisContext *c = &(raw_context_->c);
      c->flags |= REDIS_IN_CALLBACK;
    }
    ~ContextGuard() {
      // We manually delete it here.
      delete raw_context_;
    }
  };

  ContextGuard g1(base_context);

  std::unique_ptr<redisAsyncContext, RedisContextDeleter> async_context(
      base_context, RedisContextDeleter());
  std::shared_ptr<RedisAsyncContext> async_context_wrapper =
      std::make_shared<RedisAsyncContext>(std::move(async_context));

  auto old_context = new redisAsyncContext();
  ContextGuard g2(old_context);
  const std::string hostname2 = "hostname2";
  const std::string ip2 = "10.0.106.73";
  SetAddress(*old_context, hostname2, ip2, RedisContextTest::GetTestPort());
  std::shared_ptr<RedisAsyncContext> old_context_wrapper =
      std::make_shared<RedisAsyncContext>(
          std::unique_ptr<redisAsyncContext, RedisContextDeleter>(old_context,
                                                                  RedisContextDeleter()));
  redisReply reply;

  std::string error = "MOVED 1234 " + RedisContextTest::GetTestAddress() + ":" +
                      std::to_string(RedisContextTest::GetTestPort());

  reply.str = &error.front();
  reply.len = error.length();
  reply.type = REDIS_REPLY_ERROR;

  std::string fake_client_ip = "";
  std::string fake_client_pw = "";
  RedisClientOptions fake_options(fake_client_ip, 0, fake_client_pw);
  MockRedisClient redis_client(fake_options);
  RedisContext parent_context(io_service, redis_client);

  parent_context.SetRedisAsyncContextInTest(old_context_wrapper);
  RedisRequestContext *privdata =
      new RedisRequestContext(io_service,
                              [](std::shared_ptr<CallbackReply>) {},
                              parent_context,
                              {"HGET", "namespace", "key"});

  std::promise<bool> second_request;
  auto fut = second_request.get_future();

  // Checks that we retry.
  EXPECT_CALL(*RedisContextTest::GetGlobalMockRedisAsyncContext(),
              RedisAsyncCommandArgv(_, _, _, _, _))
      .WillOnce(
          Invoke([&second_request](
                     redisCallbackFn *, void *, int, const char **, const size_t *) {
            second_request.set_value(true);
            return Status::OK();
          }));

  // Whether or not this new asyncContext matches the existing one.
  EXPECT_CALL(*RedisContextTest::GetGlobalMockRedisAsyncContext(),
              GetRawRedisAsyncContext())
      .WillOnce(Return(base_context));

  // Call the function
  RedisResponseFn<MockRedisAsyncContext>(
      base_context, static_cast<void *>(&reply), static_cast<void *>(privdata));

  // Wait for the second callback to happen.
  fut.wait_for(std::chrono::milliseconds(3000));

  // The most important check in the test is this one.
  EXPECT_TRUE(RedisContextTest::GetMutableConnectTested());

  io_service.stop();
  io_service_thread_->join();
}

// Same as above test, but instead of checking for the Connect call,
// we check that we actually connected to Redis.
TEST_F(RedisContextTest, TestRedisMovedRealConnect) {
  struct RedisServers {
    RedisServers() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }
    ~RedisServers() { TestSetupUtil::ShutDownRedisServers(); }
  } redis_servers;

  // Start IO service in a different thread.
  instrumented_io_context io_service;
  std::unique_ptr<std::thread> io_service_thread_ =
      std::make_unique<std::thread>([&io_service] {
        std::unique_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work(io_service));
        io_service.run_for(std::chrono::milliseconds(10000));
      });

  // Initialize the reply with MOVED error.
  auto base_context = new redisAsyncContext();
  const std::string hostname = "hostname";
  SetAddress(*base_context,
             hostname,
             RedisContextTest::GetTestAddress(),
             RedisContextTest::GetTestPort());

  // This will delete our created base context for us. We bypass redisAsyncFree
  // because it expects very specific values in the struct.
  struct ContextGuard {
    redisAsyncContext *raw_context_;
    ContextGuard(redisAsyncContext *raw_context) : raw_context_(raw_context) {
      // Setting this flag skips the free in redisAsyncFree.
      redisContext *c = &(raw_context_->c);
      c->flags |= REDIS_IN_CALLBACK;
    }
    ~ContextGuard() {
      // We manually delete it here.
      delete raw_context_;
    }
  };

  ContextGuard g1(base_context);

  std::unique_ptr<redisAsyncContext, RedisContextDeleter> async_context(
      base_context, RedisContextDeleter());
  std::shared_ptr<RedisAsyncContext> async_context_wrapper =
      std::make_shared<RedisAsyncContext>(std::move(async_context));

  auto old_context = new redisAsyncContext();
  ContextGuard g2(old_context);
  const std::string hostname2 = "hostname2";
  const std::string ip2 = "10.0.106.73";
  SetAddress(*old_context, hostname2, ip2, RedisContextTest::GetTestPort());
  std::shared_ptr<RedisAsyncContext> old_context_wrapper =
      std::make_shared<RedisAsyncContext>(
          std::unique_ptr<redisAsyncContext, RedisContextDeleter>(old_context,
                                                                  RedisContextDeleter()));
  redisReply reply;

  std::string error = "MOVED 1234 " + RedisContextTest::GetTestAddress() + ":" +
                      std::to_string(TEST_REDIS_SERVER_PORTS.front());

  reply.str = &error.front();
  reply.len = error.length();
  reply.type = REDIS_REPLY_ERROR;

  // Create the client but don't connect it.
  std::string test_pw = "";
  RedisClientOptions fake_options(
      RedisContextTest::GetTestAddress(), TEST_REDIS_SERVER_PORTS.front(), test_pw);
  RedisClient redis_client(fake_options);
  // RAY_CHECK_OK(redis_client.Connect(io_service));

  RedisContext parent_context(io_service, redis_client);

  parent_context.SetRedisAsyncContextInTest(old_context_wrapper);
  RedisRequestContext *privdata =
      new RedisRequestContext(io_service,
                              [](std::shared_ptr<CallbackReply>) {},
                              parent_context,
                              {"HGET", "namespace", "key"});

  std::promise<bool> second_request;
  auto fut = second_request.get_future();

  // Call the function
  RedisResponseFn<redisAsyncContext>(
      base_context, static_cast<void *>(&reply), static_cast<void *>(privdata));

  // Wait for the second callback to happen.
  fut.wait_for(std::chrono::milliseconds(3000));

  io_service.stop();
  io_service_thread_->join();
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
