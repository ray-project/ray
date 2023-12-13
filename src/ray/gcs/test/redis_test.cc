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

class RedisContextTest : public ::testing::Test {
 public:
  RedisContextTest() {}
  virtual ~RedisContextTest() = default;

  static const std::string &GetTestAddress() {
    static const std::string ip = "10.0.106.72";
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

  static MockRedisContext *GetGlobalMockRedisContext() {
    static auto mock_redis_context = std::make_unique<MockRedisContext>();
    return mock_redis_context.get();
  }
};

class MockRedisContext : public RedisAsyncContext {
 public:
  MockRedisContext()
      : RedisAsyncContext(std::unique_ptr<redisAsyncContext, RedisContextDeleter>(
            new redisAsyncContext())) {
    // Ultra hacky. Populate redis_async_context_->c
    // such that redisContextFree is a noop. Might leak, actually.
    redisContext *c = &(redis_async_context_->c);
    c->flags |= REDIS_IN_CALLBACK;
  }

  MOCK_METHOD(Status,
              RedisAsyncCommandArgv,
              (redisCallbackFn * fn,
               void *privdata,
               int argc,
               const char **argv,
               const size_t *argvlen),
              (override));
};

// Hacky glue for testing.
RedisAsyncContext *CreateAsyncContext(
    std::unique_ptr<MockRedisContext, RedisContextDeleter> context) {
  return static_cast<RedisAsyncContext *>(context.release());
}

// We specialize on the mock object to mock this function.
// Checks that we reconnect to the correct port, which is the most
// important part.
template <>
std::pair<Status, std::unique_ptr<MockRedisContext, RedisContextDeleter>>
ConnectWithRetries<MockRedisContext, decltype(redisAsyncConnect)>(
    const std::string &address,
    int port,
    const decltype(redisAsyncConnect) &connect_function) {
  RAY_CHECK_EQ(address, RedisContextTest::GetTestAddress());
  RAY_CHECK_EQ(port, RedisContextTest::GetTestPort());

  // So we can assert on this at the end.
  auto &connect_tested = RedisContextTest::GetMutableConnectTested();
  connect_tested = true;

  return std::make_pair(Status::OK(),
                        std::unique_ptr<MockRedisContext, RedisContextDeleter>(
                            RedisContextTest::GetGlobalMockRedisContext()));
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
  } context_guard(base_context);

  std::unique_ptr<redisAsyncContext, RedisContextDeleter> async_context(
      base_context, RedisContextDeleter());
  std::shared_ptr<RedisAsyncContext> async_context_wrapper =
      std::make_shared<RedisAsyncContext>(std::move(async_context));
  redisReply reply;

  std::string error = "MOVED 1234 " + RedisContextTest::GetTestAddress() + ":" +
                      std::to_string(RedisContextTest::GetTestPort());

  reply.str = &error.front();
  reply.len = error.length();
  reply.type = REDIS_REPLY_ERROR;
  RedisContext parent_context(io_service);
  RedisRequestContext privdata(io_service,
                               [](std::shared_ptr<CallbackReply>) {},
                               std::move(async_context_wrapper),
                               parent_context,
                               {"HGET", "namespace", "key"});

  std::promise<bool> second_request;
  auto fut = second_request.get_future();

  // Checks that we retry.
  EXPECT_CALL(*RedisContextTest::GetGlobalMockRedisContext(),
              RedisAsyncCommandArgv(_, _, _, _, _))
      .WillOnce(
          Invoke([&second_request](
                     redisCallbackFn *, void *, int, const char **, const size_t *) {
            second_request.set_value(true);
            return Status::OK();
          }));

  // Call the function
  RedisRequestContext::RedisResponseFn<MockRedisContext>(
      base_context, static_cast<void *>(&reply), static_cast<void *>(&privdata));

  // Wait for the second callback to happen.
  fut.wait_for(std::chrono::milliseconds(3000));

  // The most important check in the test is this one.
  EXPECT_TRUE(RedisContextTest::GetMutableConnectTested());

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
  return RUN_ALL_TESTS();
}
