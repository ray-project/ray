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
#include "ray/gcs/redis_context.h"
#include "ray/util/logging.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {

class RedisContextTest : public ::testing::Test {
 public:
  RedisContextTest() {}

  virtual ~RedisContextTest() {}
};

class MockRedisContext : public RedisContext {
 public:
  MockRedisContext(instrumented_io_context &io_service, std::string address, int port)
      : RedisContext(io_service), address_(address), port_(port) {}

  // Mutable address and port to use as expectations for testing. Hacky.
  // See ConnectWithRetries above.
  std::string address_;
  int port_;

  MOCK_METHOD(void,
              RunArgvAsync,
              (std::vector<std::string> args, RedisCallback redis_callback),
              (override));
};

void FreeRedisContext(MockRedisContext *context) {}

// We specialize on the mock object to potentially mock this function.
template <>
std::pair<Status, std::unique_ptr<MockRedisContext, RedisContextDeleter<MockRedisContext>>>
ConnectWithRetries<MockRedisContext, decltype(redisAsyncConnect)>(
    const std::string &address,
    int port,
    const decltype(redisAsyncConnect) &connect_function) {
  return std::make_pair(Status::OK(), nullptr);
}

TEST_F(RedisContextTest, TestRedisMoved) {
  // Start IO service in a different thread.
  instrumented_io_context io_service;
  std::unique_ptr<std::thread> io_service_thread_ =
      std::make_unique<std::thread>([&io_service] {
        std::unique_ptr<boost::asio::io_service::work> work(
            new boost::asio::io_service::work(io_service));
        io_service.run();
      });

  // Create mock redis context.
  const std::string ip = "10.0.106.72";
  const int port = 6379;
  MockRedisContext mock_redis_context(io_service, ip, port);

  // Initialize the reply with MOVED error.
  struct redisAsyncContext base_context;
  std::unique_ptr<redisAsyncContext, RedisDeleter<redisAsyncContext>> async_context(&base_context, RedisDeleter<redisAsyncContext>());
  std::shared_ptr<RedisAsyncContext> async_context_wrapper =
      std::make_shared<RedisAsyncContext>(std::move(async_context));
  redisReply reply;
  std::string error = "MOVED 1234 " + ip + ":" + std::to_string(port);
  reply.str = &error.front();
  reply.len = error.length();
  reply.type = REDIS_REPLY_ERROR;
  RedisRequestContext privdata(io_service,
                               [](std::shared_ptr<CallbackReply>) {},
                               async_context_wrapper,
                               {"HGET", "namespace", "key"});

  // TODO set expectations
  // EXPECT_CALL(mock_redis_context, RunArgvAsync, );

  // Call the function
  RedisRequestContext::RedisResponseFn(
      &base_context, static_cast<void *>(&reply), static_cast<void *>(&privdata));

  async_context_wrapper->ResetRawRedisAsyncContext();
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
