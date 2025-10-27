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

#include "ray/gcs/store_client/redis_async_context.h"

#include <iostream>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/test_utils.h"
#include "ray/gcs/store_client/redis_context.h"
#include "ray/util/logging.h"
#include "ray/util/path_utils.h"
#include "ray/util/raii.h"

extern "C" {
#include "hiredis/async.h"
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {
instrumented_io_context io_service;

void ConnectCallback(const redisAsyncContext *c, int status) {
  ASSERT_EQ(status, REDIS_OK);
}

void DisconnectCallback(const redisAsyncContext *c, int status) {
  ASSERT_EQ(status, REDIS_OK);
}

void GetCallback(redisAsyncContext *c, void *r, void *privdata) {
  redisReply *reply = reinterpret_cast<redisReply *>(r);
  ASSERT_TRUE(reply != nullptr);
  ASSERT_EQ(std::string(reinterpret_cast<char *>(reply->str)), "test");
  io_service.stop();
}

class RedisAsyncContextTest : public ::testing::Test {
 public:
  RedisAsyncContextTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~RedisAsyncContextTest() { TestSetupUtil::ShutDownRedisServers(); }
};

TEST_F(RedisAsyncContextTest, TestRedisCommands) {
  redisAsyncContext *ac = redisAsyncConnect("127.0.0.1", TEST_REDIS_SERVER_PORTS.front());
  ASSERT_EQ(ac->err, 0);
  ray::gcs::RedisAsyncContext redis_async_context(
      io_service,
      std::unique_ptr<redisAsyncContext, RedisContextDeleter>(ac, RedisContextDeleter()));

  redisAsyncSetConnectCallback(ac, ConnectCallback);
  redisAsyncSetDisconnectCallback(ac, DisconnectCallback);

  redisAsyncCommand(ac, NULL, NULL, "SET key test");
  redisAsyncCommand(ac, GetCallback, nullptr, "GET key");

  std::shared_ptr<RedisContext> shard_context =
      std::make_shared<RedisContext>(io_service);
  ASSERT_TRUE(shard_context
                  ->Connect(std::string("127.0.0.1"),
                            TEST_REDIS_SERVER_PORTS.front(),
                            /*username=*/std::string(),
                            /*password=*/std::string())
                  .ok());

  io_service.run();
}
}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  InitShutdownRAII ray_log_shutdown_raii(
      ray::RayLog::StartRayLog,
      ray::RayLog::ShutDownRayLog,
      argv[0],
      ray::RayLogLevel::INFO,
      ray::GetLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::GetErrLogFilepathFromDirectory(/*log_dir=*/"", /*app_name=*/argv[0]),
      ray::RayLog::GetRayLogRotationMaxBytesOrDefault(),
      ray::RayLog::GetRayLogRotationBackupCountOrDefault());
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
