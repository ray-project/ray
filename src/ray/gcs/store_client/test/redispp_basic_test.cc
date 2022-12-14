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

#include <thread>
#include <chrono>

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

#include <sw/redis++/redis++.h>
using namespace ray;
using namespace sw::redis;
using namespace std::chrono_literals;

class RedisPlusPlusTest : public ::testing::Test {
 public:
  RedisPlusPlusTest() {
    TestSetupUtil::StartUpRedisServers(std::vector<int>());
    std::this_thread::sleep_for(1s);
  }

  virtual ~RedisPlusPlusTest() { TestSetupUtil::ShutDownRedisServers(); }
};

TEST_F(RedisPlusPlusTest, Basic) {
  auto port = TEST_REDIS_SERVER_PORTS.front();
  auto address = std::string("tcp://127.0.0.1:") + std::to_string(port);
  RAY_LOG(INFO) << "Connecting to: " << address;
  auto redis = Redis(address);
  redis.set("key", "val");
  auto val = redis.get("key");
  ASSERT_EQ(*val, "val");
}

int main(int argc, char **argv) {
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];

  ::testing::InitGoogleTest(&argc, argv);
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO,
                                         /*log_dir=*/"");
  return RUN_ALL_TESTS();
}
