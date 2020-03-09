// Copyright 2020 The Ray Authors.
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

#ifndef RAY_UTIL_TEST_UTIL_H
#define RAY_UTIL_TEST_UTIL_H

#include <unistd.h>

#include <string>

#include "gtest/gtest.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/util/util.h"

namespace ray {

// Magic argument to signal to mock_worker we should check message order.
int64_t SHOULD_CHECK_MESSAGE_ORDER = 123450000;

/// Wait until the condition is met, or timeout is reached.
///
/// \param[in] condition The condition to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the condition is met.
bool WaitForCondition(std::function<bool()> condition, int timeout_ms) {
  int wait_time = 0;
  while (true) {
    if (condition()) {
      return true;
    }

    // sleep 10ms.
    const int wait_interval_ms = 10;
    usleep(wait_interval_ms * 1000);
    wait_time += wait_interval_ms;
    if (wait_time > timeout_ms) {
      break;
    }
  }
  return false;
}

// A helper function to return a random task id.
inline TaskID RandomTaskId() {
  std::string data(TaskID::Size(), 0);
  FillRandom(&data);
  return TaskID::FromBinary(data);
}

std::shared_ptr<Buffer> GenerateRandomBuffer() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<> dis(1, 10);
  std::uniform_int_distribution<> value_dis(1, 255);

  std::vector<uint8_t> arg1(dis(gen), value_dis(gen));
  return std::make_shared<LocalMemoryBuffer>(arg1.data(), arg1.size(), true);
}

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids = {}) {
  return std::shared_ptr<RayObject>(
      new RayObject(GenerateRandomBuffer(), nullptr, inlined_ids));
}

/// Path to redis server executable binary.
std::string REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
std::string REDIS_CLIENT_EXEC_PATH;
/// Path to redis module library.
std::string REDIS_MODULE_LIBRARY_PATH;
/// Port of redis server.
int REDIS_SERVER_PORT;

/// Test helper class, it will start redis server before the test runs,
/// and stop redis server after the test is completed.
class RedisServiceManagerForTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> random_gen{2000, 7000};
    // Use random port to avoid port conflicts between UTs.
    REDIS_SERVER_PORT = random_gen(gen);

    std::string start_redis_command =
        REDIS_SERVER_EXEC_PATH + " --loglevel warning --loadmodule " +
        REDIS_MODULE_LIBRARY_PATH + " --port " + std::to_string(REDIS_SERVER_PORT) + " &";
    RAY_LOG(INFO) << "Start redis command is: " << start_redis_command;
    RAY_CHECK(system(start_redis_command.c_str()) == 0);
    usleep(200 * 1000);
  }

  static void TearDownTestCase() {
    std::string stop_redis_command =
        REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(REDIS_SERVER_PORT) + " shutdown";
    RAY_LOG(INFO) << "Stop redis command is: " << stop_redis_command;
    RAY_CHECK(system(stop_redis_command.c_str()) == 0);
    usleep(100 * 1000);
  }
};

}  // namespace ray

#endif  // RAY_UTIL_TEST_UTIL_H
