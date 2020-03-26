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

#include "ray/common/test_util.h"

#include <functional>

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/util/logging.h"

namespace ray {

void RedisServiceManagerForTest::SetUpTestCase() {
  auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> random_gen{2000, 7000};
  // Use random port to avoid port conflicts between UTs.
  REDIS_SERVER_PORT = random_gen(gen);

  std::string load_module_command;
  if (!REDIS_MODULE_LIBRARY_PATH.empty()) {
    // Fill load module command.
    load_module_command = "--loadmodule " + REDIS_MODULE_LIBRARY_PATH;
  }

  std::string start_redis_command = REDIS_SERVER_EXEC_PATH + " --loglevel warning " +
                                    load_module_command + " --port " +
                                    std::to_string(REDIS_SERVER_PORT) + " &";
  RAY_LOG(INFO) << "Start redis command is: " << start_redis_command;
  RAY_CHECK(system(start_redis_command.c_str()) == 0);
  usleep(200 * 1000);
}

void RedisServiceManagerForTest::TearDownTestCase() {
  std::string stop_redis_command =
      REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(REDIS_SERVER_PORT) + " shutdown";
  RAY_LOG(INFO) << "Stop redis command is: " << stop_redis_command;
  if (system(stop_redis_command.c_str()) != 0) {
    RAY_LOG(WARNING) << "Failed to stop redis. The redis process may no longer exist.";
  }
  usleep(100 * 1000);
}

void RedisServiceManagerForTest::FlushAll() {
  std::string flush_all_redis_command =
      REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(REDIS_SERVER_PORT) + " flushall";
  RAY_LOG(INFO) << "Cleaning up redis with command: " << flush_all_redis_command;
  if (system(flush_all_redis_command.c_str()) != 0) {
    RAY_LOG(WARNING) << "Failed to flush redis. The redis process may no longer exist.";
  }
  usleep(100 * 1000);
}

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

TaskID RandomTaskId() {
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
    const std::vector<ObjectID> &inlined_ids) {
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

}  // namespace ray
