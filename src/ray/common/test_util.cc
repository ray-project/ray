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

#include <fstream>
#include <functional>

#include "absl/strings/escaping.h"
#include "ray/common/buffer.h"
#include "ray/common/ray_config.h"
#include "ray/common/ray_object.h"
#include "ray/common/test_util.h"
#include "ray/util/cmd_line_utils.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/network_util.h"
#include "ray/util/path_utils.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace ray {

void TestSetupUtil::StartUpRedisServers(const std::vector<int> &redis_server_ports,
                                        bool save) {
  if (redis_server_ports.empty()) {
    TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(0, save));
  } else {
    for (const auto &port : redis_server_ports) {
      TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(port, save));
    }
  }
}

// start a redis server with specified port, use random one when 0 given
int TestSetupUtil::StartUpRedisServer(int port, bool save) {
  int actual_port = port;
  if (port == 0) {
    static std::atomic<bool> srand_called(false);
    if (!srand_called.exchange(true)) {
      srand(current_time_ms() % RAND_MAX);
    }
    // Use random port (in range [2000, 7000) to avoid port conflicts between UTs.
    do {
      actual_port = rand() % 5000 + 2000;
    } while (!CheckPortFree(actual_port));
  }

  std::string program = TEST_REDIS_SERVER_EXEC_PATH;
#ifdef _WIN32
  std::vector<std::string> cmdargs({program, "--loglevel", "warning"});
#else
  std::vector<std::string> cmdargs;
  if (!save) {
    cmdargs = {program, "--loglevel", "warning", "--save", "", "--appendonly", "no"};
  } else {
    cmdargs = {program, "--loglevel", "warning"};
  }
#endif
  cmdargs.insert(cmdargs.end(), {"--port", std::to_string(actual_port)});
  RAY_LOG(INFO) << "Start redis command is: " << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true).second);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return actual_port;
}

void TestSetupUtil::ShutDownRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    ShutDownRedisServer(port);
  }
  TEST_REDIS_SERVER_PORTS = std::vector<int>();
}

void TestSetupUtil::ShutDownRedisServer(int port) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port), "shutdown"});
  RAY_LOG(INFO) << "Stop redis command is: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs) != std::error_code()) {
    RAY_LOG(WARNING) << "Failed to stop redis. The redis process may no longer exist.";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

void TestSetupUtil::FlushAllRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    FlushRedisServer(port);
  }
}

void TestSetupUtil::ExecuteRedisCmd(int port, std::vector<std::string> cmd) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port)});
  cmdargs.insert(cmdargs.end(), cmd.begin(), cmd.end());
  RAY_LOG(INFO) << "Send command to redis: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs)) {
    RAY_LOG(WARNING) << "Failed to send request to redis.";
  }
}

void TestSetupUtil::FlushRedisServer(int port) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port), "flushall"});
  RAY_LOG(INFO) << "Cleaning up redis with command: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs)) {
    RAY_LOG(WARNING) << "Failed to flush redis. The redis process may no longer exist.";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms) {
  auto status = future.wait_for(timeout_ms);
  return status == std::future_status::ready && future.get();
}

bool WaitForCondition(std::function<bool()> condition, int timeout_ms) {
  int wait_time = 0;
  while (true) {
    if (condition()) {
      return true;
    }

    // sleep 10ms.
    const int wait_interval_ms = 10;
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_interval_ms));
    wait_time += wait_interval_ms;
    if (wait_time > timeout_ms) {
      break;
    }
  }
  return false;
}

void WaitForExpectedCount(std::atomic<int> &current_count,
                          int expected_count,
                          int timeout_ms) {
  auto condition = [&current_count, expected_count]() {
    return current_count == expected_count;
  };
  EXPECT_TRUE(WaitForCondition(condition, timeout_ms));
}

TaskID RandomTaskId() {
  std::string data(TaskID::Size(), 0);
  FillRandom(&data);
  return TaskID::FromBinary(data);
}

JobID RandomJobId() {
  std::string data(JobID::Size(), 0);
  FillRandom(&data);
  return JobID::FromBinary(data);
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
  std::vector<rpc::ObjectReference> refs;
  for (const auto &inlined_id : inlined_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(inlined_id.Binary());
    refs.push_back(ref);
  }
  return std::make_shared<RayObject>(GenerateRandomBuffer(), nullptr, refs);
}

/// Path to redis server executable binary.
std::string TEST_REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
std::string TEST_REDIS_CLIENT_EXEC_PATH;
/// Ports of redis server.
std::vector<int> TEST_REDIS_SERVER_PORTS;

}  // namespace ray
