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
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "test_util.h"

namespace ray {

void TestSetupUtil::StartUpRedisServers(const std::vector<int> &redis_server_ports) {
  if (redis_server_ports.empty()) {
    TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(0));
  } else {
    for (const auto &port : redis_server_ports) {
      TEST_REDIS_SERVER_PORTS.push_back(StartUpRedisServer(port));
    }
  }
}

// start a redis server with specified port, use random one when 0 given
int TestSetupUtil::StartUpRedisServer(const int &port) {
  int actual_port = port;
  if (port == 0) {
    // Use random port (in range [2000, 7000) to avoid port conflicts between UTs.
    actual_port = rand() % 5000 + 2000;
  }

  std::string load_module_command;
  if (!TEST_REDIS_MODULE_LIBRARY_PATH.empty()) {
    // Fill load module command.
    load_module_command = "--loadmodule " + TEST_REDIS_MODULE_LIBRARY_PATH;
  }

  std::string start_redis_command = TEST_REDIS_SERVER_EXEC_PATH + " --loglevel warning " +
                                    load_module_command + " --port " +
                                    std::to_string(actual_port) + " &";
  RAY_LOG(INFO) << "Start redis command is: " << start_redis_command;
  RAY_CHECK(system(start_redis_command.c_str()) == 0);
  usleep(200 * 1000);
  return actual_port;
}

void TestSetupUtil::ShutDownRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    ShutDownRedisServer(port);
  }
  TEST_REDIS_SERVER_PORTS = std::vector<int>();
}

void TestSetupUtil::ShutDownRedisServer(const int &port) {
  std::string stop_redis_command =
      TEST_REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(port) + " shutdown";
  RAY_LOG(INFO) << "Stop redis command is: " << stop_redis_command;
  if (system(stop_redis_command.c_str()) != 0) {
    RAY_LOG(WARNING) << "Failed to stop redis. The redis process may no longer exist.";
  }
  usleep(500 * 1000);
}

void TestSetupUtil::FlushAllRedisServers() {
  for (const auto &port : TEST_REDIS_SERVER_PORTS) {
    FlushRedisServer(port);
  }
}

void TestSetupUtil::FlushRedisServer(const int &port) {
  std::string flush_all_redis_command =
      TEST_REDIS_CLIENT_EXEC_PATH + " -p " + std::to_string(port) + " flushall";
  RAY_LOG(INFO) << "Cleaning up redis with command: " << flush_all_redis_command;
  if (system(flush_all_redis_command.c_str()) != 0) {
    RAY_LOG(WARNING) << "Failed to flush redis. The redis process may no longer exist.";
  }
  usleep(100 * 1000);
}

std::string TestSetupUtil::StartObjectStore(
    const boost::optional<std::string> &socket_name) {
  std::string socket_suffix;
  if (socket_name) {
    socket_suffix = *socket_name;
  } else {
    socket_suffix = ObjectID::FromRandom().Hex();
  }
  std::string store_socket_name =
      ray::JoinPaths(ray::GetUserTempDir(), "store" + socket_suffix);
  std::string store_pid_file = store_socket_name + ".pid";
  std::string plasma_command = TEST_STORE_EXEC_PATH + " -m 10000000 -s " +
                               store_socket_name +
                               " 1> /dev/null 2> /dev/null & echo $! > " + store_pid_file;
  RAY_LOG(DEBUG) << plasma_command;
  RAY_CHECK(system(plasma_command.c_str()) == 0);
  usleep(200 * 1000);
  return store_socket_name;
}

void TestSetupUtil::StopObjectStore(const std::string &store_socket_name) {
  KillProcessBySocketName(store_socket_name);
}

std::string TestSetupUtil::StartGcsServer(const std::string &redis_address) {
  std::string gcs_server_socket_name =
      ray::JoinPaths(ray::GetUserTempDir(), "gcs_server" + ObjectID::FromRandom().Hex());
  std::string gcs_server_start_cmd = TEST_GCS_SERVER_EXEC_PATH;
  gcs_server_start_cmd.append(" --redis_address=" + redis_address)
      .append(" --redis_port=6379")
      .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
      .append(" & echo $! > " + gcs_server_socket_name + ".pid");

  RAY_LOG(INFO) << "Start gcs server command: " << gcs_server_start_cmd;
  RAY_CHECK(system(gcs_server_start_cmd.c_str()) == 0);
  usleep(200 * 1000);
  RAY_LOG(INFO) << "GCS server started.";
  return gcs_server_socket_name;
}

void TestSetupUtil::StopGcsServer(const std::string &gcs_server_socket_name) {
  KillProcessBySocketName(gcs_server_socket_name);
}

std::string TestSetupUtil::StartRaylet(const std::string &store_socket_name,
                                       const std::string &node_ip_address,
                                       const int &port, const std::string &redis_address,
                                       const std::string &resource) {
  std::string raylet_socket_name =
      ray::JoinPaths(ray::GetUserTempDir(), "raylet" + ObjectID::FromRandom().Hex());
  std::string raylet_start_cmd = TEST_RAYLET_EXEC_PATH;
  raylet_start_cmd.append(" --raylet_socket_name=" + raylet_socket_name)
      .append(" --store_socket_name=" + store_socket_name)
      .append(" --object_manager_port=0 --node_manager_port=" + std::to_string(port))
      .append(" --node_ip_address=" + node_ip_address)
      .append(" --redis_address=" + redis_address)
      .append(" --redis_port=6379")
      .append(" --min-worker-port=0")
      .append(" --max-worker-port=0")
      .append(" --num_initial_workers=1")
      .append(" --maximum_startup_concurrency=10")
      .append(" --static_resource_list=" + resource)
      .append(" --python_worker_command=\"" + TEST_MOCK_WORKER_EXEC_PATH + " " +
              store_socket_name + " " + raylet_socket_name + " " + std::to_string(port) +
              "\"")
      .append(" --config_list=initial_reconstruction_timeout_milliseconds,2000")
      .append(" & echo $! > " + raylet_socket_name + ".pid");

  RAY_LOG(DEBUG) << "Raylet Start command: " << raylet_start_cmd;
  RAY_CHECK(system(raylet_start_cmd.c_str()) == 0);
  usleep(200 * 1000);
  return raylet_socket_name;
}

void TestSetupUtil::StopRaylet(const std::string &raylet_socket_name) {
  KillProcessBySocketName(raylet_socket_name);
}

std::string TestSetupUtil::StartRayletMonitor(const std::string &redis_address) {
  std::string raylet_monitor_socket_name = ray::JoinPaths(
      ray::GetUserTempDir(), "raylet_monitor" + ObjectID::FromRandom().Hex() + ".pid");
  std::string raylet_monitor_pid = raylet_monitor_socket_name + ".pid";
  std::string raylet_monitor_start_cmd = TEST_RAYLET_MONITOR_EXEC_PATH;
  raylet_monitor_start_cmd.append(" --redis_address=" + redis_address)
      .append(" --redis_port=6379")
      .append(" & echo $! > " + raylet_monitor_pid);

  RAY_LOG(DEBUG) << "Raylet monitor Start command: " << raylet_monitor_start_cmd;
  RAY_CHECK(system(raylet_monitor_start_cmd.c_str()) == 0);
  usleep(200 * 1000);
  return raylet_monitor_socket_name;
}

void TestSetupUtil::StopRayletMonitor(const std::string &raylet_monitor_socket_name) {
  KillProcessBySocketName(raylet_monitor_socket_name);
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

void KillProcessBySocketName(std::string socket_name) {
  std::string pid = socket_name + ".pid";
  std::string kill_9 = "kill -9 `cat " + pid + "`";
  RAY_LOG(DEBUG) << kill_9;
  ASSERT_TRUE(system(kill_9.c_str()) == 0);
  ASSERT_TRUE(system(("rm -f " + pid).c_str()) == 0);
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
std::string TEST_REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
std::string TEST_REDIS_CLIENT_EXEC_PATH;
/// Path to redis module library.
std::string TEST_REDIS_MODULE_LIBRARY_PATH;
/// Ports of redis server.
std::vector<int> TEST_REDIS_SERVER_PORTS;

/// Path to object store executable binary.
std::string TEST_STORE_EXEC_PATH;

/// Path to gcs server executable binary.
std::string TEST_GCS_SERVER_EXEC_PATH;

/// Path to raylet executable binary.
std::string TEST_RAYLET_EXEC_PATH;
/// Path to mock worker executable binary. Required by raylet.
std::string TEST_MOCK_WORKER_EXEC_PATH;
/// Path to raylet monitor executable binary.
std::string TEST_RAYLET_MONITOR_EXEC_PATH;

}  // namespace ray
