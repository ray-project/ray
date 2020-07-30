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

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/test_util.h"
#include "ray/util/filesystem.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

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
    static std::atomic<bool> srand_called(false);
    if (!srand_called.exchange(true)) {
      srand(current_time_ms() % RAND_MAX);
    }
    // Use random port (in range [2000, 7000) to avoid port conflicts between UTs.
    actual_port = rand() % 5000 + 2000;
  }

  std::string program = TEST_REDIS_SERVER_EXEC_PATH;
  std::vector<std::string> cmdargs({program, "--loglevel", "warning"});
  if (!TEST_REDIS_MODULE_LIBRARY_PATH.empty()) {
    cmdargs.insert(cmdargs.end(), {"--loadmodule", TEST_REDIS_MODULE_LIBRARY_PATH});
  }
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

void TestSetupUtil::ShutDownRedisServer(const int &port) {
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

void TestSetupUtil::FlushRedisServer(const int &port) {
  std::vector<std::string> cmdargs(
      {TEST_REDIS_CLIENT_EXEC_PATH, "-p", std::to_string(port), "flushall"});
  RAY_LOG(INFO) << "Cleaning up redis with command: " << CreateCommandLine(cmdargs);
  if (Process::Call(cmdargs)) {
    RAY_LOG(WARNING) << "Failed to flush redis. The redis process may no longer exist.";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
  std::vector<std::string> cmdargs(
      {TEST_STORE_EXEC_PATH, "-m", "10000000", "-s", store_socket_name});
  RAY_LOG(DEBUG) << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true, store_socket_name + ".pid").second);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return store_socket_name;
}

void TestSetupUtil::StopObjectStore(const std::string &store_socket_name) {
  KillProcessBySocketName(store_socket_name);
}

std::string TestSetupUtil::StartGcsServer(const std::string &redis_address) {
  std::string gcs_server_socket_name =
      ray::JoinPaths(ray::GetUserTempDir(), "gcs_server" + ObjectID::FromRandom().Hex());
  std::vector<std::string> cmdargs(
      {TEST_GCS_SERVER_EXEC_PATH, "--redis_address=" + redis_address, "--redis_port=6379",
       "--config_list=initial_reconstruction_timeout_milliseconds,2000"});
  RAY_LOG(INFO) << "Start gcs server command: " << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true, gcs_server_socket_name + ".pid").second);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
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
  std::vector<std::string> cmdargs(
      {TEST_RAYLET_EXEC_PATH, "--raylet_socket_name=" + raylet_socket_name,
       "--store_socket_name=" + store_socket_name, "--object_manager_port=0",
       "--node_manager_port=" + std::to_string(port),
       "--node_ip_address=" + node_ip_address, "--redis_address=" + redis_address,
       "--redis_port=6379", "--min-worker-port=0", "--max-worker-port=0",
       "--num_initial_workers=1", "--maximum_startup_concurrency=10",
       "--static_resource_list=" + resource,
       "--python_worker_command=" +
           CreateCommandLine({TEST_MOCK_WORKER_EXEC_PATH, store_socket_name,
                              raylet_socket_name, std::to_string(port)}),
       "--config_list=initial_reconstruction_timeout_milliseconds,2000"});
  RAY_LOG(DEBUG) << "Raylet Start command: " << CreateCommandLine(cmdargs);
  RAY_CHECK(!Process::Spawn(cmdargs, true, raylet_socket_name + ".pid").second);
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  return raylet_socket_name;
}

void TestSetupUtil::StopRaylet(const std::string &raylet_socket_name) {
  KillProcessBySocketName(raylet_socket_name);
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

void KillProcessBySocketName(std::string socket_name) {
  std::string pidfile_path = socket_name + ".pid";
  {
    std::ifstream pidfile(pidfile_path, std::ios_base::in);
    RAY_CHECK(pidfile.good());
    pid_t pid = -1;
    pidfile >> pid;
    RAY_CHECK(pid != -1);
    Process::FromPid(pid).Kill();
  }
  ASSERT_EQ(unlink(pidfile_path.c_str()), 0);
}

int KillAllExecutable(const std::string &executable) {
  std::vector<std::string> cmdargs;
#ifdef _WIN32
  cmdargs.insert(cmdargs.end(), {"taskkill", "/IM", executable});
#else
  cmdargs.insert(cmdargs.end(), {"pkill", "-x", executable});
#endif
  return Process::Call(cmdargs).value();
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

}  // namespace ray
