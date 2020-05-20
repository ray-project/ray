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

#ifndef RAY_COMMON_TEST_UTIL_H
#define RAY_COMMON_TEST_UTIL_H

#include <unistd.h>

#include <functional>
#include <string>

#include <boost/optional.hpp>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/util/util.h"

namespace ray {

class Buffer;
class RayObject;

// Magic argument to signal to mock_worker we should check message order.
static const int64_t SHOULD_CHECK_MESSAGE_ORDER = 123450000;

/// Wait until the condition is met, or timeout is reached.
///
/// \param[in] condition The condition to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the condition is met.
bool WaitForCondition(std::function<bool()> condition, int timeout_ms);

/// Used to kill process whose pid is stored in `socket_name.id` file.
void KillProcessBySocketName(std::string socket_name);

// A helper function to return a random task id.
TaskID RandomTaskId();

std::shared_ptr<Buffer> GenerateRandomBuffer();

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids = {});

/// Path to redis server executable binary.
extern std::string REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
extern std::string REDIS_CLIENT_EXEC_PATH;
/// Path to redis module library.
extern std::string REDIS_MODULE_LIBRARY_PATH;
/// Ports of redis server.
extern std::vector<int> REDIS_SERVER_PORTS;

/// Path to object store executable binary.
extern std::string STORE_EXEC_PATH;

/// Path to gcs server executable binary.
extern std::string GCS_SERVER_EXEC_PATH;

/// Path to raylet executable binary.
extern std::string RAYLET_EXEC_PATH;
/// Path to mock worker executable binary. Required by raylet.
extern std::string MOCK_WORKER_EXEC_PATH;
/// Path to raylet monitor executable binary.
extern std::string RAYLET_MONITOR_EXEC_PATH;

//--------------------------------------------------------------------------------
// COMPONENT MANAGEMENT CLASSES FOR TEST CASES
//--------------------------------------------------------------------------------
/// Test cases can use it to 1) start redis server(s), 2) stop redis server(s) and 3)
/// flush all redis servers.
class RedisServiceManagerForTest {
 public:
  static void StartUpRedisServers(std::vector<int> redis_server_ports);
  static void ShutDownRedisServers();
  static void FlushAllRedisServers();

 private:
  static int StartUpRedisServer(int port);
  static void ShutDownRedisServer(int port);
  static void FlushRedisServer(int port);
};

/// Test cases can use it to 1) start object store and 2) stop object store.
class ObjectStoreManagerForTest {
 public:
  static std::string StartStore(
      const boost::optional<std::string> &socket_name = boost::none);
  static void StopStore(const std::string &store_socket_name);
};

/// Test cases can use it to 1) start gcs server and 2) stop gcs server.
class GcsServerManagerForTest {
 public:
  static std::string StartGcsServer(std::string redis_address);
  static void StopGcsServer(std::string gcs_server_socket_name);
};

/// Test cases can use it to 1) start raylet and 2) stop raylet.
class RayletManagerForTest {
 public:
  static std::string StartRaylet(std::string store_socket_name,
                                 std::string node_ip_address, int port,
                                 std::string redis_address, std::string resource);
  static void StopRaylet(std::string raylet_socket_name);

  static std::string StartRayletMonitor(std::string redis_address);
  static void StopRayletMonitor(std::string raylet_monitor_socket_name);
};

}  // namespace ray

#endif  // RAY_UTIL_TEST_UTIL_H
