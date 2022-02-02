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

#pragma once

#include <boost/optional.hpp>
#include <functional>
#include <future>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/common.pb.h"

namespace ray {

static inline std::vector<rpc::ObjectReference> ObjectIdsToRefs(
    std::vector<ObjectID> object_ids) {
  std::vector<rpc::ObjectReference> refs;
  for (const auto &object_id : object_ids) {
    rpc::ObjectReference ref;
    ref.set_object_id(object_id.Binary());
    refs.push_back(ref);
  }
  return refs;
}

class Buffer;
class RayObject;

// Magic argument to signal to mock_worker we should check message order.
static const int64_t SHOULD_CHECK_MESSAGE_ORDER = 123450000;

/// Wait until the future is ready, or timeout is reached.
///
/// \param[in] future The future to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the future is ready.
bool WaitReady(std::future<bool> future, const std::chrono::milliseconds &timeout_ms);

/// Wait until the condition is met, or timeout is reached.
///
/// \param[in] condition The condition to wait for.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the condition is met.
bool WaitForCondition(std::function<bool()> condition, int timeout_ms);

/// Wait until the expected count is met, or timeout is reached.
///
/// \param[in] current_count The current count.
/// \param[in] expected_count The expected count.
/// \param[in] timeout_ms Timeout in milliseconds to wait for for.
/// \return Whether the expected count is met.
void WaitForExpectedCount(std::atomic<int> &current_count, int expected_count,
                          int timeout_ms = 60000);

/// Used to kill process whose pid is stored in `socket_name.id` file.
void KillProcessBySocketName(std::string socket_name);

/// Kills all processes with the given executable name (similar to killall).
/// Note: On Windows, this should include the file extension (e.g. ".exe"), if any.
/// This cannot be done automatically as doing so may be incorrect in some cases.
int KillAllExecutable(const std::string &executable_with_suffix);

// A helper function to return a random task id.
TaskID RandomTaskId();

// A helper function to return a random job id.
JobID RandomJobId();

std::shared_ptr<Buffer> GenerateRandomBuffer();

std::shared_ptr<RayObject> GenerateRandomObject(
    const std::vector<ObjectID> &inlined_ids = {});

/// Path to redis server executable binary.
extern std::string TEST_REDIS_SERVER_EXEC_PATH;
/// Path to redis client executable binary.
extern std::string TEST_REDIS_CLIENT_EXEC_PATH;
/// Ports of redis server.
extern std::vector<int> TEST_REDIS_SERVER_PORTS;

/// Path to gcs server executable binary.
extern std::string TEST_GCS_SERVER_EXEC_PATH;

/// Path to raylet executable binary.
extern std::string TEST_RAYLET_EXEC_PATH;
/// Path to mock worker executable binary. Required by raylet.
extern std::string TEST_MOCK_WORKER_EXEC_PATH;

//--------------------------------------------------------------------------------
// COMPONENT MANAGEMENT CLASSES FOR TEST CASES
//--------------------------------------------------------------------------------
/// Test cases can use it to
/// 1. start/stop/flush redis server(s)
/// 2. start/stop object store
/// 3. start/stop gcs server
/// 4. start/stop raylet
/// 5. start/stop raylet monitor
class TestSetupUtil {
 public:
  static void StartUpRedisServers(const std::vector<int> &redis_server_ports);
  static void ShutDownRedisServers();
  static void FlushAllRedisServers();

  static std::string StartGcsServer(const std::string &redis_address);
  static void StopGcsServer(const std::string &gcs_server_socket_name);
  static std::string StartRaylet(const std::string &node_ip_address, const int &port,
                                 const std::string &bootstrap_address,
                                 const std::string &resource,
                                 std::string *store_socket_name);
  static void StopRaylet(const std::string &raylet_socket_name);

 private:
  static int StartUpRedisServer(const int &port);
  static void ShutDownRedisServer(const int &port);
  static void FlushRedisServer(const int &port);
};

}  // namespace ray
