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

#include <functional>
#include <future>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/asio_util.h"
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
void WaitForExpectedCount(std::atomic<int> &current_count,
                          int expected_count,
                          int timeout_ms = 60000);

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

//--------------------------------------------------------------------------------
// COMPONENT MANAGEMENT CLASSES FOR TEST CASES
//--------------------------------------------------------------------------------
/// Test cases can use it to start/stop/flush redis server(s).
class TestSetupUtil {
 public:
  static void StartUpRedisServers(const std::vector<int> &redis_server_ports,
                                  bool save = false);
  static void ShutDownRedisServers();
  static void FlushAllRedisServers();

  static void ExecuteRedisCmd(int port, std::vector<std::string> cmd);
  static int StartUpRedisServer(int port, bool save = false);
  static void ShutDownRedisServer(int port);
  static void FlushRedisServer(int port);
};

template <size_t k, typename T>
struct SaveArgToUniquePtrAction {
  std::unique_ptr<T> *pointer;

  template <typename... Args>
  void operator()(const Args &...args) const {
    *pointer = std::make_unique<T>(std::get<k>(std::tie(args...)));
  }
};

// Copies the k-th arg with make_unique(arg<k>) into ptr.
template <size_t k, typename T>
SaveArgToUniquePtrAction<k, T> SaveArgToUniquePtr(std::unique_ptr<T> *ptr) {
  return {ptr};
}

}  // namespace ray
