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
/// Port of redis server.
extern int REDIS_SERVER_PORT;

/// Test helper class, it will start redis server before the test runs,
/// and stop redis server after the test is completed.
class RedisServiceManagerForTest : public ::testing::Test {
 public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void FlushAll();
};

}  // namespace ray

#endif  // RAY_UTIL_TEST_UTIL_H
