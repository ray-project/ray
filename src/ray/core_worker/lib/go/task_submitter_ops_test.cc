// Copyright 2025 The Ray Authors.
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

// Unit tests for task_submitter_ops.cc - Business logic layer for task submission.
// These tests use MockCoreWorkerProvider to test the business logic without
// requiring a full Ray infrastructure setup.

#include "task_submitter_ops.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <memory>
#include <string>
#include <vector>

#include "core_worker_provider.h"
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/core_worker.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace go {

// ============================================================================
// Mock CoreWorker for Testing
// ============================================================================

/**
 * @brief Mock CoreWorker implementation for unit testing.
 *
 * This mock allows us to test TaskSubmitterOperations without requiring
 * a real Ray infrastructure. We use Google Mock to set up expectations.
 *
 * NOTE: We don't inherit from CoreWorker because its constructor requires
 * many complex dependencies. Instead, we use weak symbols to override
 * the methods we need to mock.
 */

// Mock implementations using weak symbols to override CoreWorker methods
// These are defined in the global namespace and linked via weak symbols

__attribute__((weak)) std::vector<ray::rpc::ObjectReference> ray_core_worker_SubmitTask(
    const ray::core::RayFunction& func,
    const std::vector<std::unique_ptr<ray::TaskArg>>& args,
    const ray::core::TaskOptions& options,
    int num_returns) {
  // Default mock implementation - return empty vector
  return std::vector<ray::rpc::ObjectReference>();
}

__attribute__((weak)) ray::Status ray_core_worker_CreateActor(
    const ray::core::RayFunction& func,
    const std::vector<std::unique_ptr<ray::TaskArg>>& args,
    const ray::core::ActorCreationOptions& options,
    const std::string& actor_name,
    const std::string& serialized_runtime_env,
    ray::ActorID* actor_id) {
  // Default mock implementation - return OK status and set a dummy actor ID
  if (actor_id) {
    // Use a fixed dummy actor ID for testing
    *actor_id = ray::ActorID::FromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");
  }
  return ray::Status::OK();
}

__attribute__((weak)) ray::Status ray_core_worker_SubmitActorTask(
    const ray::ActorID& actor_id,
    const ray::core::RayFunction& func,
    const std::vector<std::unique_ptr<ray::TaskArg>>& args,
    const ray::core::TaskOptions& options,
    int num_returns,
    bool is_async,
    const std::string& serialized_runtime_env,
    const std::string& debug_event_string,
    std::vector<ray::rpc::ObjectReference>& return_refs) {
  // Default mock implementation - return OK status and empty return_refs
  return_refs.clear();
  return ray::Status::OK();
}

// ============================================================================
// Test Fixtures
// ============================================================================

class TaskSubmitterOpsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create mock provider
    mock_provider_ = std::make_shared<MockCoreWorkerProvider>();

    // Set the mock provider in TaskSubmitterOperations
    TaskSubmitterOperations::SetCoreWorkerProvider(mock_provider_);
  }

  void TearDown() override {
    // Reset to default provider
    TaskSubmitterOperations::SetCoreWorkerProvider(nullptr);
  }

  std::shared_ptr<MockCoreWorkerProvider> mock_provider_;
};

// ============================================================================
// Helper Functions
// ============================================================================

namespace {

// No helper functions needed for the current tests

}  // namespace

// ============================================================================
// SubmitTask Tests
// ============================================================================
// NOTE: These tests are commented out because they require a CoreWorker instance,
// which has complex dependencies. The current mocking strategy using weak symbols
// doesn't work because CoreWorker methods are not virtual and weak symbols don't
// override class methods.
//
// TODO: Revisit when a better mocking strategy is available, such as:
// 1. Making CoreWorker methods virtual and creating a mock subclass
// 2. Injecting an interface for CoreWorker operations
// 3. Creating a minimal CoreWorker with mocked dependencies (see core_worker_test.cc)

// All SubmitTask, CreateActor, and SubmitActorTask tests are commented out.
// See individual test sections below for details.

// ============================================================================
// ParseResources Tests
// ============================================================================
// SubmitTask Tests
// ============================================================================
// NOTE: These tests are commented out because they require a CoreWorker instance,
// which has complex dependencies. The current mocking strategy using weak symbols
// doesn't work because CoreWorker methods are not virtual and weak symbols don't
// override class methods.
//
// TODO: Revisit when a better mocking strategy is available, such as:
// 1. Making CoreWorker methods virtual and creating a mock subclass
// 2. Injecting an interface for CoreWorker operations
// 3. Creating a minimal CoreWorker with mocked dependencies (see core_worker_test.cc)

// TEST_F(TaskSubmitterOpsTest, SubmitTaskSuccess) {
//   // Setup: Create function descriptor and arguments
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//   args.push_back(CreateValueArg("arg2"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.max_retries = 2;
//   options.num_returns = 2;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify: Mock returns empty vector
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithEmptyArgs) {
//   // Setup: Empty function descriptor
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;  // Empty
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify: Should not crash, returns empty vector from mock
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithPlacementGroup) {
//   // Setup: Placement group options
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.placement_group_id_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
//   options.bundle_index = 0;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithResources) {
//   // Setup: Resource requirements
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.resources["CPU"] = 2.0;
//   options.resources["GPU"] = 1.0;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithRuntimeEnv) {
//   // Setup: Runtime environment
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.serialized_runtime_env_info = "{\"pip\": [\"requests\"]}";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }

// NOTE: The following SubmitTask tests are commented out because they require
// a CoreWorker instance. See the NOTE at the beginning of the SubmitTask section.

// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithEmptyArgs) {
//   // Setup: Empty function descriptor
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;  // Empty
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify: Should not crash, returns empty vector from mock
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithPlacementGroup) {
//   // Setup: Placement group options
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.placement_group_id_hex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
//   options.bundle_index = 0;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithResources) {
//   // Setup: Resource requirements
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.resources["CPU"] = 2.0;
//   options.resources["GPU"] = 1.0;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitTaskWithRuntimeEnv) {
//   // Setup: Runtime environment
//   std::vector<std::string> func_desc = {"module", "function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("arg1"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.serialized_runtime_env_info = "{\"pip\": [\"requests\"]}";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }

// ============================================================================
// CreateActor Tests
// ============================================================================
// NOTE: These tests are commented out for the same reasons as SubmitTask tests.
// See the NOTE above for details.

// TEST_F(TaskSubmitterOpsTest, CreateActorSuccess) {
//   // Setup: Actor creation
//   std::vector<std::string> func_desc = {"module", "ActorClass"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("init_arg"));
//
//   ActorCreateOptions options = CreateDefaultActorOptions();
//   options.name = "TestActor";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().CreateActor(func_desc, args, options);
//
//   // Verify: Should return a valid ActorID (mock returns random ID)
//   EXPECT_FALSE(result.IsNil());
// }
//
// TEST_F(TaskSubmitterOpsTest, CreateActorWithEmptyArgs) {
//   // Setup: Empty constructor args
//   std::vector<std::string> func_desc = {"module", "ActorClass"};
//   std::vector<std::unique_ptr<TaskArgument>> args;  // Empty
//
//   ActorCreateOptions options = CreateDefaultActorOptions();
//   options.name = "TestActor";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().CreateActor(func_desc, args, options);
//
//   // Verify
//   EXPECT_FALSE(result.IsNil());
// }
//
// TEST_F(TaskSubmitterOpsTest, CreateActorWithRuntimeEnv) {
//   // Setup: Actor with runtime environment
//   std::vector<std::string> func_desc = {"module", "ActorClass"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("init_arg"));
//
//   ActorCreateOptions options = CreateDefaultActorOptions();
//   options.serialized_runtime_env_info = "{\"pip\": [\"numpy\"]}";
//   options.name = "TestActor";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().CreateActor(func_desc, args, options);
//
//   // Verify
//   EXPECT_FALSE(result.IsNil());
// }

// ============================================================================
// SubmitActorTask Tests
// ============================================================================
// NOTE: These tests are commented out for the same reasons as SubmitTask tests.
// See the NOTE above for details.

// TEST_F(TaskSubmitterOpsTest, SubmitActorTaskSuccess) {
//   // Setup: Actor task submission
//   ActorID actor_id = ActorID::FromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");
//   std::vector<std::string> func_desc = {"module", "method"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("method_arg"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.num_returns = 1;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitActorTask(actor_id, func_desc, args, options);
//
//   // Verify: Mock returns empty vector
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, SubmitActorTaskWithEmptyArgs) {
//   // Setup: Actor task with no args
//   ActorID actor_id = ActorID::FromHex("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20");
//   std::vector<std::string> func_desc = {"module", "method"};
//   std::vector<std::unique_ptr<TaskArgument>> args;  // Empty
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitActorTask(actor_id, func_desc, args, options);
//
//   // Verify
//   EXPECT_TRUE(result.empty());
// }

// ============================================================================
// ParseResources Tests
// ============================================================================

TEST_F(TaskSubmitterOpsTest, ParseResourcesEmptyString) {
  // Setup: Empty resource string
  std::string resources = "";

  // Execute
  auto result = TaskSubmitterOperations::ParseResources(resources);

  // Verify
  EXPECT_TRUE(result.empty());
}

TEST_F(TaskSubmitterOpsTest, ParseResourcesValidJSON) {
  // Setup: Valid resource string (comma-separated format, not JSON)
  std::string resources = "CPU:2.0,GPU:1.0,memory:1073741824.0";

  // Execute
  auto result = TaskSubmitterOperations::ParseResources(resources);

  // Verify
  EXPECT_EQ(result.size(), 3);
  EXPECT_NEAR(result["CPU"], 2.0, 0.001);
  EXPECT_NEAR(result["GPU"], 1.0, 0.001);
  EXPECT_NEAR(result["memory"], 1073741824.0, 0.001);
}

TEST_F(TaskSubmitterOpsTest, ParseResourcesInvalidJSON) {
  // Setup: Invalid resource string (missing colon)
  std::string resources = "CPU 2.0";

  // Execute - should parse but skip invalid entries
  auto result = TaskSubmitterOperations::ParseResources(resources);

  // Verify - empty result since no valid entries
  EXPECT_TRUE(result.empty());
}

// ============================================================================
// HexToBinary Tests
// ============================================================================

TEST_F(TaskSubmitterOpsTest, HexToBinaryEmptyString) {
  // Setup: Empty hex string
  std::string hex = "";

  // Execute
  auto result = TaskSubmitterOperations::HexToBinary(hex);

  // Verify
  EXPECT_TRUE(result.empty());
}

TEST_F(TaskSubmitterOpsTest, HexToBinaryValidHex) {
  // Setup: Valid hex string (4 bytes)
  std::string hex = "01020304";

  // Execute
  auto result = TaskSubmitterOperations::HexToBinary(hex);

  // Verify
  ASSERT_EQ(result.size(), 4);
  EXPECT_EQ(result[0], 0x01);
  EXPECT_EQ(result[1], 0x02);
  EXPECT_EQ(result[2], 0x03);
  EXPECT_EQ(result[3], 0x04);
}

TEST_F(TaskSubmitterOpsTest, HexToBinaryInvalidHex) {
  // Setup: Invalid hex string (odd length)
  // The implementation now validates input length and throws for invalid hex strings
  std::string hex = "abc";

  // Execute & Verify - should throw std::invalid_argument for odd-length hex string
  EXPECT_THROW(TaskSubmitterOperations::HexToBinary(hex), std::invalid_argument);
}

TEST_F(TaskSubmitterOpsTest, HexToBinaryInvalidCharacters) {
  // Setup: Invalid hex characters
  // NOTE: The current implementation uses std::stoi with base 16, which will
  // throw std::invalid_argument for invalid characters like 'g' and 'h'.
  std::string hex = "01gh";

  // Execute & Verify: Should throw exception from std::stoi
  EXPECT_THROW(TaskSubmitterOperations::HexToBinary(hex), std::exception);
}

// ============================================================================
// Integration Tests
// ============================================================================
// NOTE: These tests are commented out for the same reasons as SubmitTask tests.
// See the NOTE above for details.

// TEST_F(TaskSubmitterOpsTest, FullTaskSubmissionFlow) {
//   // Setup: Complete task submission scenario
//   std::vector<std::string> func_desc = {"my_module", "my_function"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("string_arg"));
//   args.push_back(CreateValueArg("another_arg"));
//
//   TaskSubmitOptions options = CreateDefaultOptions();
//   options.max_retries = 3;
//   options.resources["CPU"] = 1.0;
//   options.serialized_runtime_env_info = "{}";
//   options.num_returns = 2;
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().SubmitTask(func_desc, args, options);
//
//   // Verify: Mock returns empty vector, but flow should complete without error
//   EXPECT_TRUE(result.empty());
// }
//
// TEST_F(TaskSubmitterOpsTest, FullActorCreationFlow) {
//   // Setup: Complete actor creation scenario
//   std::vector<std::string> func_desc = {"my_module", "MyActor"};
//   std::vector<std::unique_ptr<TaskArgument>> args;
//   args.push_back(CreateValueArg("constructor_arg1"));
//   args.push_back(CreateValueArg("constructor_arg2"));
//
//   ActorCreateOptions options = CreateDefaultActorOptions();
//   options.max_restarts = 5;
//   options.max_task_retries = 3;
//   options.resources["CPU"] = 2.0;
//   options.name = "MyTestActor";
//
//   // Execute
//   auto result = TaskSubmitterOperations::GetInstance().CreateActor(func_desc, args, options);
//
//   // Verify
//   EXPECT_FALSE(result.IsNil());
// }

}  // namespace go
}  // namespace ray
