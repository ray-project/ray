// Copyright 2021 The Ray Authors.
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

#include <memory>
// clang-format off
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
// clang-format on
using namespace ::testing;  // NOLINT
using namespace ray::gcs;   // NOLINT
using namespace ray;        // NOLINT

class GcsFunctionManagerTest : public Test {
 public:
  void SetUp() override {
    kv = std::make_unique<MockInternalKVInterface>();
    function_manager = std::make_unique<GcsFunctionManager>(*kv, io_context);
  }
  std::unique_ptr<GcsFunctionManager> function_manager;
  std::unique_ptr<MockInternalKVInterface> kv;
  instrumented_io_context io_context;
};

TEST_F(GcsFunctionManagerTest, TestFunctionManagerGC) {
  JobID job_id = BaseID<JobID>::FromRandom();
  int num_del_called = 0;
  auto f = [&num_del_called]() mutable { ++num_del_called; };
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("RemoteFunction:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("ActorClass:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("FunctionsToRun:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  function_manager->AddJobReference(job_id);
  EXPECT_EQ(0, num_del_called);
  function_manager->AddJobReference(job_id);
  EXPECT_EQ(0, num_del_called);
  function_manager->AddJobReference(job_id);
  EXPECT_EQ(0, num_del_called);
  function_manager->RemoveJobReference(job_id);
  EXPECT_EQ(0, num_del_called);
  function_manager->RemoveJobReference(job_id);
  EXPECT_EQ(0, num_del_called);
  function_manager->RemoveJobReference(job_id);
  EXPECT_EQ(3, num_del_called);
}

TEST_F(GcsFunctionManagerTest, TestDuplicateRemoveJobReference) {
  JobID job_id = BaseID<JobID>::FromRandom();
  int num_del_called = 0;
  auto f = [&num_del_called]() mutable { ++num_del_called; };
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("RemoteFunction:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("ActorClass:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("FunctionsToRun:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));

  // Add a job reference (counter becomes 1)
  function_manager->AddJobReference(job_id);
  EXPECT_EQ(0, num_del_called);

  // First RemoveJobReference call - should succeed and trigger cleanup
  function_manager->RemoveJobReference(job_id);
  EXPECT_EQ(3, num_del_called);  // All 3 Del operations should be called

  // Second RemoveJobReference call for the same job - this should crash without fix
  // With the fix, this should handle gracefully and not crash
  EXPECT_NO_THROW(function_manager->RemoveJobReference(job_id));

  // KV operations should not be called again
  EXPECT_EQ(3, num_del_called);
}

TEST_F(GcsFunctionManagerTest, TestNetworkRetryScenario) {
  JobID job_id = JobID::FromBinary("03000000");
  int num_del_called = 0;
  auto f = [&num_del_called]() mutable { ++num_del_called; };
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("RemoteFunction:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("ActorClass:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("FunctionsToRun:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));

  // 1. Job gets added (simulate job creation)
  function_manager->AddJobReference(job_id);

  // 2. First MarkJobFinished call succeeds on GCS side
  function_manager->RemoveJobReference(job_id);
  EXPECT_EQ(3, num_del_called);

  // 3. Network failure causes raylet to retry MarkJobFinished
  // This second call should NOT crash the GCS
  EXPECT_NO_THROW({ function_manager->RemoveJobReference(job_id); });

  // 4. KV operations should not be called again (idempotent)
  EXPECT_EQ(3, num_del_called);

  // 5. Additional retries should also be handled gracefully
  EXPECT_NO_THROW({
    function_manager->RemoveJobReference(job_id);
    function_manager->RemoveJobReference(job_id);
  });

  EXPECT_EQ(3, num_del_called);  // Still only called once
}
