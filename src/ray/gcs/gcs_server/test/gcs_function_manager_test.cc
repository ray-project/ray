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

// clang-format off
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "ray/gcs/gcs_server/gcs_function_manager.h"
#include "ray/gcs/gcs_server/gcs_kv_manager.h"
#include "mock/ray/gcs/gcs_server/gcs_kv_manager.h"
// clang-format on
using namespace ::testing;
using namespace ray::gcs;
using namespace ray;

class GcsFunctionManagerTest : public Test {
 public:
  void SetUp() override {
    kv = std::make_unique<MockInternalKVInterface>();
    function_manager = std::make_unique<GcsFunctionManager>(*kv);
  }
  std::unique_ptr<GcsFunctionManager> function_manager;
  std::unique_ptr<MockInternalKVInterface> kv;
};

TEST_F(GcsFunctionManagerTest, TestFunctionManagerGC) {
  JobID job_id = BaseID<JobID>::FromRandom();
  int num_del_called = 0;
  auto f = [&num_del_called]() mutable { ++num_del_called; };
  EXPECT_CALL(*kv, Del(StrEq("fun"), StartsWith("IsolatedExports:"), true, _))
      .WillOnce(InvokeWithoutArgs(f));
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
  EXPECT_EQ(4, num_del_called);
}
