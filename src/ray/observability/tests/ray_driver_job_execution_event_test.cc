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

#include "ray/observability/ray_driver_job_execution_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayDriverJobExecutionEventTest : public ::testing::Test {};

TEST_F(RayDriverJobExecutionEventTest, TestMerge) {
  rpc::JobTableData data;
  data.set_job_id("test_job_id_1");
  auto event1 = std::make_unique<RayDriverJobExecutionEvent>(
      data, rpc::events::DriverJobExecutionEvent::SUCCESS, "test_session_name_1");
  auto event2 = std::make_unique<RayDriverJobExecutionEvent>(
      data, rpc::events::DriverJobExecutionEvent::FAILURE, "test_session_name_1");
  event1->Merge(std::move(*event2));
  auto serialized_event = std::move(*event1).Serialize();
  ASSERT_EQ(serialized_event.driver_job_execution_event().states_size(), 2);
  ASSERT_EQ(serialized_event.driver_job_execution_event().states(0).state(),
            rpc::events::DriverJobExecutionEvent::SUCCESS);
  ASSERT_EQ(serialized_event.driver_job_execution_event().states(1).state(),
            rpc::events::DriverJobExecutionEvent::FAILURE);
}

}  // namespace observability
}  // namespace ray
