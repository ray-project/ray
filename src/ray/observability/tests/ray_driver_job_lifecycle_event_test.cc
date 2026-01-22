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

#include "ray/observability/ray_driver_job_lifecycle_event.h"

#include "gtest/gtest.h"

namespace ray {
namespace observability {

class RayDriverJobLifecycleEventTest : public ::testing::Test {};

TEST_F(RayDriverJobLifecycleEventTest, TestMerge) {
  rpc::JobTableData data;
  data.set_job_id("test_job_id_1");
  auto event1 = std::make_unique<RayDriverJobLifecycleEvent>(
      data, rpc::events::DriverJobLifecycleEvent::CREATED, "test_session_name_1");
  auto event2 = std::make_unique<RayDriverJobLifecycleEvent>(
      data, rpc::events::DriverJobLifecycleEvent::FINISHED, "test_session_name_1");
  event1->Merge(std::move(*event2));
  auto serialized_event = std::move(*event1).Serialize();
  ASSERT_EQ(serialized_event.driver_job_lifecycle_event().state_transitions_size(), 2);
  ASSERT_EQ(serialized_event.driver_job_lifecycle_event().state_transitions(0).state(),
            rpc::events::DriverJobLifecycleEvent::CREATED);
  ASSERT_EQ(serialized_event.driver_job_lifecycle_event().state_transitions(1).state(),
            rpc::events::DriverJobLifecycleEvent::FINISHED);
}

TEST_F(RayDriverJobLifecycleEventTest, TestSerialize) {
  rpc::JobTableData data;
  data.set_job_id("test_job_id_with_info");

  auto *job_info = data.mutable_job_info();
  job_info->set_message("Job failed due to runtime error");
  job_info->set_error_type("USER_SCRIPT_ERROR");
  job_info->set_driver_exit_code(1);

  auto event = std::make_unique<RayDriverJobLifecycleEvent>(
      data, rpc::events::DriverJobLifecycleEvent::FINISHED, "test_session");
  auto serialized_event = std::move(*event).Serialize();

  ASSERT_EQ(serialized_event.driver_job_lifecycle_event().state_transitions_size(), 1);

  const auto &transition =
      serialized_event.driver_job_lifecycle_event().state_transitions(0);
  ASSERT_EQ(transition.state(), rpc::events::DriverJobLifecycleEvent::FINISHED);
  ASSERT_TRUE(transition.has_timestamp());

  ASSERT_EQ(transition.message(), "Job failed due to runtime error");
  ASSERT_EQ(transition.error_type(), "USER_SCRIPT_ERROR");
  ASSERT_EQ(transition.exit_code(), 1);
}

}  // namespace observability
}  // namespace ray
