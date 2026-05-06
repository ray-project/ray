// Copyright 2026 The Ray Authors.
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

#include "ray/observability/python_event_interface.h"

#include "gtest/gtest.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"
#include "src/ray/protobuf/public/events_driver_job_definition_event.pb.h"
#include "src/ray/protobuf/public/events_driver_job_lifecycle_event.pb.h"

namespace ray {
namespace observability {

TEST(PythonRayEventTest, TestSerializeDefinitionEvent) {
  // Create a DriverJobDefinitionEvent and serialize it.
  rpc::events::DriverJobDefinitionEvent def_event;
  def_event.set_job_id("test-job-123");
  def_event.set_entrypoint("python train.py");
  def_event.mutable_config()->set_serialized_runtime_env("{}");
  std::string serialized = def_event.SerializeAsString();

  // Create PythonRayEvent with the serialized data.
  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-job-123",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/
      rpc::events::RayEvent::kDriverJobDefinitionEventFieldNumber);

  // Verify metadata.
  EXPECT_EQ(event->GetEntityId(), "test-job-123");
  EXPECT_EQ(event->GetEventType(), rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);

  // Serialize to RayEvent proto and verify nested message.
  rpc::events::RayEvent ray_event = std::move(*event).Serialize();

  EXPECT_EQ(ray_event.source_type(), rpc::events::RayEvent::GCS);
  EXPECT_EQ(ray_event.event_type(), rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  EXPECT_EQ(ray_event.severity(), rpc::events::RayEvent::INFO);
  EXPECT_EQ(ray_event.session_name(), "test-session");
  EXPECT_FALSE(ray_event.event_id().empty());
  EXPECT_TRUE(ray_event.has_timestamp());

  // Verify nested event was correctly deserialized via reflection.
  ASSERT_TRUE(ray_event.has_driver_job_definition_event());
  const auto &nested = ray_event.driver_job_definition_event();
  EXPECT_EQ(nested.job_id(), "test-job-123");
  EXPECT_EQ(nested.entrypoint(), "python train.py");
  EXPECT_EQ(nested.config().serialized_runtime_env(), "{}");
}

TEST(PythonRayEventTest, TestSerializeLifecycleEvent) {
  // Create a DriverJobLifecycleEvent and serialize it.
  rpc::events::DriverJobLifecycleEvent lifecycle_event;
  lifecycle_event.set_job_id("test-job-456");
  auto *transition = lifecycle_event.add_state_transitions();
  transition->set_state(rpc::events::DriverJobLifecycleEvent::FINISHED);
  std::string serialized = lifecycle_event.SerializeAsString();

  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::DRIVER_JOB_LIFECYCLE_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-job-456",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/
      rpc::events::RayEvent::kDriverJobLifecycleEventFieldNumber);

  rpc::events::RayEvent ray_event = std::move(*event).Serialize();

  ASSERT_TRUE(ray_event.has_driver_job_lifecycle_event());
  const auto &nested = ray_event.driver_job_lifecycle_event();
  EXPECT_EQ(nested.job_id(), "test-job-456");
  ASSERT_EQ(nested.state_transitions_size(), 1);
  EXPECT_EQ(nested.state_transitions(0).state(),
            rpc::events::DriverJobLifecycleEvent::FINISHED);
}

TEST(PythonRayEventTest, TestSerializeInvalidFieldNumber) {
  // Use an invalid field number — should log error but not crash.
  rpc::events::DriverJobDefinitionEvent def_event;
  def_event.set_job_id("test-job");
  std::string serialized = def_event.SerializeAsString();

  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-job",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/9999);  // Invalid field number

  // Should not crash — just log an error and not set the nested field.
  rpc::events::RayEvent ray_event = std::move(*event).Serialize();

  // Common fields should still be set.
  EXPECT_EQ(ray_event.source_type(), rpc::events::RayEvent::GCS);
  EXPECT_EQ(ray_event.session_name(), "test-session");

  // No nested event should be set.
  EXPECT_FALSE(ray_event.has_driver_job_definition_event());
  EXPECT_FALSE(ray_event.has_driver_job_lifecycle_event());
}

}  // namespace observability
}  // namespace ray
