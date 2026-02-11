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
#include "src/ray/protobuf/public/events_submission_job_definition_event.pb.h"
#include "src/ray/protobuf/public/events_submission_job_lifecycle_event.pb.h"

namespace ray {
namespace observability {

// Field numbers from the RayEvent proto for submission job events.
constexpr int kSubmissionJobDefinitionFieldNumber = 19;
constexpr int kSubmissionJobLifecycleFieldNumber = 20;

TEST(PythonRayEventTest, TestSerializeDefinitionEvent) {
  // Create a SubmissionJobDefinitionEvent and serialize it.
  rpc::events::SubmissionJobDefinitionEvent def_event;
  def_event.set_submission_id("test-submission-123");
  def_event.set_entrypoint("python train.py");
  def_event.mutable_config()->set_serialized_runtime_env("{}");
  std::string serialized = def_event.SerializeAsString();

  // Create PythonRayEvent with the serialized data.
  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-submission-123",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/kSubmissionJobDefinitionFieldNumber);

  // Verify metadata.
  EXPECT_EQ(event->GetEntityId(), "test-submission-123");
  EXPECT_EQ(event->GetEventType(),
            rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT);

  // Serialize to RayEvent proto and verify nested message.
  rpc::events::RayEvent ray_event = std::move(*event).Serialize();

  EXPECT_EQ(ray_event.source_type(), rpc::events::RayEvent::GCS);
  EXPECT_EQ(ray_event.event_type(),
            rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT);
  EXPECT_EQ(ray_event.severity(), rpc::events::RayEvent::INFO);
  EXPECT_EQ(ray_event.session_name(), "test-session");
  EXPECT_FALSE(ray_event.event_id().empty());
  EXPECT_TRUE(ray_event.has_timestamp());

  // Verify nested event was correctly deserialized via reflection.
  ASSERT_TRUE(ray_event.has_submission_job_definition_event());
  const auto &nested = ray_event.submission_job_definition_event();
  EXPECT_EQ(nested.submission_id(), "test-submission-123");
  EXPECT_EQ(nested.entrypoint(), "python train.py");
  EXPECT_EQ(nested.config().serialized_runtime_env(), "{}");
}

TEST(PythonRayEventTest, TestSerializeLifecycleEvent) {
  // Create a SubmissionJobLifecycleEvent and serialize it.
  rpc::events::SubmissionJobLifecycleEvent lifecycle_event;
  lifecycle_event.set_submission_id("test-submission-456");
  auto *transition = lifecycle_event.add_state_transitions();
  transition->set_state(rpc::events::SubmissionJobLifecycleEvent::RUNNING);
  transition->set_message("Job started running");
  std::string serialized = lifecycle_event.SerializeAsString();

  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::SUBMISSION_JOB_LIFECYCLE_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-submission-456",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/kSubmissionJobLifecycleFieldNumber);

  rpc::events::RayEvent ray_event = std::move(*event).Serialize();

  ASSERT_TRUE(ray_event.has_submission_job_lifecycle_event());
  const auto &nested = ray_event.submission_job_lifecycle_event();
  EXPECT_EQ(nested.submission_id(), "test-submission-456");
  ASSERT_EQ(nested.state_transitions_size(), 1);
  EXPECT_EQ(nested.state_transitions(0).state(),
            rpc::events::SubmissionJobLifecycleEvent::RUNNING);
  EXPECT_EQ(nested.state_transitions(0).message(), "Job started running");
}

TEST(PythonRayEventTest, TestSerializeInvalidFieldNumber) {
  // Use an invalid field number — should log error but not crash.
  rpc::events::SubmissionJobDefinitionEvent def_event;
  def_event.set_submission_id("test-submission");
  std::string serialized = def_event.SerializeAsString();

  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test-submission",
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
  EXPECT_FALSE(ray_event.has_submission_job_definition_event());
  EXPECT_FALSE(ray_event.has_submission_job_lifecycle_event());
}

TEST(PythonRayEventTest, TestSupportsMerge) {
  rpc::events::SubmissionJobDefinitionEvent def_event;
  std::string serialized = def_event.SerializeAsString();

  auto event = CreatePythonRayEvent(
      /*source_type=*/static_cast<int>(rpc::events::RayEvent::GCS),
      /*event_type=*/
      static_cast<int>(rpc::events::RayEvent::SUBMISSION_JOB_DEFINITION_EVENT),
      /*severity=*/static_cast<int>(rpc::events::RayEvent::INFO),
      /*entity_id=*/"test",
      /*message=*/"",
      /*session_name=*/"test-session",
      /*serialized_event_data=*/serialized,
      /*nested_event_field_number=*/kSubmissionJobDefinitionFieldNumber);

  // PythonRayEvent should not support merge.
  EXPECT_FALSE(event->SupportsMerge());
}

}  // namespace observability
}  // namespace ray
