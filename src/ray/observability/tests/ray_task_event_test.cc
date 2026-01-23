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

#include <gtest/gtest.h>

#include <optional>

#include "ray/common/task/task_util.h"
#include "ray/observability/ray_actor_task_definition_event.h"
#include "ray/observability/ray_event_interface.h"
#include "ray/observability/ray_task_definition_event.h"
#include "ray/observability/ray_task_lifecycle_event.h"
#include "ray/observability/ray_task_profile_event.h"

namespace ray {
namespace observability {

TaskSpecification CreateTaskSpec(const JobID &job_id,
                                 std::optional<ActorID> actor_id = std::nullopt) {
  TaskSpecBuilder builder;
  rpc::JobConfig job_config;
  auto task_id = TaskID::FromRandom(job_id);
  bool is_actor_task = actor_id.has_value();
  auto function_descriptor =
      FunctionDescriptorBuilder::BuildPython("test_module",
                                             is_actor_task ? "TestActor" : "TestClass",
                                             is_actor_task ? "test_method" : "test_func",
                                             "");
  rpc::Address caller_address;
  caller_address.set_node_id(NodeID::FromRandom().Binary());
  caller_address.set_ip_address("127.0.0.1");
  caller_address.set_port(1234);

  builder.SetCommonTaskSpec(task_id,
                            is_actor_task ? "test_actor_task" : "test_task",
                            Language::PYTHON,
                            function_descriptor,
                            job_id,
                            job_config,
                            TaskID::Nil(),
                            0,
                            TaskID::Nil(),
                            caller_address,
                            1,
                            false,
                            false,
                            -1,
                            {},
                            {},
                            "",
                            0,
                            TaskID::Nil(),
                            "");
  if (is_actor_task) {
    auto actor_creation_dummy_object_id = ObjectID::FromIndex(task_id, 1);
    builder.SetActorTaskSpec(*actor_id,
                             actor_creation_dummy_object_id,
                             /*max_retries=*/0,
                             /*retry_exceptions=*/false,
                             /*serialized_retry_exception_allowlist=*/"",
                             /*sequence_number=*/0,
                             /*tensor_transport=*/std::nullopt);
  }
  return std::move(builder).ConsumeAndBuild();
}

class TaskDefinitionEventTest : public ::testing::TestWithParam<bool> {};

TEST_P(TaskDefinitionEventTest, TestDefinitionEventCreation) {
  bool is_actor_task = GetParam();
  JobID job_id = JobID::FromInt(1);
  ActorID actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  TaskSpecification task_spec =
      is_actor_task ? CreateTaskSpec(job_id, actor_id) : CreateTaskSpec(job_id);
  TaskID task_id = task_spec.TaskId();
  int32_t attempt_number = 0;
  std::string session_name = "test_session";

  std::unique_ptr<RayEventInterface> event;
  if (is_actor_task) {
    event = std::make_unique<RayActorTaskDefinitionEvent>(
        task_spec, task_id, attempt_number, job_id, session_name);
  } else {
    event = std::make_unique<RayTaskDefinitionEvent>(
        task_spec, task_id, attempt_number, job_id, session_name);
  }

  // Verify entity ID format
  std::string expected_entity_id =
      task_id.Binary() + "_" + std::to_string(attempt_number);
  EXPECT_EQ(event->GetEntityId(), expected_entity_id);

  // Verify event type
  auto expected_event_type = is_actor_task
                                 ? rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT
                                 : rpc::events::RayEvent::TASK_DEFINITION_EVENT;
  EXPECT_EQ(event->GetEventType(), expected_event_type);

  // Serialize and verify common fields
  auto serialized = std::move(*event).Serialize();
  EXPECT_FALSE(serialized.event_id().empty());
  EXPECT_EQ(serialized.source_type(), rpc::events::RayEvent::CORE_WORKER);
  EXPECT_EQ(serialized.severity(), rpc::events::RayEvent::INFO);
  EXPECT_EQ(serialized.session_name(), session_name);

  // Verify type-specific fields
  if (is_actor_task) {
    EXPECT_TRUE(serialized.has_actor_task_definition_event());
    const auto &def_event = serialized.actor_task_definition_event();
    EXPECT_EQ(def_event.task_id(), task_id.Binary());
    EXPECT_EQ(def_event.task_attempt(), attempt_number);
    EXPECT_EQ(def_event.job_id(), job_id.Binary());
    EXPECT_EQ(def_event.actor_id(), actor_id.Binary());
    EXPECT_EQ(def_event.language(), Language::PYTHON);
    EXPECT_EQ(def_event.actor_task_name(), "test_actor_task");
  } else {
    EXPECT_TRUE(serialized.has_task_definition_event());
    const auto &def_event = serialized.task_definition_event();
    EXPECT_EQ(def_event.task_id(), task_id.Binary());
    EXPECT_EQ(def_event.task_attempt(), attempt_number);
    EXPECT_EQ(def_event.job_id(), job_id.Binary());
    EXPECT_EQ(def_event.language(), Language::PYTHON);
    EXPECT_EQ(def_event.task_name(), "test_task");
  }
}

INSTANTIATE_TEST_SUITE_P(NormalAndActorTask,
                         TaskDefinitionEventTest,
                         ::testing::Values(false, true));

TEST(RayTaskEventTest, TestTaskLifecycleEventCreation) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(1));
  int32_t attempt_number = 0;
  JobID job_id = JobID::FromInt(1);
  NodeID node_id = NodeID::FromRandom();
  rpc::TaskStatus task_status = rpc::TaskStatus::SUBMITTED_TO_WORKER;
  int64_t timestamp_ns = 1234567890;
  std::string session_name = "test_session";

  RayTaskLifecycleEvent event(
      task_id, attempt_number, job_id, node_id, task_status, timestamp_ns, session_name);

  // Verify entity ID format
  std::string expected_entity_id =
      task_id.Binary() + "_" + std::to_string(attempt_number);
  EXPECT_EQ(event.GetEntityId(), expected_entity_id);

  // Verify event type
  EXPECT_EQ(event.GetEventType(), rpc::events::RayEvent::TASK_LIFECYCLE_EVENT);

  // Serialize and verify
  auto serialized = std::move(event).Serialize();
  EXPECT_FALSE(serialized.event_id().empty());
  EXPECT_EQ(serialized.source_type(), rpc::events::RayEvent::CORE_WORKER);
  EXPECT_EQ(serialized.severity(), rpc::events::RayEvent::INFO);
  EXPECT_EQ(serialized.session_name(), session_name);

  EXPECT_TRUE(serialized.has_task_lifecycle_event());
  const auto &lifecycle_event = serialized.task_lifecycle_event();
  EXPECT_EQ(lifecycle_event.task_id(), task_id.Binary());
  EXPECT_EQ(lifecycle_event.task_attempt(), attempt_number);
  EXPECT_EQ(lifecycle_event.job_id(), job_id.Binary());
  EXPECT_EQ(lifecycle_event.node_id(), node_id.Binary());
  EXPECT_EQ(lifecycle_event.state_transitions_size(), 1);
  EXPECT_EQ(lifecycle_event.state_transitions(0).state(), task_status);
}

TEST(RayTaskEventTest, TestTaskLifecycleEventMerge) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(1));
  int32_t attempt_number = 0;
  JobID job_id = JobID::FromInt(1);
  NodeID node_id = NodeID::FromRandom();
  std::string session_name = "test_session";

  // Create first event with SUBMITTED_TO_WORKER status
  RayTaskLifecycleEvent event1(task_id,
                               attempt_number,
                               job_id,
                               node_id,
                               rpc::TaskStatus::SUBMITTED_TO_WORKER,
                               1000,
                               session_name);

  // Create second event with RUNNING status
  RayTaskLifecycleEvent event2(task_id,
                               attempt_number,
                               job_id,
                               node_id,
                               rpc::TaskStatus::RUNNING,
                               2000,
                               session_name);

  // Merge event2 into event1
  event1.Merge(std::move(event2));

  // Serialize and verify both state transitions are present
  auto serialized = std::move(event1).Serialize();
  EXPECT_TRUE(serialized.has_task_lifecycle_event());
  const auto &lifecycle_event = serialized.task_lifecycle_event();
  EXPECT_EQ(lifecycle_event.state_transitions_size(), 2);
  EXPECT_EQ(lifecycle_event.state_transitions(0).state(),
            rpc::TaskStatus::SUBMITTED_TO_WORKER);
  EXPECT_EQ(lifecycle_event.state_transitions(1).state(), rpc::TaskStatus::RUNNING);
}

TEST(RayTaskEventTest, TestTaskProfileEventCreation) {
  TaskID task_id = TaskID::FromRandom(JobID::FromInt(1));
  int32_t attempt_number = 0;
  JobID job_id = JobID::FromInt(1);
  NodeID node_id = NodeID::FromRandom();
  std::string component_type = "worker";
  std::string component_id = "worker_id_123";
  std::string node_ip_address = "127.0.0.1";
  std::string event_name = "task_execution";
  int64_t start_time_ns = 1000;
  int64_t end_time_ns = 2000;
  std::string session_name = "test_session";

  RayTaskProfileEvent event(task_id,
                            attempt_number,
                            job_id,
                            node_id,
                            component_type,
                            component_id,
                            node_ip_address,
                            event_name,
                            start_time_ns,
                            end_time_ns,
                            session_name);

  // Verify entity ID format
  std::string expected_entity_id =
      task_id.Binary() + "_" + std::to_string(attempt_number);
  EXPECT_EQ(event.GetEntityId(), expected_entity_id);

  // Verify event type
  EXPECT_EQ(event.GetEventType(), rpc::events::RayEvent::TASK_PROFILE_EVENT);

  // Serialize and verify
  auto serialized = std::move(event).Serialize();
  EXPECT_FALSE(serialized.event_id().empty());
  EXPECT_EQ(serialized.source_type(), rpc::events::RayEvent::CORE_WORKER);
  EXPECT_EQ(serialized.severity(), rpc::events::RayEvent::INFO);
  EXPECT_EQ(serialized.session_name(), session_name);

  EXPECT_TRUE(serialized.has_task_profile_events());
  const auto &profile_event = serialized.task_profile_events();
  EXPECT_EQ(profile_event.task_id(), task_id.Binary());
  EXPECT_EQ(profile_event.attempt_number(), attempt_number);
  EXPECT_EQ(profile_event.job_id(), job_id.Binary());
  EXPECT_TRUE(profile_event.has_profile_events());
  EXPECT_EQ(profile_event.profile_events().component_type(), component_type);
  EXPECT_EQ(profile_event.profile_events().events_size(), 1);
  EXPECT_EQ(profile_event.profile_events().events(0).event_name(), event_name);
  EXPECT_EQ(profile_event.profile_events().events(0).start_time(), start_time_ns);
  EXPECT_EQ(profile_event.profile_events().events(0).end_time(), end_time_ns);
}

}  // namespace observability
}  // namespace ray
