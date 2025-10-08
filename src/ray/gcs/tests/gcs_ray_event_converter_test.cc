// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_ray_event_converter.h"

#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"

namespace ray {
namespace gcs {

TEST(GcsRayEventConverterTest, TestConvertToTaskEventData) {
  rpc::events::AddEventsRequest request;

  // Convert empty request
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Test empty request
  EXPECT_EQ(task_event_data_requests.size(), 0);
}

TEST(GcsRayEventConverterTest, TestConvertTaskDefinitionEvent) {
  rpc::events::AddEventsRequest request;

  // Create a task definition event
  auto *event = request.mutable_events_data()->add_events();
  event->set_event_id("test_event_id");
  event->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  event->set_source_type(rpc::events::RayEvent::CORE_WORKER);
  event->set_severity(rpc::events::RayEvent::INFO);
  event->set_message("test message");

  auto *task_def_event = event->mutable_task_definition_event();

  task_def_event->set_task_type(rpc::TaskType::NORMAL_TASK);
  task_def_event->set_language(rpc::Language::PYTHON);
  task_def_event->mutable_task_func()
      ->mutable_python_function_descriptor()
      ->set_function_name("test_task_name");
  task_def_event->set_task_id("test_task_id");
  task_def_event->set_task_attempt(1);
  task_def_event->set_job_id("test_job_id");
  task_def_event->set_task_name("test_task_name");

  task_def_event->set_parent_task_id("parent_task_id");
  task_def_event->set_placement_group_id("pg_id");

  // Add some required resources
  (*task_def_event->mutable_required_resources())["CPU"] = 1.0;
  (*task_def_event->mutable_required_resources())["memory"] = 1024.0;

  // Set runtime env info
  task_def_event->set_serialized_runtime_env("test_env");

  // Convert
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Verify conversion
  ASSERT_EQ(task_event_data_requests.size(), 1);
  const auto &task_event_data = task_event_data_requests[0];
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 1);
  const auto &converted_task = task_event_data.data().events_by_task(0);
  EXPECT_EQ(converted_task.task_id(), "test_task_id");
  EXPECT_EQ(converted_task.attempt_number(), 1);
  EXPECT_EQ(converted_task.job_id(), "test_job_id");
  EXPECT_EQ(task_event_data.data().job_id(), "test_job_id");

  // Verify task info
  ASSERT_TRUE(converted_task.has_task_info());
  const auto &task_info = converted_task.task_info();
  EXPECT_EQ(task_info.name(), "test_task_name");
  EXPECT_EQ(task_info.type(), rpc::TaskType::NORMAL_TASK);
  EXPECT_EQ(task_info.language(), rpc::Language::PYTHON);
  EXPECT_EQ(task_info.func_or_class_name(), "test_task_name");
  EXPECT_EQ(task_info.runtime_env_info().serialized_runtime_env(), "test_env");
  EXPECT_EQ(task_info.parent_task_id(), "parent_task_id");
  EXPECT_EQ(task_info.placement_group_id(), "pg_id");

  // Verify required resources
  EXPECT_EQ(task_info.required_resources().at("CPU"), 1.0);
  EXPECT_EQ(task_info.required_resources().at("memory"), 1024.0);
}

TEST(GcsRayEventConverterTest, TestConvertWithDroppedTaskAttempts) {
  rpc::events::AddEventsRequest request;

  // Create a proper TaskID for testing
  const auto job_id = JobID::FromInt(100);
  const auto driver_task_id = TaskID::ForDriverTask(job_id);
  const auto test_task_id = TaskID::ForNormalTask(job_id, driver_task_id, 1);
  const auto task_id_binary = test_task_id.Binary();

  // Add dropped task attempts to metadata
  auto *dropped_attempt = request.mutable_events_data()
                              ->mutable_task_events_metadata()
                              ->add_dropped_task_attempts();
  dropped_attempt->set_task_id(task_id_binary);
  dropped_attempt->set_attempt_number(2);

  // Convert
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Verify dropped task attempts are copied
  ASSERT_FALSE(task_event_data_requests.empty());
  EXPECT_EQ(task_event_data_requests[0].data().dropped_task_attempts_size(), 1);
  const auto &converted_dropped =
      task_event_data_requests[0].data().dropped_task_attempts(0);
  EXPECT_EQ(converted_dropped.task_id(), task_id_binary);
  EXPECT_EQ(converted_dropped.attempt_number(), 2);
}

TEST(GcsRayEventConverterTest, TestMultipleJobIds) {
  rpc::events::AddEventsRequest request;

  // Create events with different job IDs
  const auto job_id_1 = JobID::FromInt(100);
  const auto job_id_2 = JobID::FromInt(200);

  // Create first task event
  auto *event1 = request.mutable_events_data()->add_events();
  event1->set_event_id("test_event_1");
  event1->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  auto *task_def_event1 = event1->mutable_task_definition_event();
  task_def_event1->set_task_type(rpc::TaskType::NORMAL_TASK);
  task_def_event1->set_language(rpc::Language::PYTHON);
  task_def_event1->set_task_id("task_1");
  task_def_event1->set_job_id(job_id_1.Binary());
  task_def_event1->set_task_name("task_1_name");

  // Create second task event with different job ID
  auto *event2 = request.mutable_events_data()->add_events();
  event2->set_event_id("test_event_2");
  event2->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  auto *task_def_event2 = event2->mutable_task_definition_event();
  task_def_event2->set_task_type(rpc::TaskType::NORMAL_TASK);
  task_def_event2->set_language(rpc::Language::PYTHON);
  task_def_event2->set_task_id("task_2");
  task_def_event2->set_job_id(job_id_2.Binary());
  task_def_event2->set_task_name("task_2_name");

  // Add dropped task attempts for both job IDs
  const auto driver_task_id_1 = TaskID::ForDriverTask(job_id_1);
  const auto test_task_id_1 = TaskID::ForNormalTask(job_id_1, driver_task_id_1, 1);

  const auto driver_task_id_2 = TaskID::ForDriverTask(job_id_2);
  const auto test_task_id_2 = TaskID::ForNormalTask(job_id_2, driver_task_id_2, 1);

  // Add dropped task attempt for job_id_1
  auto *dropped_attempt_1 = request.mutable_events_data()
                                ->mutable_task_events_metadata()
                                ->add_dropped_task_attempts();
  dropped_attempt_1->set_task_id(test_task_id_1.Binary());
  dropped_attempt_1->set_attempt_number(3);

  // Add dropped task attempt for job_id_2
  auto *dropped_attempt_2 = request.mutable_events_data()
                                ->mutable_task_events_metadata()
                                ->add_dropped_task_attempts();
  dropped_attempt_2->set_task_id(test_task_id_2.Binary());
  dropped_attempt_2->set_attempt_number(4);

  // Convert
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Verify that we get two separate requests (one for each job ID)
  ASSERT_EQ(task_event_data_requests.size(), 2);

  // Check that each request has the correct job ID and dropped task attempts
  bool found_job_1 = false, found_job_2 = false;
  for (const auto &req : task_event_data_requests) {
    if (req.data().job_id() == job_id_1.Binary()) {
      found_job_1 = true;
      EXPECT_EQ(req.data().events_by_task_size(), 1);
      EXPECT_EQ(req.data().events_by_task(0).job_id(), job_id_1.Binary());

      // Verify dropped task attempt for job_id_1
      EXPECT_EQ(req.data().dropped_task_attempts_size(), 1);
      const auto &dropped = req.data().dropped_task_attempts(0);
      EXPECT_EQ(dropped.task_id(), test_task_id_1.Binary());
      EXPECT_EQ(dropped.attempt_number(), 3);
    } else if (req.data().job_id() == job_id_2.Binary()) {
      found_job_2 = true;
      EXPECT_EQ(req.data().events_by_task_size(), 1);
      EXPECT_EQ(req.data().events_by_task(0).job_id(), job_id_2.Binary());

      // Verify dropped task attempt for job_id_2
      EXPECT_EQ(req.data().dropped_task_attempts_size(), 1);
      const auto &dropped = req.data().dropped_task_attempts(0);
      EXPECT_EQ(dropped.task_id(), test_task_id_2.Binary());
      EXPECT_EQ(dropped.attempt_number(), 4);
    }
  }
  EXPECT_TRUE(found_job_1);
  EXPECT_TRUE(found_job_2);
}

TEST(GcsRayEventConverterTest, TestSameJobIdGrouping) {
  rpc::events::AddEventsRequest request;

  // Create multiple events with the same job ID
  const auto job_id = JobID::FromInt(100);

  // Create first task event
  auto *event1 = request.mutable_events_data()->add_events();
  event1->set_event_id("test_event_1");
  event1->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  auto *task_def_event1 = event1->mutable_task_definition_event();
  task_def_event1->set_task_type(rpc::TaskType::NORMAL_TASK);
  task_def_event1->set_language(rpc::Language::PYTHON);
  task_def_event1->set_task_id("task_1");
  task_def_event1->set_job_id(job_id.Binary());
  task_def_event1->set_task_name("task_1_name");

  // Create second task event with same job ID
  auto *event2 = request.mutable_events_data()->add_events();
  event2->set_event_id("test_event_2");
  event2->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  auto *task_def_event2 = event2->mutable_task_definition_event();
  task_def_event2->set_task_type(rpc::TaskType::NORMAL_TASK);
  task_def_event2->set_language(rpc::Language::PYTHON);
  task_def_event2->set_task_id("task_2");
  task_def_event2->set_job_id(job_id.Binary());
  task_def_event2->set_task_name("task_2_name");

  // Convert
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Verify that we get one request with both events grouped together
  ASSERT_EQ(task_event_data_requests.size(), 1);
  EXPECT_EQ(task_event_data_requests[0].data().job_id(), job_id.Binary());
  EXPECT_EQ(task_event_data_requests[0].data().events_by_task_size(), 2);

  // Verify both tasks are present
  const auto &events = task_event_data_requests[0].data().events_by_task();
  EXPECT_EQ(events[0].job_id(), job_id.Binary());
  EXPECT_EQ(events[1].job_id(), job_id.Binary());
}

TEST(GcsRayEventConverterTest, TestConvertTaskProfileEvents) {
  rpc::events::AddEventsRequest request;

  // Create a task profile event
  auto *event = request.mutable_events_data()->add_events();
  event->set_event_id("test_event_id");
  event->set_event_type(rpc::events::RayEvent::TASK_PROFILE_EVENT);
  event->set_source_type(rpc::events::RayEvent::CORE_WORKER);
  event->set_severity(rpc::events::RayEvent::INFO);
  event->set_message("test message");

  auto *task_profile_events = event->mutable_task_profile_events();
  task_profile_events->set_task_id("test_task_id");
  task_profile_events->set_attempt_number(1);
  task_profile_events->set_job_id("test_job_id");

  // Add a profile event
  auto *profile_events = task_profile_events->mutable_profile_events();
  profile_events->set_component_id("test_component_id");
  profile_events->set_component_type("worker");
  profile_events->set_node_ip_address("test_address");

  // add a profile event entry
  auto *ProfileEventEntry = profile_events->add_events();
  ProfileEventEntry->set_start_time(123456789);
  ProfileEventEntry->set_end_time(123456799);
  ProfileEventEntry->set_extra_data(R"({"foo": "bar"})");
  ProfileEventEntry->set_event_name("test_event");

  // Convert
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));

  // Verify conversion
  EXPECT_EQ(task_event_data_requests.size(), 1);
  auto &task_event_data = task_event_data_requests[0];
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 1);
  const auto &converted_task = task_event_data.data().events_by_task(0);

  EXPECT_EQ(converted_task.task_id(), "test_task_id");
  EXPECT_EQ(converted_task.attempt_number(), 1);
  EXPECT_EQ(converted_task.job_id(), "test_job_id");
  EXPECT_EQ(converted_task.profile_events().events_size(), 1);
  EXPECT_EQ(task_event_data.data().job_id(), "test_job_id");

  // Check profile event fields
  EXPECT_TRUE(converted_task.has_profile_events());
  const auto &profile_event = converted_task.profile_events();
  EXPECT_EQ(profile_event.component_id(), "test_component_id");
  EXPECT_EQ(profile_event.component_type(), "worker");
  EXPECT_EQ(profile_event.node_ip_address(), "test_address");

  // verify that there is one profile event entry and values match our expectations
  EXPECT_TRUE(profile_event.events().size() == 1);
  const auto &entry = profile_event.events(0);
  EXPECT_EQ(entry.start_time(), 123456789);
  EXPECT_EQ(entry.end_time(), 123456799);
  EXPECT_EQ(entry.extra_data(), R"({"foo": "bar"})");
  EXPECT_EQ(entry.event_name(), "test_event");
}

TEST(GcsRayEventConverterTest, TestConvertTaskExecutionEvent) {
  rpc::events::AddEventsRequest request;
  rpc::events::RayEvent &event = *request.mutable_events_data()->mutable_events()->Add();
  event.set_event_type(rpc::events::RayEvent::TASK_EXECUTION_EVENT);
  rpc::events::TaskExecutionEvent &exec_event = *event.mutable_task_execution_event();

  // Set basic fields
  exec_event.set_task_id("test_task_id");
  exec_event.set_task_attempt(3);
  exec_event.set_job_id("test_job_id");
  exec_event.set_node_id("test_node_id");
  exec_event.set_worker_id("test_worker_id");
  exec_event.set_worker_pid(1234);

  // Set a RayErrorInfo
  exec_event.mutable_ray_error_info()->set_error_message("error");

  google::protobuf::Timestamp ts;
  ts.set_seconds(42);
  ts.set_nanos(123456789);
  (*exec_event.mutable_task_state())[rpc::TaskStatus::SUBMITTED_TO_WORKER] = ts;

  // Call the converter
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));
  rpc::TaskEvents task_event = task_event_data_requests[0].data().events_by_task()[0];

  // Check basic fields
  EXPECT_EQ(task_event.attempt_number(), 3);
  EXPECT_EQ(task_event.job_id(), "test_job_id");
  EXPECT_TRUE(task_event.has_state_updates());
  const auto &state_updates = task_event.state_updates();
  EXPECT_EQ(state_updates.node_id(), "test_node_id");
  EXPECT_EQ(state_updates.worker_id(), "test_worker_id");
  EXPECT_EQ(state_updates.worker_pid(), 1234);
  EXPECT_EQ(state_updates.error_info().error_message(), "error");

  // Check state_ts_ns
  ASSERT_EQ(state_updates.state_ts_ns().size(), 1);
  int64_t expected_ns = 42 * 1000000000LL + 123456789;
  EXPECT_EQ(state_updates.state_ts_ns().at(5), expected_ns);
}

TEST(GcsRayEventConverterTest, TestConvertActorTaskDefinitionEvent) {
  rpc::events::AddEventsRequest request;
  rpc::events::RayEvent &event = *request.mutable_events_data()->mutable_events()->Add();
  event.set_event_type(rpc::events::RayEvent::ACTOR_TASK_DEFINITION_EVENT);
  rpc::events::ActorTaskDefinitionEvent &actor_def_event =
      *event.mutable_actor_task_definition_event();

  // Set basic fields
  actor_def_event.set_task_id("test_actor_task_id");
  actor_def_event.set_task_attempt(2);
  actor_def_event.set_job_id("test_job_id");
  actor_def_event.set_actor_task_name("test_actor_task");
  actor_def_event.set_language(rpc::Language::PYTHON);
  actor_def_event.set_actor_id("actor-123");
  actor_def_event.set_parent_task_id("parent-actor-task");
  actor_def_event.set_placement_group_id("pg-actor");

  // Set runtime env info
  actor_def_event.set_serialized_runtime_env("test_actor_env");

  // Set actor function descriptor (Python)
  auto *func_desc = actor_def_event.mutable_actor_func();
  auto *python_func = func_desc->mutable_python_function_descriptor();
  python_func->set_function_name("test_actor_function");
  python_func->set_class_name("TestActorClass");

  // Add required resources
  (*actor_def_event.mutable_required_resources())["CPU"] = 2.0;
  (*actor_def_event.mutable_required_resources())["GPU"] = 1.0;

  // Call the converter
  auto task_event_data_requests = ConvertToTaskEventDataRequests(std::move(request));
  rpc::TaskEvents task_event = task_event_data_requests[0].data().events_by_task()[0];

  // Check basic fields
  EXPECT_EQ(task_event.task_id(), "test_actor_task_id");
  EXPECT_EQ(task_event.attempt_number(), 2);
  EXPECT_EQ(task_event.job_id(), "test_job_id");

  // Check task info
  EXPECT_TRUE(task_event.has_task_info());
  const auto &task_info = task_event.task_info();
  EXPECT_EQ(task_info.type(), rpc::TaskType::ACTOR_TASK);
  EXPECT_EQ(task_info.name(), "test_actor_task");
  EXPECT_EQ(task_info.language(), rpc::Language::PYTHON);
  EXPECT_EQ(task_info.func_or_class_name(), "test_actor_function");
  EXPECT_EQ(task_info.runtime_env_info().serialized_runtime_env(), "test_actor_env");
  EXPECT_EQ(task_info.actor_id(), "actor-123");
  EXPECT_EQ(task_info.parent_task_id(), "parent-actor-task");
  EXPECT_EQ(task_info.placement_group_id(), "pg-actor");

  // Check required resources
  EXPECT_EQ(task_info.required_resources().at("CPU"), 2.0);
  EXPECT_EQ(task_info.required_resources().at("GPU"), 1.0);
}

}  // namespace gcs
}  // namespace ray
