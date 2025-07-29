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

#include "ray/gcs/gcs_server/gcs_one_event_converter.h"

#include "gtest/gtest.h"
#include "src/ray/protobuf/events_base_event.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class GcsOneEventConverterTest : public ::testing::Test {
 public:
  GcsOneEventConverterTest() {}
};

TEST_F(GcsOneEventConverterTest, TestConvertToTaskEventData) {
  rpc::events::AddEventRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsOneEventConverter converter;

  // Test empty request
  converter.ConvertToTaskEventData(request, task_event_data);
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 0);
  EXPECT_EQ(task_event_data.data().dropped_task_attempts_size(), 0);
}

TEST_F(GcsOneEventConverterTest, TestConvertTaskDefinitionEvent) {
  rpc::events::AddEventRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsOneEventConverter converter;

  // Create a task definition event
  auto *event = request.mutable_events_data()->add_events();
  event->set_event_id("test_event_id");
  event->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);
  event->set_source_type(rpc::events::RayEvent::CORE_WORKER);
  event->set_severity(rpc::events::RayEvent::INFO);
  event->set_message("test message");

  auto *task_def_event = event->mutable_task_definition_event();
  task_def_event->set_task_id("test_task_id");
  task_def_event->set_task_attempt(1);
  task_def_event->set_job_id("test_job_id");
  task_def_event->set_task_name("test_task_name");

  // Add some required resources
  (*task_def_event->mutable_required_resources())["CPU"] = "1.0";
  (*task_def_event->mutable_required_resources())["memory"] = "1024.0";

  // Set runtime env info
  auto *runtime_env = task_def_event->mutable_runtime_env_info();
  runtime_env->set_serialized_runtime_env("test_env");

  // Convert
  converter.ConvertToTaskEventData(request, task_event_data);

  // Verify conversion
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 1);
  const auto &converted_task = task_event_data.data().events_by_task(0);
  EXPECT_EQ(converted_task.task_id(), "test_task_id");
  EXPECT_EQ(converted_task.attempt_number(), 1);
  EXPECT_EQ(converted_task.job_id(), "test_job_id");
  EXPECT_EQ(task_event_data.data().job_id(), "test_job_id");

  // Verify task info
  EXPECT_TRUE(converted_task.has_task_info());
  const auto &task_info = converted_task.task_info();
  EXPECT_EQ(task_info.name(), "test_task_name");
  EXPECT_EQ(task_info.runtime_env_info().serialized_runtime_env(), "test_env");

  // Verify required resources
  EXPECT_EQ(task_info.required_resources().at("CPU"), 1.0);
  EXPECT_EQ(task_info.required_resources().at("memory"), 1024.0);
}

TEST_F(GcsOneEventConverterTest, TestConvertWithDroppedTaskAttempts) {
  rpc::events::AddEventRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsOneEventConverter converter;

  // Add dropped task attempts to metadata
  auto *dropped_attempt = request.mutable_events_data()
                              ->mutable_task_events_metadata()
                              ->add_dropped_task_attempts();
  dropped_attempt->set_task_id("dropped_task_id");
  dropped_attempt->set_attempt_number(2);

  // Convert
  converter.ConvertToTaskEventData(request, task_event_data);

  // Verify dropped task attempts are copied
  EXPECT_EQ(task_event_data.data().dropped_task_attempts_size(), 1);
  const auto &converted_dropped = task_event_data.data().dropped_task_attempts(0);
  EXPECT_EQ(converted_dropped.task_id(), "dropped_task_id");
  EXPECT_EQ(converted_dropped.attempt_number(), 2);
}

TEST_F(GcsOneEventConverterTest, TestInvalidResourceValues) {
  rpc::events::AddEventRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsOneEventConverter converter;

  // Create a task definition event with invalid resource values
  auto *event = request.mutable_events_data()->add_events();
  event->set_event_type(rpc::events::RayEvent::TASK_DEFINITION_EVENT);

  auto *task_def_event = event->mutable_task_definition_event();
  task_def_event->set_task_id("test_task_id");
  task_def_event->set_job_id("test_job_id");

  // Add invalid resource values
  (*task_def_event->mutable_required_resources())["CPU"] = "invalid_value";
  (*task_def_event->mutable_required_resources())["memory"] = "1024.0";  // valid

  // Convert
  converter.ConvertToTaskEventData(request, task_event_data);

  // Verify conversion succeeds and only valid resources are included
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 1);
  const auto &task_info = task_event_data.data().events_by_task(0).task_info();
  EXPECT_EQ(task_info.required_resources().size(), 1);
  EXPECT_EQ(task_info.required_resources().at("memory"), 1024.0);
  // CPU should not be present due to invalid value
  EXPECT_EQ(task_info.required_resources().find("CPU"),
            task_info.required_resources().end());
}

TEST_F(GcsOneEventConverterTest, TestConvertTaskExecutionEvent) {
  GcsOneEventConverter converter;
  rpc::events::TaskExecutionEvent exec_event;
  rpc::TaskEvents task_event;

  // Set basic fields
  exec_event.set_task_id("test_task_id");
  exec_event.set_task_attempt(3);
  exec_event.set_job_id("test_job_id");
  exec_event.set_node_id("test_node_id");
  exec_event.set_worker_id("test_worker_id");
  exec_event.set_worker_pid(1234);

  // Set a RayErrorInfo
  exec_event.mutable_ray_error_info()->set_error_message("error");

  // Set a task state: TaskStatus = 5 (SUBMITTED_TO_WORKER), timestamp = 42s 123456789ns
  google::protobuf::Timestamp ts;
  ts.set_seconds(42);
  ts.set_nanos(123456789);
  (*exec_event.mutable_task_state())[5] = ts;

  // Call the converter
  converter.ConvertTaskExecutionEventToTaskEvent(exec_event, task_event);

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

TEST_F(GcsOneEventConverterTest, TestConvertActorTaskDefinitionEvent) {
  GcsOneEventConverter converter;
  rpc::events::ActorTaskDefinitionEvent actor_def_event;
  rpc::TaskEvents task_event;

  // Set basic fields
  actor_def_event.set_task_id("test_actor_task_id");
  actor_def_event.set_task_attempt(2);
  actor_def_event.set_job_id("test_job_id");

  // Set runtime env info
  auto *runtime_env = actor_def_event.mutable_runtime_env_info();
  runtime_env->set_serialized_runtime_env("test_actor_env");

  // Set actor function descriptor (Python)
  auto *func_desc = actor_def_event.mutable_actor_func();
  auto *python_func = func_desc->mutable_python_function_descriptor();
  python_func->set_function_name("test_actor_function");
  python_func->set_class_name("TestActorClass");

  // Add required resources
  (*actor_def_event.mutable_required_resources())["CPU"] = "2.0";
  (*actor_def_event.mutable_required_resources())["GPU"] = "1.0";

  // Call the converter
  converter.ConvertActorTaskDefinitionEventToTaskEvent(actor_def_event, task_event);

  // Check basic fields
  EXPECT_EQ(task_event.task_id(), "test_actor_task_id");
  EXPECT_EQ(task_event.attempt_number(), 2);
  EXPECT_EQ(task_event.job_id(), "test_job_id");

  // Check task info
  EXPECT_TRUE(task_event.has_task_info());
  const auto &task_info = task_event.task_info();
  EXPECT_EQ(task_info.language(), rpc::Language::PYTHON);
  EXPECT_EQ(task_info.func_or_class_name(), "test_actor_function");
  EXPECT_EQ(task_info.runtime_env_info().serialized_runtime_env(), "test_actor_env");

  // Check required resources
  EXPECT_EQ(task_info.required_resources().at("CPU"), 2.0);
  EXPECT_EQ(task_info.required_resources().at("GPU"), 1.0);
}

TEST_F(GcsOneEventConverterTest,
       TestConvertActorTaskDefinitionEventWithInvalidResources) {
  GcsOneEventConverter converter;
  rpc::events::ActorTaskDefinitionEvent actor_def_event;
  rpc::TaskEvents task_event;

  // Set basic fields
  actor_def_event.set_task_id("test_actor_task_id");
  actor_def_event.set_job_id("test_job_id");

  // Set actor function descriptor (Java)
  auto *func_desc = actor_def_event.mutable_actor_func();
  auto *java_func = func_desc->mutable_java_function_descriptor();
  java_func->set_function_name("testJavaActorFunction");

  // Add mixed valid and invalid resource values
  (*actor_def_event.mutable_required_resources())["CPU"] = "invalid_value";
  (*actor_def_event.mutable_required_resources())["memory"] = "2048.0";  // valid

  // Call the converter
  converter.ConvertActorTaskDefinitionEventToTaskEvent(actor_def_event, task_event);

  // Check basic fields
  EXPECT_EQ(task_event.task_id(), "test_actor_task_id");
  EXPECT_EQ(task_event.job_id(), "test_job_id");

  // Check task info
  EXPECT_TRUE(task_event.has_task_info());
  const auto &task_info = task_event.task_info();
  EXPECT_EQ(task_info.language(), rpc::Language::JAVA);
  EXPECT_EQ(task_info.func_or_class_name(), "testJavaActorFunction");

  // Check that only valid resources are included
  EXPECT_EQ(task_info.required_resources().size(), 1);
  EXPECT_EQ(task_info.required_resources().at("memory"), 2048.0);
  // CPU should not be present due to invalid value
  EXPECT_EQ(task_info.required_resources().find("CPU"),
            task_info.required_resources().end());
}

TEST_F(GcsOneEventConverterTest, TestConvertActorTaskExecutionEvent) {
  GcsOneEventConverter converter;
  rpc::events::ActorTaskExecutionEvent actor_exec_event;
  rpc::TaskEvents task_event;

  // Set basic fields
  actor_exec_event.set_task_id("test_actor_task_id");
  actor_exec_event.set_task_attempt(4);
  actor_exec_event.set_job_id("test_job_id");
  actor_exec_event.set_node_id("test_node_id");
  actor_exec_event.set_worker_id("test_worker_id");
  actor_exec_event.set_worker_pid(5678);

  // Set a RayErrorInfo
  actor_exec_event.mutable_ray_error_info()->set_error_message("actor error");

  // Set multiple task states with different timestamps
  // TaskStatus = 5 (SUBMITTED_TO_WORKER), timestamp = 100s 500000000ns
  google::protobuf::Timestamp ts1;
  ts1.set_seconds(100);
  ts1.set_nanos(500000000);
  (*actor_exec_event.mutable_task_state())[5] = ts1;

  // TaskStatus = 6 (RUNNING), timestamp = 101s 750000000ns
  google::protobuf::Timestamp ts2;
  ts2.set_seconds(101);
  ts2.set_nanos(750000000);
  (*actor_exec_event.mutable_task_state())[6] = ts2;

  // Call the converter
  converter.ConvertActorTaskExecutionEventToTaskEvent(actor_exec_event, task_event);

  // Check basic fields
  EXPECT_EQ(task_event.task_id(), "test_actor_task_id");
  EXPECT_EQ(task_event.attempt_number(), 4);
  EXPECT_EQ(task_event.job_id(), "test_job_id");
  EXPECT_TRUE(task_event.has_state_updates());
  const auto &state_updates = task_event.state_updates();
  EXPECT_EQ(state_updates.node_id(), "test_node_id");
  EXPECT_EQ(state_updates.worker_id(), "test_worker_id");
  EXPECT_EQ(state_updates.worker_pid(), 5678);
  EXPECT_EQ(state_updates.error_info().error_message(), "actor error");

  // Check state_ts_ns - should have 2 entries
  ASSERT_EQ(state_updates.state_ts_ns().size(), 2);

  // Check first state (SUBMITTED_TO_WORKER)
  int64_t expected_ns1 = 100 * 1000000000LL + 500000000;
  EXPECT_EQ(state_updates.state_ts_ns().at(5), expected_ns1);

  // Check second state (RUNNING)
  int64_t expected_ns2 = 101 * 1000000000LL + 750000000;
  EXPECT_EQ(state_updates.state_ts_ns().at(6), expected_ns2);
}

TEST_F(GcsOneEventConverterTest, TestConvertTaskProfileEventsToTaskEvent) {
  GcsOneEventConverter converter;
  rpc::events::TaskProfileEvents task_profile_events;
  rpc::TaskEvents task_event;

  // Set basic fields
  task_profile_events.set_task_id("test_task_id");
  task_profile_events.set_attempt_number(1);
  task_profile_events.set_job_id("test_job_id");

  // Add a profile event
  rpc::events::ProfileEvents profile_events;
  ;
  profile_events.set_component_id("test_component_id");
  profile_events.set_component_type("worker");
  profile_events.set_node_ip_address("test_address");

  // add a profile event entry
  auto *ProfileEventEntry = profile_events.add_events();
  ProfileEventEntry->set_start_time(123456789);
  ProfileEventEntry->set_end_time(123456799);
  ProfileEventEntry->set_extra_data("{\"foo\": \"bar\"}");
  ProfileEventEntry->set_event_name("test_event");

  *task_profile_events.mutable_profile_events() = profile_events;

  // Call the converter
  converter.ConvertTaskProfileEventsToTaskEvent(task_profile_events, task_event);

  // Check basic fields
  EXPECT_EQ(task_event.task_id(), "test_task_id");
  EXPECT_EQ(task_event.attempt_number(), 1);
  EXPECT_EQ(task_event.job_id(), "test_job_id");
  EXPECT_EQ(task_event.profile_events().events_size(), 1);

  // Check profile event fields
  const auto &profile_event = task_event.profile_events();
  EXPECT_EQ(profile_event.component_id(), "test_component_id");
  EXPECT_EQ(profile_event.component_type(), "worker");
  EXPECT_EQ(profile_event.node_ip_address(), "test_address");

  // verify that there is one profile event entry and values match our expectations
  EXPECT_TRUE(profile_event.events().size() == 1);
  const auto &entry = profile_event.events(0);
  EXPECT_EQ(entry.start_time(), 123456789);
  EXPECT_EQ(entry.end_time(), 123456799);
  EXPECT_EQ(entry.extra_data(), "{\"foo\": \"bar\"}");
  EXPECT_EQ(entry.event_name(), "test_event");
}
}  // namespace gcs
}  // namespace ray
