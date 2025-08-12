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

#include "ray/gcs/gcs_server/gcs_ray_event_converter.h"

#include "gtest/gtest.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/events_base_event.pb.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

class GcsRayEventConverterTest : public ::testing::Test {
 public:
  GcsRayEventConverterTest() {}
};

TEST_F(GcsRayEventConverterTest, TestConvertToTaskEventData) {
  rpc::events::AddEventsRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsRayEventConverter converter;

  // Test empty request
  converter.ConvertToTaskEventDataRequest(std::move(request), task_event_data);
  EXPECT_EQ(task_event_data.data().events_by_task_size(), 0);
  EXPECT_EQ(task_event_data.data().dropped_task_attempts_size(), 0);
}

TEST_F(GcsRayEventConverterTest, TestConvertTaskDefinitionEvent) {
  rpc::events::AddEventsRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsRayEventConverter converter;

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
  auto *runtime_env = task_def_event->mutable_runtime_env_info();
  runtime_env->set_serialized_runtime_env("test_env");

  // Convert
  converter.ConvertToTaskEventDataRequest(std::move(request), task_event_data);

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

TEST_F(GcsRayEventConverterTest, TestConvertWithDroppedTaskAttempts) {
  rpc::events::AddEventsRequest request;
  rpc::AddTaskEventDataRequest task_event_data;
  GcsRayEventConverter converter;

  // Add dropped task attempts to metadata
  auto *dropped_attempt = request.mutable_events_data()
                              ->mutable_task_events_metadata()
                              ->add_dropped_task_attempts();
  dropped_attempt->set_task_id("dropped_task_id");
  dropped_attempt->set_attempt_number(2);

  // Convert
  converter.ConvertToTaskEventDataRequest(std::move(request), task_event_data);

  // Verify dropped task attempts are copied
  EXPECT_EQ(task_event_data.data().dropped_task_attempts_size(), 1);
  const auto &converted_dropped = task_event_data.data().dropped_task_attempts(0);
  EXPECT_EQ(converted_dropped.task_id(), "dropped_task_id");
  EXPECT_EQ(converted_dropped.attempt_number(), 2);
}

TEST_F(GcsRayEventConverterTest, TestConvertTaskExecutionEvent) {
  GcsRayEventConverter converter;
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
  converter.ConvertToTaskEvents(std::move(exec_event), task_event);

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

}  // namespace gcs
}  // namespace ray
