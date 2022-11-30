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

#include "ray/core_worker/task_event_buffer.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"

namespace ray {

namespace core {

namespace worker {

class TaskEventBufferTest : public ::testing::Test {
 public:
  TaskEventBufferTest()
      : task_event_buffer_(std::make_unique<TaskEventBufferImpl>(
            std::make_unique<ray::gcs::MockGcsClient>(),
            /*manual_flush*/ true)) {}

  virtual void TearDown() { task_event_buffer_->Stop(); };

  std::unique_ptr<TaskEventBufferImpl> task_event_buffer_;
};

TEST_F(TaskEventBufferTest, TestAddEvent) {
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().events_by_task_size(), 0);

  // Test add status event
  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskStatusEvent(
      task_id_1, rpc::TaskStatus::RUNNING, nullptr, nullptr);

  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().events_by_task_size(), 1);

  task_event_buffer_->AddProfileEvent(
      task_id_1, rpc::ProfileEventEntry(), "dummy_type", "dummy_id", "dummy_ip");
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().events_by_task_size(), 2);
}

TEST_F(TaskEventBufferTest, TestFlushEvents) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskStatusEvent(
        task_id, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  }

  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().events_by_task_size(), num_events);

  // Manually call flush should call GCS client's flushing grpc.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->gcs_client_.get())
          ->mock_task_accessor;
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(1);

  task_event_buffer_->FlushEvents(false);

  // Expect no more events.
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().events_by_task_size(), 0);
}

TEST_F(TaskEventBufferTest, TestBackPressure) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskStatusEvent(
        task_id, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->gcs_client_.get())
          ->mock_task_accessor;
  // Multiple flush calls should only result in 1 grpc call if not forced flush.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(1);

  task_event_buffer_->FlushEvents(false);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskStatusEvent(
      task_id_1, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskStatusEvent(
      task_id_2, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  task_event_buffer_->FlushEvents(false);
}

TEST_F(TaskEventBufferTest, TestForcedFlush) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskStatusEvent(
        task_id, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->gcs_client_.get())
          ->mock_task_accessor;

  // Multiple flush calls with forced should result in same number of grpc call.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(2);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskStatusEvent(
      task_id_1, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskStatusEvent(
      task_id_2, rpc::TaskStatus::RUNNING, nullptr, nullptr);
  task_event_buffer_->FlushEvents(true);
}

}  // namespace worker

}  // namespace core

}  // namespace ray
