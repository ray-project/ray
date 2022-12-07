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

#include <google/protobuf/util/message_differencer.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"

using ::testing::_;
using ::testing::Return;

namespace ray {

namespace core {

namespace worker {

class TaskEventBufferTest : public ::testing::Test {
 public:
  TaskEventBufferTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_task_events_in_buffer": 100
}
  )");

    task_event_buffer_ = std::make_unique<TaskEventBufferImpl>(
        std::make_unique<ray::gcs::MockGcsClient>());
  }

  virtual void SetUp() { RAY_CHECK_OK(task_event_buffer_->Start(/*auto_flush*/ false)); }

  virtual void TearDown() { task_event_buffer_->Stop(); };

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  rpc::TaskEvents GenTaskEvents(TaskID task_id, uint64_t attempt_num) {
    rpc::TaskEvents task_events;
    task_events.set_task_id(task_id.Binary());
    task_events.set_attempt_number(attempt_num);
    return task_events;
  }

  std::unique_ptr<TaskEventBufferImpl> task_event_buffer_ = nullptr;
};

class TaskEventBufferTestManualStart : public TaskEventBufferTest {
  void SetUp() override {}
};

TEST_F(TaskEventBufferTestManualStart, TestGcsClientFail) {
  ASSERT_NE(task_event_buffer_, nullptr);

  // Mock GCS connect fail.
  auto gcs_client =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient());
  EXPECT_CALL(*gcs_client, Connect)
      .Times(1)
      .WillOnce(Return(Status::UnknownError("error")));

  // Expect no flushing even if auto flush is on since start fails.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(0);

  ASSERT_TRUE(task_event_buffer_->Start(/*auto_flush*/ true).IsUnknownError());
  ASSERT_FALSE(task_event_buffer_->Enabled());
}

TEST_F(TaskEventBufferTest, TestAddEvent) {
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), 0);

  // Test add status event
  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_1, 0));

  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), 1);

  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_1, 1));
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), 2);
}

TEST_F(TaskEventBufferTest, TestFlushEvents) {
  size_t num_events = 20;
  auto task_ids = GenTaskIDs(num_events);

  for (const auto &task_id : task_ids) {
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), num_events);

  // Manually call flush should call GCS client's flushing grpc.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  // Expect data flushed match
  rpc::TaskEventData expected_data;
  expected_data.set_num_task_events_dropped(0);
  for (const auto &task_id : task_ids) {
    auto event = expected_data.add_events_by_task();
    event->set_task_id(task_id.Binary());
    event->set_attempt_number(0);
  }

  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
      .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                    ray::gcs::StatusCallback callback) {
        if (google::protobuf::util::MessageDifferencer::Equals(*actual_data,
                                                               expected_data)) {
          return Status::OK();
        }
        return Status::UnknownError("");
      });

  task_event_buffer_->FlushEvents(false);

  // Expect no more events.
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), 0);
}

TEST_F(TaskEventBufferTest, TestFailedFlush) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  // Mock gRPC sent failure.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData)
      .Times(2)
      .WillOnce(Return(Status::GrpcUnknown("grpc error")))
      .WillOnce(Return(Status::OK()));

  // Flush
  task_event_buffer_->FlushEvents(false);

  // Expect the number of dropped events incremented.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsDropped(), num_events);

  // Adding some more events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  // Flush successfully will reset the num events dropped.
  task_event_buffer_->FlushEvents(false);
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsDropped(), 0);
}

TEST_F(TaskEventBufferTest, TestBackPressure) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;
  // Multiple flush calls should only result in 1 grpc call if not forced flush.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(1);

  task_event_buffer_->FlushEvents(false);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_2, 0));
  task_event_buffer_->FlushEvents(false);
}

TEST_F(TaskEventBufferTest, TestForcedFlush) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  // Multiple flush calls with forced should result in same number of grpc call.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(2);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id_2, 0));
  task_event_buffer_->FlushEvents(true);
}

TEST_F(TaskEventBufferTest, TestBufferSizeLimit) {
  size_t num_limit = 100;  // Synced with test setup
  size_t num_batch1 = 100;
  size_t num_batch2 = 100;

  auto task_ids1 = GenTaskIDs(num_batch1);
  auto task_ids2 = GenTaskIDs(num_batch2);

  // Adding them
  for (auto &task_id : task_ids1) {
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  for (auto &task_id : task_ids2) {
    task_event_buffer_->AddTaskEvent(GenTaskEvents(task_id, 0));
  }

  // Expect only limit.
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), num_limit);

  // Expect task events match.
  std::unordered_set<TaskID> task_ids2_set(task_ids2.begin(), task_ids2.end());
  for (auto &task_event : task_event_buffer_->GetAllTaskEvents()) {
    auto task_id = TaskID::FromBinary(task_event.task_id());
    EXPECT_EQ(task_ids2_set.count(task_id), 1);
  }
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsDropped(), num_batch1);

  // Expect the reported data to contain number of dropped events.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  rpc::TaskEventData expected_data;
  expected_data.set_num_task_events_dropped(num_batch1);
  for (const auto &task_id : task_ids2) {
    auto event = expected_data.add_events_by_task();
    event->set_task_id(task_id.Binary());
    event->set_attempt_number(0);
  }

  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
      .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                    ray::gcs::StatusCallback callback) {
        if (google::protobuf::util::MessageDifferencer::Equals(*actual_data,
                                                               expected_data)) {
          return Status::OK();
        }
        return Status::UnknownError("");
      });

  task_event_buffer_->FlushEvents(false);

  // Expect data flushed.
  ASSERT_EQ(task_event_buffer_->GetAllTaskEvents().size(), 0);
}

}  // namespace worker

}  // namespace core

}  // namespace ray
