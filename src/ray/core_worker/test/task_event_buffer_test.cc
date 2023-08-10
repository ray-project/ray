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
  "task_events_max_buffer_size": 100,
  "task_events_send_batch_size": 100
}
  )");

    task_event_buffer_ = std::make_unique<TaskEventBufferImpl>(
        std::make_unique<ray::gcs::MockGcsClient>());
  }

  virtual void SetUp() { RAY_CHECK_OK(task_event_buffer_->Start(/*auto_flush*/ false)); }

  virtual void TearDown() {
    if (task_event_buffer_) task_event_buffer_->Stop();
  };

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  std::unique_ptr<TaskEvent> GenStatusTaskEvent(TaskID task_id,
                                                int32_t attempt_num,
                                                int64_t running_ts = 1) {
    return std::make_unique<TaskStatusEvent>(
        task_id, JobID::FromInt(0), attempt_num, rpc::TaskStatus::RUNNING, running_ts);
  }

  std::unique_ptr<TaskEvent> GenProfileTaskEvent(TaskID task_id, int32_t attempt_num) {
    return std::make_unique<TaskProfileEvent>(
        task_id, JobID::FromInt(0), attempt_num, "", "", "", "test_event", 1);
  }

  static void CompareTaskEventData(const rpc::TaskEventData &actual_data,
                                   const rpc::TaskEventData &expect_data) {
    // Sort and compare
    std::vector<std::string> actual_events;
    std::vector<std::string> expect_events;
    for (const auto &e : actual_data.events_by_task()) {
      actual_events.push_back(e.DebugString());
    }
    for (const auto &e : expect_data.events_by_task()) {
      expect_events.push_back(e.DebugString());
    }
    std::sort(actual_events.begin(), actual_events.end());
    std::sort(expect_events.begin(), expect_events.end());
    EXPECT_EQ(actual_events.size(), expect_events.size());
    for (size_t i = 0; i < actual_events.size(); ++i) {
      EXPECT_EQ(actual_events[i], expect_events[i]);
    }

    EXPECT_EQ(actual_data.num_profile_task_events_dropped(),
              expect_data.num_profile_task_events_dropped());
    EXPECT_EQ(actual_data.num_status_task_events_dropped(),
              expect_data.num_status_task_events_dropped());
  }

  std::unique_ptr<TaskEventBufferImpl> task_event_buffer_ = nullptr;
};

class TaskEventBufferTestManualStart : public TaskEventBufferTest {
  void SetUp() override {}
};

class TaskEventBufferTestBatchSend : public TaskEventBufferTest {
 public:
  TaskEventBufferTestBatchSend() : TaskEventBufferTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_buffer_size": 100,
  "task_events_send_batch_size": 10
}
  )");
  }
};

class TaskEventBufferTestLimitProfileEvents : public TaskEventBufferTest {
 public:
  TaskEventBufferTestLimitProfileEvents() : TaskEventBufferTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_profile_events_for_task": 10
}
  )");
  }
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
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);

  // Test add status event
  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));

  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 1);

  // Test add profile events
  task_event_buffer_->AddTaskEvent(GenProfileTaskEvent(task_id_1, 1));
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 2);
}

TEST_F(TaskEventBufferTest, TestFlushEvents) {
  size_t num_events = 20;
  auto task_ids = GenTaskIDs(num_events);

  std::vector<std::unique_ptr<TaskEvent>> task_events;
  for (const auto &task_id : task_ids) {
    task_events.push_back(GenStatusTaskEvent(task_id, 0));
  }

  // Expect data flushed match
  rpc::TaskEventData expected_data;
  expected_data.set_num_profile_task_events_dropped(0);
  expected_data.set_num_status_task_events_dropped(0);
  for (const auto &task_event : task_events) {
    auto event = expected_data.add_events_by_task();
    task_event->ToRpcTaskEventsOrDrop(event);
  }

  for (auto &task_event : task_events) {
    task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), num_events);

  // Manually call flush should call GCS client's flushing grpc.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
      .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                    ray::gcs::StatusCallback callback) {
        CompareTaskEventData(*actual_data, expected_data);
        return Status::OK();
      });

  task_event_buffer_->FlushEvents(false);

  // Expect no more events.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
}

TEST_F(TaskEventBufferTest, TestFailedFlush) {
  size_t num_status_events = 20;
  size_t num_profile_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_status_events + num_profile_events; ++i) {
    auto task_id = RandomTaskId();
    if (i % 2 == 0) {
      task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 0));
    } else {
      task_event_buffer_->AddTaskEvent(GenProfileTaskEvent(task_id, 0));
    }
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
  ASSERT_EQ(task_event_buffer_->GetTotalNumStatusTaskEventsDropped(), num_status_events);
  ASSERT_EQ(task_event_buffer_->GetTotalNumProfileTaskEventsDropped(),
            num_profile_events);

  // Adding some more events
  for (size_t i = 0; i < num_status_events + num_profile_events; ++i) {
    auto task_id = RandomTaskId();
    if (i % 2 == 0) {
      task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 1));
    } else {
      task_event_buffer_->AddTaskEvent(GenProfileTaskEvent(task_id, 1));
    }
  }

  // Flush successfully will reset the num events dropped.
  task_event_buffer_->FlushEvents(false);
  ASSERT_EQ(task_event_buffer_->GetTotalNumStatusTaskEventsDropped(), num_status_events);
  ASSERT_EQ(task_event_buffer_->GetTotalNumProfileTaskEventsDropped(),
            num_profile_events);
}

TEST_F(TaskEventBufferTest, TestBackPressure) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;
  // Multiple flush calls should only result in 1 grpc call if not forced flush.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(1);

  task_event_buffer_->FlushEvents(false);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_2, 0));
  task_event_buffer_->FlushEvents(false);
}

TEST_F(TaskEventBufferTest, TestForcedFlush) {
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  // Multiple flush calls with forced should result in same number of grpc call.
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(2);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_2, 0));
  task_event_buffer_->FlushEvents(true);
}

TEST_F(TaskEventBufferTestBatchSend, TestBatchedSend) {
  size_t num_events = 100;
  size_t batch_size = 10;  // Sync with constructor.
  std::vector<TaskID> task_ids;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_ids.push_back(task_id);
    task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 0));
  }

  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  // With batch size = 10, there should be 10 flush calls
  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData)
      .Times(num_events / batch_size)
      .WillRepeatedly([&batch_size](std::unique_ptr<rpc::TaskEventData> actual_data,
                                    ray::gcs::StatusCallback callback) {
        EXPECT_EQ(actual_data->events_by_task_size(), batch_size);
        callback(Status::OK());
        return Status::OK();
      });

  for (int i = 0; i * batch_size < num_events; i++) {
    task_event_buffer_->FlushEvents(true);
    EXPECT_EQ(task_event_buffer_->GetNumTaskEventsStored(),
              num_events - (i + 1) * batch_size);
  }

  // With last flush, there should be no more events in the buffer and as data.
  EXPECT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
}

TEST_F(TaskEventBufferTest, TestBufferSizeLimit) {
  size_t num_limit = 100;  // Synced with test setup
  size_t num_profile = 50;
  size_t num_status = 50;

  // Generate 2 batches of events each, where batch 1 will be evicted by batch 2.
  std::vector<std::unique_ptr<TaskEvent>> profile_events_1;
  std::vector<std::unique_ptr<TaskEvent>> status_events_1;
  std::vector<std::unique_ptr<TaskEvent>> profile_events_2;
  std::vector<std::unique_ptr<TaskEvent>> status_events_2;

  // Generate data
  for (size_t i = 0; i < 50; ++i) {
    status_events_1.push_back(GenStatusTaskEvent(RandomTaskId(), 0));
    status_events_2.push_back(GenStatusTaskEvent(RandomTaskId(), 0));
    profile_events_1.push_back(GenProfileTaskEvent(RandomTaskId(), 0));
    profile_events_2.push_back(GenProfileTaskEvent(RandomTaskId(), 0));
  }

  rpc::TaskEventData expected_data;
  expected_data.set_num_profile_task_events_dropped(num_profile);
  expected_data.set_num_status_task_events_dropped(num_status);
  for (const auto &event_ptr : profile_events_2) {
    auto expect_event = expected_data.add_events_by_task();
    // Copy the data
    auto event = std::make_unique<TaskProfileEvent>(
        *static_cast<TaskProfileEvent *>(event_ptr.get()));
    event->ToRpcTaskEventsOrDrop(expect_event);
  }
  for (const auto &event_ptr : status_events_2) {
    auto expect_event = expected_data.add_events_by_task();
    // Copy the data
    auto event = std::make_unique<TaskStatusEvent>(
        *static_cast<TaskStatusEvent *>(event_ptr.get()));
    event->ToRpcTaskEventsOrDrop(expect_event);
  }

  // Add the data
  for (auto &event : profile_events_1) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  for (auto &event : status_events_1) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  for (auto &event : profile_events_2) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  for (auto &event : status_events_2) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  // Expect only limit in buffer.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), num_limit);

  // Expect the reported data to match.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
      .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                    ray::gcs::StatusCallback callback) {
        // Sort and compare
        CompareTaskEventData(*actual_data, expected_data);
        return Status::OK();
      });

  task_event_buffer_->FlushEvents(false);

  // Expect data flushed.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
  ASSERT_EQ(task_event_buffer_->GetNumProfileTaskEventsDroppedSinceLastFlush(), 0);
  ASSERT_EQ(task_event_buffer_->GetNumStatusTaskEventsDroppedSinceLastFlush(), 0);
  ASSERT_EQ(task_event_buffer_->GetTotalNumProfileTaskEventsDropped(), num_profile);
  ASSERT_EQ(task_event_buffer_->GetTotalNumStatusTaskEventsDropped(), num_status);
}

TEST_F(TaskEventBufferTestLimitProfileEvents, TestLimitProfileEventsPerTask) {
  size_t num_profile_events_per_task = 10;
  size_t num_total_profile_events = 1000;
  std::vector<std::unique_ptr<TaskEvent>> profile_events;
  auto task_id = RandomTaskId();

  // Generate data for the same task attempts.
  for (size_t i = 0; i < num_total_profile_events; ++i) {
    profile_events.push_back(GenProfileTaskEvent(task_id, 0));
  }

  // Add all
  for (auto &event : profile_events) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }

  // Assert dropped count
  task_event_buffer_->FlushEvents(false);
  ASSERT_EQ(task_event_buffer_->GetTotalNumProfileTaskEventsDropped(),
            num_total_profile_events - num_profile_events_per_task);
  ASSERT_EQ(task_event_buffer_->GetTotalNumStatusTaskEventsDropped(), 0);
}

TEST_F(TaskEventBufferTest, TestGracefulDestruction) {
  delete task_event_buffer_.release();
}

}  // namespace worker

}  // namespace core

}  // namespace ray
