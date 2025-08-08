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

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/task/task_util.h"
#include "ray/common/test_util.h"
#include "ray/util/event.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::MakeAction;
using ::testing::Return;

namespace ray {

namespace core {

namespace worker {

class MockEventAggregatorClient : public ray::rpc::EventAggregatorClient {
 public:
  MOCK_METHOD(void,
              AddEvents,
              (const rpc::events::AddEventsRequest &request,
               const rpc::ClientCallback<rpc::events::AddEventsReply> &callback),
              (override));
};

class MockEventAggregatorAddEvents
    : public ::testing::ActionInterface<void(
          const rpc::events::AddEventsRequest &request,
          const rpc::ClientCallback<rpc::events::AddEventsReply> &callback)> {
 public:
  MockEventAggregatorAddEvents(Status status, rpc::events::AddEventsReply reply)
      : status_(std::move(status)), reply_(std::move(reply)) {}

  void Perform(const std::tuple<const rpc::events::AddEventsRequest &,
                                const rpc::ClientCallback<rpc::events::AddEventsReply> &>
                   &args) override {
    std::get<1>(args)(status_, std::move(reply_));
  }

 private:
  Status status_;
  rpc::events::AddEventsReply reply_;
};

class TaskEventBufferTest : public ::testing::Test {
 public:
  TaskEventBufferTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_status_events_buffer_on_worker": 100,
  "task_events_send_batch_size": 100
}
  )");

    task_event_buffer_ = std::make_unique<TaskEventBufferImpl>(
        std::make_unique<ray::gcs::MockGcsClient>(),
        std::make_unique<MockEventAggregatorClient>());
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

  TaskSpecification BuildTaskSpec(TaskID task_id, int32_t attempt_num) {
    TaskSpecBuilder builder;
    rpc::Address empty_address;
    rpc::JobConfig config;
    std::unordered_map<std::string, double> resources = {{"CPU", 1}};
    std::unordered_map<std::string, std::string> labels = {{"label1", "value1"}};
    builder.SetCommonTaskSpec(task_id,
                              "dummy_task",
                              Language::PYTHON,
                              FunctionDescriptorBuilder::BuildPython(
                                  "dummy_module", "dummy_class", "dummy_function", ""),
                              JobID::Nil(),
                              config,
                              TaskID::Nil(),
                              0,
                              TaskID::Nil(),
                              empty_address,
                              1,
                              false,
                              false,
                              -1,
                              resources,
                              resources,
                              "",
                              0,
                              TaskID::Nil(),
                              "",
                              std::make_shared<rpc::RuntimeEnvInfo>(),
                              "",
                              true,
                              labels);
    return std::move(builder).ConsumeAndBuild();
  }

  std::unique_ptr<TaskEvent> GenFullStatusTaskEvent(TaskID task_id, int32_t attempt_num) {
    // Generate a task spec
    auto task_spec = BuildTaskSpec(task_id, attempt_num);

    // Generate a status update
    auto status_update = TaskStatusEvent::TaskStateUpdate(123u);

    return std::make_unique<TaskStatusEvent>(
        task_id,
        JobID::FromInt(0),
        attempt_num,
        rpc::TaskStatus::RUNNING,
        1,
        /*is_actor_task_event=*/false,
        std::make_shared<TaskSpecification>(task_spec),
        status_update);
  }

  std::unique_ptr<TaskEvent> GenStatusTaskEvent(
      TaskID task_id,
      int32_t attempt_num,
      int64_t running_ts = 1,
      std::optional<const TaskStatusEvent::TaskStateUpdate> state_update =
          absl::nullopt) {
    return std::make_unique<TaskStatusEvent>(task_id,
                                             JobID::FromInt(0),
                                             attempt_num,
                                             rpc::TaskStatus::RUNNING,
                                             running_ts,
                                             /*is_actor_task_event=*/false,
                                             nullptr,
                                             state_update);
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

    EXPECT_EQ(actual_data.num_profile_events_dropped(),
              expect_data.num_profile_events_dropped());

    std::vector<std::string> actual_dropped_task_attempts;
    std::vector<std::string> expect_dropped_task_attempts;

    for (const auto &t : actual_data.dropped_task_attempts()) {
      actual_dropped_task_attempts.push_back(t.DebugString());
    }
    for (const auto &t : expect_data.dropped_task_attempts()) {
      expect_dropped_task_attempts.push_back(t.DebugString());
    }

    std::sort(actual_dropped_task_attempts.begin(), actual_dropped_task_attempts.end());
    std::sort(expect_dropped_task_attempts.begin(), expect_dropped_task_attempts.end());
    EXPECT_EQ(actual_dropped_task_attempts.size(), expect_dropped_task_attempts.size());
    for (size_t i = 0; i < actual_dropped_task_attempts.size(); ++i) {
      EXPECT_EQ(actual_dropped_task_attempts[i], expect_dropped_task_attempts[i]);
    }
  }

  static void CompareRayEventsData(const rpc::events::RayEventsData &actual_data,
                                   const rpc::events::RayEventsData &expect_data) {
    // Sort and compare
    std::vector<std::string> actual_events;
    std::vector<std::string> expect_events;
    for (const auto &e : actual_data.events()) {
      auto event_copy = e;
      event_copy.set_event_id(UniqueID::Nil().Binary());
      actual_events.push_back(event_copy.DebugString());
    }
    for (const auto &e : expect_data.events()) {
      auto event_copy = e;
      event_copy.set_event_id(UniqueID::Nil().Binary());
      expect_events.push_back(event_copy.DebugString());
    }
    std::sort(actual_events.begin(), actual_events.end());
    std::sort(expect_events.begin(), expect_events.end());
    EXPECT_EQ(actual_events.size(), expect_events.size());
    for (size_t i = 0; i < actual_events.size(); ++i) {
      EXPECT_EQ(actual_events[i], expect_events[i]);
    }

    std::vector<std::string> actual_dropped_task_attempts;
    std::vector<std::string> expect_dropped_task_attempts;

    for (const auto &t : actual_data.task_events_metadata().dropped_task_attempts()) {
      actual_dropped_task_attempts.push_back(t.DebugString());
    }
    for (const auto &t : expect_data.task_events_metadata().dropped_task_attempts()) {
      expect_dropped_task_attempts.push_back(t.DebugString());
    }
    std::sort(actual_dropped_task_attempts.begin(), actual_dropped_task_attempts.end());
    std::sort(expect_dropped_task_attempts.begin(), expect_dropped_task_attempts.end());
    EXPECT_EQ(actual_dropped_task_attempts.size(), expect_dropped_task_attempts.size());

    for (size_t i = 0; i < actual_dropped_task_attempts.size(); ++i) {
      EXPECT_EQ(actual_dropped_task_attempts[i], expect_dropped_task_attempts[i]);
    }
  }

  std::unique_ptr<TaskEventBufferImpl> task_event_buffer_ = nullptr;
};

struct DifferentDestination {
  bool to_gcs;
  bool to_aggregator;
};

class TaskEventBufferTestManualStart : public TaskEventBufferTest {
  void SetUp() override {}
};

class TaskEventBufferTestBatchSendDifferentDestination
    : public TaskEventBufferTest,
      public ::testing::WithParamInterface<DifferentDestination> {
 public:
  TaskEventBufferTestBatchSendDifferentDestination() : TaskEventBufferTest() {
    const auto [to_gcs, to_aggregator] = GetParam();
    std::string to_gcs_str = to_gcs ? "true" : "false";
    std::string to_aggregator_str = to_aggregator ? "true" : "false";
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_status_events_buffer_on_worker": 100,
  "task_events_max_num_profile_events_buffer_on_worker": 100,
  "task_events_send_batch_size": 10,
  "enable_core_worker_task_event_to_gcs": )" +
        to_gcs_str + R"(,
  "enable_core_worker_ray_event_to_aggregator": )" +
        to_aggregator_str + R"(
}
  )");
  }
};

class TaskEventBufferTestLimitBufferDifferentDestination
    : public TaskEventBufferTest,
      public ::testing::WithParamInterface<DifferentDestination> {
 public:
  TaskEventBufferTestLimitBufferDifferentDestination() : TaskEventBufferTest() {
    const auto [to_gcs, to_aggregator] = GetParam();
    std::string to_gcs_str = to_gcs ? "true" : "false";
    std::string to_aggregator_str = to_aggregator ? "true" : "false";
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_status_events_buffer_on_worker": 10,
  "task_events_max_num_profile_events_buffer_on_worker": 5,
  "task_events_send_batch_size": 10,
  "enable_core_worker_task_event_to_gcs": )" +
        to_gcs_str + R"(,
  "enable_core_worker_ray_event_to_aggregator": )" +
        to_aggregator_str + R"(
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
  "task_events_max_num_profile_events_per_task": 10,
  "task_events_max_num_profile_events_buffer_on_worker": 20
}
  )");
  }
};

class TaskEventBufferTestDifferentDestination
    : public TaskEventBufferTest,
      public ::testing::WithParamInterface<DifferentDestination> {
 public:
  TaskEventBufferTestDifferentDestination() : TaskEventBufferTest() {
    const auto [to_gcs, to_aggregator] = GetParam();
    std::string to_gcs_str = to_gcs ? "true" : "false";
    std::string to_aggregator_str = to_aggregator ? "true" : "false";
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_status_events_buffer_on_worker": 100,
  "task_events_send_batch_size": 100,
  "enable_core_worker_task_event_to_gcs": )" +
        to_gcs_str + R"(,
  "enable_core_worker_ray_event_to_aggregator": )" +
        to_aggregator_str + R"(
}
  )");
  }
};

void ReadContentFromFile(std::vector<std::string> &vc,
                         std::string log_file,
                         std::string filter = "") {
  std::string line;
  std::ifstream read_file;
  read_file.open(log_file, std::ios::binary);
  while (std::getline(read_file, line)) {
    if (filter.empty() || line.find(filter) != std::string::npos) {
      vc.push_back(line);
    }
  }
  read_file.close();
}

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

TEST_F(TaskEventBufferTest, TestAddEvents) {
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);

  // Test add status event
  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));

  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 1);

  // Test add profile events
  task_event_buffer_->AddTaskEvent(GenProfileTaskEvent(task_id_1, 1));
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 2);
}

TEST_P(TaskEventBufferTestDifferentDestination, TestFlushEvents) {
  const auto [to_gcs, to_aggregator] = GetParam();
  size_t num_events = 20;
  auto task_ids = GenTaskIDs(num_events);

  std::vector<std::unique_ptr<TaskEvent>> task_events;
  for (const auto &task_id : task_ids) {
    task_events.push_back(GenFullStatusTaskEvent(task_id, 0));
  }

  // Expect data flushed match. Generate expected data
  rpc::TaskEventData expected_task_event_data;
  rpc::events::RayEventsData expected_ray_events_data;
  expected_task_event_data.set_num_profile_events_dropped(0);
  for (const auto &task_event : task_events) {
    auto event = expected_task_event_data.add_events_by_task();
    task_event->ToRpcTaskEvents(event);

    RayEventsPair ray_events_pair;
    task_event->ToRpcRayEvents(ray_events_pair);
    auto [task_definition_event, task_execution_event] = ray_events_pair;
    if (task_definition_event) {
      auto event = expected_ray_events_data.add_events();
      *event = std::move(task_definition_event.value());
    }
    if (task_execution_event) {
      auto event = expected_ray_events_data.add_events();
      *event = std::move(task_execution_event.value());
    }
  }

  for (auto &task_event : task_events) {
    task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), num_events);

  // Manually call flush should call GCS client's flushing grpc.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;
  if (to_gcs) {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
        .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                      ray::gcs::StatusCallback callback) {
          CompareTaskEventData(*actual_data, expected_task_event_data);
          return Status::OK();
        });
  } else {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _)).Times(0);
  }

  // If ray events to aggregator is enabled, expect to call AddEvents grpc.
  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  rpc::events::AddEventsRequest add_events_request;
  if (to_aggregator) {
    rpc::events::AddEventsReply reply;
    Status status = Status::OK();
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _))
        .WillOnce(DoAll(
            Invoke([&](const rpc::events::AddEventsRequest &request,
                       const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) {
              CompareRayEventsData(request.events_data(), expected_ray_events_data);
            }),
            MakeAction(
                new MockEventAggregatorAddEvents(std::move(status), std::move(reply)))));
  } else {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(0);
  }

  task_event_buffer_->FlushEvents(false);

  // Expect no more events.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
}

TEST_P(TaskEventBufferTestDifferentDestination, TestFailedFlush) {
  const auto [to_gcs, to_aggregator] = GetParam();
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
  if (to_gcs) {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData)
        .Times(2)
        .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                      ray::gcs::StatusCallback callback) {
          callback(Status::RpcError("grpc error", grpc::StatusCode::UNKNOWN));
          return Status::OK();
        })
        .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                      ray::gcs::StatusCallback callback) {
          callback(Status::OK());
          return Status::OK();
        });
  }

  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  if (to_aggregator) {
    rpc::events::AddEventsReply reply_1;
    Status status_1 = Status::RpcError("grpc error", grpc::StatusCode::UNKNOWN);
    rpc::events::AddEventsReply reply_2;
    Status status_2 = Status::OK();

    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _))
        .Times(2)
        .WillOnce(MakeAction(
            new MockEventAggregatorAddEvents(std::move(status_1), std::move(reply_1))))
        .WillOnce(MakeAction(
            new MockEventAggregatorAddEvents(std::move(status_2), std::move(reply_2))));
  }

  // Flush
  task_event_buffer_->FlushEvents(false);

  // Expect the number of dropped events incremented.
  if (to_gcs) {
    ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                  TaskEventBufferCounter::kTotalNumFailedToReport),
              1);
  }
  if (to_aggregator) {
    ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                  TaskEventBufferCounter::kTotalNumFailedRequestsToAggregator),
              1);
  }

  // Adding some more events
  for (size_t i = 0; i < num_status_events + num_profile_events; ++i) {
    auto task_id = RandomTaskId();
    if (i % 2 == 0) {
      task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 1));
    } else {
      task_event_buffer_->AddTaskEvent(GenProfileTaskEvent(task_id, 1));
    }
  }

  // Flush successfully will not affect the failed to report count.
  task_event_buffer_->FlushEvents(false);
  if (to_gcs) {
    ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                  TaskEventBufferCounter::kTotalNumFailedToReport),
              1);
  }
  if (to_aggregator) {
    ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                  TaskEventBufferCounter::kTotalNumFailedRequestsToAggregator),
              1);
  }
}

TEST_P(TaskEventBufferTestDifferentDestination, TestBackPressure) {
  const auto [to_gcs, to_aggregator] = GetParam();
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
  if (to_gcs) {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(1);
  } else {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(0);
  }

  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  if (to_aggregator) {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(1);
  } else {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(0);
  }

  task_event_buffer_->FlushEvents(false);

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_2, 0));
  task_event_buffer_->FlushEvents(false);
}

TEST_P(TaskEventBufferTestDifferentDestination, TestForcedFlush) {
  const auto [to_gcs, to_aggregator] = GetParam();
  size_t num_events = 20;
  // Adding some events
  for (size_t i = 0; i < num_events; ++i) {
    auto task_id = RandomTaskId();
    task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id, 0));
  }

  // Multiple flush calls with forced should result in same number of grpc call.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;
  if (to_gcs) {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(2);
  } else {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(0);
  }

  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  if (to_aggregator) {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(2);
  } else {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(0);
  }

  auto task_id_1 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_1, 0));
  task_event_buffer_->FlushEvents(false);

  auto task_id_2 = RandomTaskId();
  task_event_buffer_->AddTaskEvent(GenStatusTaskEvent(task_id_2, 0));
  task_event_buffer_->FlushEvents(true);
}

TEST_P(TaskEventBufferTestBatchSendDifferentDestination, TestBatchedSend) {
  const auto [to_gcs, to_aggregator] = GetParam();
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
  if (to_gcs) {
    // With batch size = 10, there should be 10 flush calls
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData)
        .Times(num_events / batch_size)
        .WillRepeatedly([&batch_size](std::unique_ptr<rpc::TaskEventData> actual_data,
                                      ray::gcs::StatusCallback callback) {
          EXPECT_EQ(actual_data->events_by_task_size(), batch_size);
          callback(Status::OK());
          return Status::OK();
        });
  } else {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData).Times(0);
  }

  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  if (to_aggregator) {
    rpc::events::AddEventsReply reply;
    Status status = Status::OK();
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _))
        .Times(num_events / batch_size)
        .WillRepeatedly(DoAll(
            Invoke([&batch_size](
                       const rpc::events::AddEventsRequest &request,
                       const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) {
              EXPECT_EQ(request.events_data().events_size(), batch_size);
            }),
            MakeAction(
                new MockEventAggregatorAddEvents(std::move(status), std::move(reply)))));
  } else {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(0);
  }

  for (int i = 0; i * batch_size < num_events; i++) {
    task_event_buffer_->FlushEvents(true);
    EXPECT_EQ(task_event_buffer_->GetNumTaskEventsStored(),
              num_events - (i + 1) * batch_size);
  }

  // With last flush, there should be no more events in the buffer and as data.
  EXPECT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
}

TEST_P(TaskEventBufferTestLimitBufferDifferentDestination,
       TestBufferSizeLimitStatusEvents) {
  const auto [to_gcs, to_aggregator] = GetParam();
  size_t num_limit_status_events = 10;  // sync with setup
  size_t num_status_dropped = 10;

  // Generate 2 batches of events each, where batch 1 will be evicted by batch 2.
  std::vector<std::unique_ptr<TaskEvent>> status_events_1;
  std::vector<std::unique_ptr<TaskEvent>> status_events_2;

  // Generate data
  for (size_t i = 0; i < num_limit_status_events; ++i) {
    status_events_1.push_back(GenStatusTaskEvent(RandomTaskId(), 0));
    status_events_2.push_back(GenStatusTaskEvent(RandomTaskId(), 0));
  }

  rpc::TaskEventData expected_data;
  rpc::events::RayEventsData expected_ray_events_data;
  for (const auto &event_ptr : status_events_1) {
    rpc::TaskAttempt rpc_task_attempt;
    auto task_attempt = event_ptr->GetTaskAttempt();
    rpc_task_attempt.set_task_id(task_attempt.first.Binary());
    rpc_task_attempt.set_attempt_number(task_attempt.second);
    *(expected_data.add_dropped_task_attempts()) = rpc_task_attempt;
    *(expected_ray_events_data.mutable_task_events_metadata()
          ->add_dropped_task_attempts()) = rpc_task_attempt;
  }

  for (const auto &event_ptr : status_events_2) {
    auto expect_event = expected_data.add_events_by_task();
    // Copy the data
    auto event = std::make_unique<TaskStatusEvent>(
        *static_cast<TaskStatusEvent *>(event_ptr.get()));
    event->ToRpcTaskEvents(expect_event);

    RayEventsPair ray_events_pair;
    event->ToRpcRayEvents(ray_events_pair);
    auto [task_definition_event, task_execution_event] = ray_events_pair;
    if (task_definition_event) {
      auto event = expected_ray_events_data.add_events();
      *event = std::move(task_definition_event.value());
    }
    if (task_execution_event) {
      auto event = expected_ray_events_data.add_events();
      *event = std::move(task_execution_event.value());
    }
  }

  // Add the data
  for (auto &event : status_events_1) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  for (auto &event : status_events_2) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  // Expect only limit in buffer.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), num_limit_status_events);

  // Expect the reported data to match.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  if (to_gcs) {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
        .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                      ray::gcs::StatusCallback callback) {
          // Sort and compare
          CompareTaskEventData(*actual_data, expected_data);
          return Status::OK();
        });
  } else {
    EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _)).Times(0);
  }

  auto event_aggregator_client = static_cast<MockEventAggregatorClient *>(
      task_event_buffer_->event_aggregator_client_.get());
  if (to_aggregator) {
    rpc::events::AddEventsReply reply;
    Status status = Status::OK();
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _))
        .WillOnce(DoAll(
            Invoke([&](const rpc::events::AddEventsRequest &request,
                       const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) {
              CompareRayEventsData(request.events_data(), expected_ray_events_data);
            }),
            MakeAction(
                new MockEventAggregatorAddEvents(std::move(status), std::move(reply)))));
  } else {
    EXPECT_CALL(*event_aggregator_client, AddEvents(_, _)).Times(0);
  }
  task_event_buffer_->FlushEvents(false);

  // Expect data flushed.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush),
            0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush),
            0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskProfileEventDropped),
            0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskStatusEventDropped),
            num_status_dropped);
}

TEST_F(TaskEventBufferTestLimitProfileEvents, TestBufferSizeLimitProfileEvents) {
  size_t num_limit_profile_events = 20;  // sync with setup
  size_t num_profile_dropped = 20;

  // Generate 2 batches of events each, where batch 1 will be evicted by batch 2.
  std::vector<std::unique_ptr<TaskEvent>> profile_events_1;
  std::vector<std::unique_ptr<TaskEvent>> profile_events_2;

  // Generate data
  for (size_t i = 0; i < num_limit_profile_events; ++i) {
    profile_events_1.push_back(GenProfileTaskEvent(RandomTaskId(), 0));
    profile_events_2.push_back(GenProfileTaskEvent(RandomTaskId(), 0));
  }

  // Add the data
  for (auto &event : profile_events_1) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }
  for (auto &event : profile_events_2) {
    task_event_buffer_->AddTaskEvent(std::move(event));
  }

  // Expect only limit in buffer.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), num_limit_profile_events);

  // Expect the reported data to match.
  auto task_gcs_accessor =
      static_cast<ray::gcs::MockGcsClient *>(task_event_buffer_->GetGcsClient())
          ->mock_task_accessor;

  EXPECT_CALL(*task_gcs_accessor, AsyncAddTaskEventData(_, _))
      .WillOnce([&](std::unique_ptr<rpc::TaskEventData> actual_data,
                    ray::gcs::StatusCallback callback) {
        EXPECT_EQ(actual_data->num_profile_events_dropped(), num_profile_dropped);
        EXPECT_EQ(actual_data->events_by_task_size(), num_limit_profile_events);
        return Status::OK();
      });

  task_event_buffer_->FlushEvents(false);

  // Expect data flushed.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kNumTaskProfileEventDroppedSinceLastFlush),
            0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kNumTaskStatusEventDroppedSinceLastFlush),
            0);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskProfileEventDropped),
            num_profile_dropped);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskStatusEventDropped),
            0);
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
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskProfileEventDropped),
            num_total_profile_events - num_profile_events_per_task);
  ASSERT_EQ(task_event_buffer_->stats_counter_.Get(
                TaskEventBufferCounter::kTotalNumTaskStatusEventDropped),
            0);
}

TEST_F(TaskEventBufferTest, TestIsDebuggerPausedFlag) {
  // Generate the event
  auto task_id = RandomTaskId();
  TaskStatusEvent::TaskStateUpdate state_update(true);
  auto task_event = GenStatusTaskEvent(task_id, 0, 1, state_update);

  // Convert to rpc
  rpc::TaskEventData expected_data;
  expected_data.set_num_profile_events_dropped(0);
  auto event = expected_data.add_events_by_task();
  task_event->ToRpcTaskEvents(event);

  // Verify the flag is set
  ASSERT_TRUE(event->state_updates().is_debugger_paused());
}

TEST_F(TaskEventBufferTest, TestGracefulDestruction) {
  delete task_event_buffer_.release();
}

INSTANTIATE_TEST_SUITE_P(TaskEventBufferTest,
                         TaskEventBufferTestDifferentDestination,
                         ::testing::Values(DifferentDestination{true, true},
                                           DifferentDestination{true, false},
                                           DifferentDestination{false, true},
                                           DifferentDestination{false, false}));

INSTANTIATE_TEST_SUITE_P(TaskEventBufferTest,
                         TaskEventBufferTestBatchSendDifferentDestination,
                         ::testing::Values(DifferentDestination{true, true},
                                           DifferentDestination{true, false},
                                           DifferentDestination{false, true},
                                           DifferentDestination{false, false}));

INSTANTIATE_TEST_SUITE_P(TaskEventBufferTest,
                         TaskEventBufferTestLimitBufferDifferentDestination,
                         ::testing::Values(DifferentDestination{true, true},
                                           DifferentDestination{true, false},
                                           DifferentDestination{false, true},
                                           DifferentDestination{false, false}));

}  // namespace worker

}  // namespace core

}  // namespace ray
