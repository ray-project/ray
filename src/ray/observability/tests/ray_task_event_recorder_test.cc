// Copyright 2025 The Ray Authors.
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

#include "ray/observability/ray_task_event_recorder.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/asio/instrumented_io_context.h"
#include "ray/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/task_ray_event_interface.h"
#include "src/ray/protobuf/events_event_aggregator_service.pb.h"

namespace ray {
namespace observability {

// Aggregator client fake that records both the exported events and the dropped task
// attempts reported in the request metadata.
class FakeAggregatorClient : public rpc::EventAggregatorClient {
 public:
  void AddEvents(
      const rpc::events::AddEventsRequest &request,
      const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) override {
    absl::MutexLock lock(&mutex_);
    for (const auto &event : request.events_data().events()) {
      recorded_events_.push_back(event);
    }
    for (const auto &attempt :
         request.events_data().task_events_metadata().dropped_task_attempts()) {
      dropped_attempts_.push_back(attempt);
    }
    callback(Status::OK(), rpc::events::AddEventsReply{});
  }

  std::vector<rpc::events::RayEvent> GetRecordedEvents() {
    absl::MutexLock lock(&mutex_);
    return recorded_events_;
  }

  std::vector<rpc::TaskAttempt> GetDroppedAttempts() {
    absl::MutexLock lock(&mutex_);
    return dropped_attempts_;
  }

 private:
  std::vector<rpc::events::RayEvent> recorded_events_ ABSL_GUARDED_BY(mutex_);
  std::vector<rpc::TaskAttempt> dropped_attempts_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

// A valid 24-byte TaskID binary derived from a human label. The recorder's drop-warning
// path calls TaskID::FromBinary(attempt.first), which CHECK-fails unless the id is
// exactly TaskID::Size() (24) bytes — so test task ids must be full size, not short
// strings.
std::string Tid(const std::string &label) {
  std::string binary = label;
  binary.resize(TaskID::Size(), '_');
  return binary;
}

// Minimal task event: carries a task attempt + event type. The task attempt uses a valid
// 24-byte task-id binary (derived from `label` via Tid); the serialized RayEvent's
// `message` uses the human `label` ("label:attempt") so tests can identify exported
// events.
class FakeTaskRayEvent : public RayEventInterface, public TaskRayEventInterface {
 public:
  FakeTaskRayEvent(const std::string &label,
                   int32_t attempt,
                   rpc::events::RayEvent::EventType type)
      : task_id_(Tid(label)), label_(label), attempt_(attempt), type_(type) {}

  std::string GetEntityId() const override { return task_id_ + std::to_string(attempt_); }

  void Merge(RayEventInterface &&other) override {}

  ray::StatusSetOr<ray::rpc::events::RayEvent, ray::StatusT::Invalid> Serialize() &&
      override {
    rpc::events::RayEvent event;
    event.set_event_type(type_);
    event.set_message(label_ + ":" + std::to_string(attempt_));
    return event;
  }

  rpc::events::RayEvent::EventType GetEventType() const override { return type_; }

  bool SupportsMerge() const override { return true; }

  TaskAttemptId GetTaskAttempt() const override { return {task_id_, attempt_}; }

 private:
  std::string task_id_;
  std::string label_;
  int32_t attempt_;
  rpc::events::RayEvent::EventType type_;
};

class RayTaskEventRecorderTest : public ::testing::Test {
 public:
  RayTaskEventRecorderTest() {
    fake_client_ = std::make_unique<FakeAggregatorClient>();
    fake_dropped_counter_ = std::make_unique<FakeCounter>();
    node_id_ = NodeID::FromRandom();
    recorder_ =
        std::make_unique<RayTaskEventRecorder>(*fake_client_,
                                               PeriodicalRunner::Create(io_service_),
                                               max_buffer_size_,
                                               "core_worker",
                                               *fake_dropped_counter_,
                                               node_id_);
  }

  std::unique_ptr<RayEventInterface> StatusEvent(const std::string &task_id,
                                                 int32_t attempt) {
    return std::make_unique<FakeTaskRayEvent>(
        task_id, attempt, rpc::events::RayEvent::TASK_LIFECYCLE_EVENT);
  }

  std::unique_ptr<RayEventInterface> ProfileEvent(const std::string &task_id,
                                                  int32_t attempt) {
    return std::make_unique<FakeTaskRayEvent>(
        task_id, attempt, rpc::events::RayEvent::TASK_PROFILE_EVENT);
  }

  void AddOne(std::unique_ptr<RayEventInterface> event) {
    AddOneTo(*recorder_, std::move(event));
  }

  void AddOneTo(RayTaskEventRecorder &recorder,
                std::unique_ptr<RayEventInterface> event) {
    std::vector<std::unique_ptr<RayEventInterface>> events;
    events.push_back(std::move(event));
    recorder.AddEvents(std::move(events));
  }

  // Sum of all dropped-event metric values across tag sets.
  size_t TotalDropped() {
    size_t total = 0;
    for (const auto &[tags, value] : fake_dropped_counter_->GetTagToValue()) {
      total += value;
    }
    return total;
  }

  instrumented_io_context io_service_;
  std::unique_ptr<FakeAggregatorClient> fake_client_;
  std::unique_ptr<FakeCounter> fake_dropped_counter_;
  std::unique_ptr<RayTaskEventRecorder> recorder_;
  size_t max_buffer_size_ = 3;
  NodeID node_id_;
};

// Status events within capacity are all exported, with node_id stamped, no drops.
TEST_F(RayTaskEventRecorderTest, TestStatusEventsExported) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  recorder_->StartExportingEvents();

  AddOne(StatusEvent("task1", 0));
  AddOne(StatusEvent("task2", 0));
  AddOne(StatusEvent("task3", 0));
  io_service_.run_one();

  auto recorded = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded.size(), 3);
  for (const auto &event : recorded) {
    EXPECT_EQ(event.node_id(), node_id_.Binary());
  }
  EXPECT_TRUE(fake_client_->GetDroppedAttempts().empty());
  EXPECT_EQ(TotalDropped(), 0);
}

// Overflowing the status ring reports the evicted attempts as dropped and does not
// export their events.
TEST_F(RayTaskEventRecorderTest, TestStatusRingOverflowReportsDroppedAttempts) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  recorder_->StartExportingEvents();

  // Capacity is 3; adding 5 distinct attempts evicts the two oldest (task1, task2).
  for (int i = 1; i <= 5; i++) {
    AddOne(StatusEvent("task" + std::to_string(i), 0));
  }
  io_service_.run_one();

  auto recorded = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded.size(), 3);
  std::vector<std::string> messages;
  for (const auto &event : recorded) {
    messages.push_back(event.message());
  }
  // task1 and task2 were evicted; task3..task5 survive.
  EXPECT_EQ(std::count(messages.begin(), messages.end(), "task1:0"), 0);
  EXPECT_EQ(std::count(messages.begin(), messages.end(), "task2:0"), 0);

  auto dropped = fake_client_->GetDroppedAttempts();
  ASSERT_EQ(dropped.size(), 2);
  std::vector<std::string> dropped_ids;
  for (const auto &attempt : dropped) {
    dropped_ids.push_back(attempt.task_id());
    EXPECT_EQ(attempt.attempt_number(), 0);
  }
  EXPECT_EQ(std::count(dropped_ids.begin(), dropped_ids.end(), Tid("task1")), 1);
  EXPECT_EQ(std::count(dropped_ids.begin(), dropped_ids.end(), Tid("task2")), 1);
}

// Once an attempt is dropped, later events for it are dropped on add (sticky).
TEST_F(RayTaskEventRecorderTest, TestStickyDropForDroppedAttempt) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  recorder_->StartExportingEvents();

  // Fill the ring, then evict task1 by adding task4.
  AddOne(StatusEvent("task1", 0));
  AddOne(StatusEvent("task2", 0));
  AddOne(StatusEvent("task3", 0));
  AddOne(StatusEvent("task4", 0));  // evicts task1 -> task1 dropped (sticky)
  // A new event for the dropped attempt task1 is dropped on add.
  AddOne(StatusEvent("task1", 0));
  io_service_.run_one();

  auto recorded = fake_client_->GetRecordedEvents();
  // Only task2, task3, task4 are exported; both task1 events are gone.
  ASSERT_EQ(recorded.size(), 3);
  for (const auto &event : recorded) {
    EXPECT_NE(event.message(), "task1:0");
  }
  auto dropped = fake_client_->GetDroppedAttempts();
  ASSERT_EQ(dropped.size(), 1);
  EXPECT_EQ(dropped[0].task_id(), Tid("task1"));
}

// All-or-none per attempt: a buffered event whose attempt was dropped (via eviction of an
// earlier event of the same attempt) is skipped on export, and the attempt is reported.
TEST_F(RayTaskEventRecorderTest, TestAllOrNoneSkipsBufferedEventForDroppedAttempt) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  recorder_->StartExportingEvents();

  // Two events for task1 fill 2 of 3 slots; task2 fills the ring.
  AddOne(StatusEvent("task1", 0));  // task1 event #1 (front)
  AddOne(StatusEvent("task1", 0));  // task1 event #2
  AddOne(StatusEvent("task2", 0));  // ring full: [t1#1, t1#2, t2]
  // task3 evicts the front (task1 event #1) -> task1 dropped, but task1 event #2 remains.
  AddOne(StatusEvent("task3", 0));
  io_service_.run_one();

  auto recorded = fake_client_->GetRecordedEvents();
  // task1's remaining buffered event is skipped; only task2 and task3 are exported.
  ASSERT_EQ(recorded.size(), 2);
  for (const auto &event : recorded) {
    EXPECT_NE(event.message(), "task1:0");
  }
  auto dropped = fake_client_->GetDroppedAttempts();
  ASSERT_EQ(dropped.size(), 1);
  EXPECT_EQ(dropped[0].task_id(), Tid("task1"));
}

// A flush with only dropped attempts and no surviving events still sends the metadata
// (events_size()==0 but dropped_task_attempts>0), exercising the has_aggregator_payload
// guard's "send anyway" branch.
TEST_F(RayTaskEventRecorderTest, TestMetadataOnlySendForFullyDroppedAttempt) {
  RayConfig::instance().initialize(R"({"enable_ray_event": true})");
  FakeAggregatorClient client;
  FakeCounter counter;
  RayTaskEventRecorder recorder(client,
                                PeriodicalRunner::Create(io_service_),
                                /*max_buffer_size=*/1,
                                "core_worker",
                                counter,
                                node_id_);
  recorder.StartExportingEvents();

  // Capacity 1: the second task1 event evicts the first, flagging task1 as dropped; the
  // survivor (also task1) is then skipped on export, leaving only the metadata.
  AddOneTo(recorder, StatusEvent("task1", 0));
  AddOneTo(recorder, StatusEvent("task1", 0));
  io_service_.run_one();

  EXPECT_TRUE(client.GetRecordedEvents().empty());
  auto dropped = client.GetDroppedAttempts();
  ASSERT_EQ(dropped.size(), 1);
  EXPECT_EQ(dropped[0].task_id(), Tid("task1"));
}

// Profile events beyond the per-task cap are dropped (newest), and not reported in-band.
TEST_F(RayTaskEventRecorderTest, TestProfilePerTaskCap) {
  RayConfig::instance().initialize(
      R"({"enable_ray_event": true,
          "task_events_max_num_profile_events_per_task": 2,
          "task_events_max_num_profile_events_buffer_on_worker": 1000})");
  recorder_->StartExportingEvents();

  // 5 profile events for the same attempt; only 2 are kept, 3 dropped.
  for (int i = 0; i < 5; i++) {
    AddOne(ProfileEvent("task1", 0));
  }
  io_service_.run_one();

  // The 2 kept profile events merge (same entity_id + type) into a single RayEvent.
  auto recorded = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded.size(), 1);
  EXPECT_EQ(recorded[0].event_type(), rpc::events::RayEvent::TASK_PROFILE_EVENT);
  EXPECT_EQ(TotalDropped(), 3);
  // Profile drops are metric-only; they are not reported as dropped attempts.
  EXPECT_TRUE(fake_client_->GetDroppedAttempts().empty());
}

// Profile events beyond the global cap are dropped (newest), across attempts.
TEST_F(RayTaskEventRecorderTest, TestProfileGlobalCap) {
  RayConfig::instance().initialize(
      R"({"enable_ray_event": true,
          "task_events_max_num_profile_events_per_task": 1000,
          "task_events_max_num_profile_events_buffer_on_worker": 2})");
  recorder_->StartExportingEvents();

  // 4 profile events across distinct attempts; only the first 2 are kept.
  AddOne(ProfileEvent("task1", 0));
  AddOne(ProfileEvent("task2", 0));
  AddOne(ProfileEvent("task3", 0));
  AddOne(ProfileEvent("task4", 0));
  io_service_.run_one();

  auto recorded = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded.size(), 2);
  EXPECT_EQ(TotalDropped(), 2);
  EXPECT_TRUE(fake_client_->GetDroppedAttempts().empty());
}

// When ray events are disabled, nothing is buffered or exported.
TEST_F(RayTaskEventRecorderTest, TestDisabled) {
  RayConfig::instance().initialize(R"({"enable_ray_event": false})");
  recorder_->StartExportingEvents();

  AddOne(StatusEvent("task1", 0));
  io_service_.run_one();

  EXPECT_TRUE(fake_client_->GetRecordedEvents().empty());
  EXPECT_TRUE(fake_client_->GetDroppedAttempts().empty());
}

}  // namespace observability
}  // namespace ray
