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

#include "ray/observability/ray_event_recorder.h"

#include <chrono>
#include <mutex>
#include <optional>
#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/observability/fake_metric.h"
#include "ray/observability/metric_interface.h"
#include "ray/observability/ray_actor_definition_event.h"
#include "ray/observability/ray_actor_lifecycle_event.h"
#include "ray/observability/ray_driver_job_definition_event.h"
#include "ray/observability/ray_driver_job_lifecycle_event.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"
#include "src/ray/protobuf/public/events_driver_job_lifecycle_event.pb.h"

namespace ray {
namespace observability {

class FakeEventAggregatorClient : public rpc::EventAggregatorClient {
 public:
  FakeEventAggregatorClient() {}

  void AddEvents(
      const rpc::events::AddEventsRequest &request,
      const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) override {
    absl::MutexLock lock(&mutex_);
    for (const auto &event : request.events_data().events()) {
      recorded_events_.push_back(event);
    }
    if (hold_callbacks_) {
      pending_callback_ = callback;
      return;
    }
    callback(Status::OK(), rpc::events::AddEventsReply{});
  }

  std::vector<rpc::events::RayEvent> GetRecordedEvents() {
    absl::MutexLock lock(&mutex_);
    return recorded_events_;
  }

  void HoldCallbacks() {
    absl::MutexLock lock(&mutex_);
    hold_callbacks_ = true;
  }

  bool HasPendingCallback() {
    absl::MutexLock lock(&mutex_);
    return pending_callback_.has_value();
  }

  void ReleaseCallbacks() {
    rpc::ClientCallback<rpc::events::AddEventsReply> callback;
    {
      absl::MutexLock lock(&mutex_);
      if (!pending_callback_) {
        return;
      }
      callback = std::move(*pending_callback_);
      pending_callback_.reset();
      hold_callbacks_ = false;
    }
    callback(Status::OK(), rpc::events::AddEventsReply{});
  }

 private:
  std::vector<rpc::events::RayEvent> recorded_events_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
  bool hold_callbacks_ ABSL_GUARDED_BY(mutex_) = false;
  std::optional<rpc::ClientCallback<rpc::events::AddEventsReply>> pending_callback_
      ABSL_GUARDED_BY(mutex_);
};

class RayEventRecorderTest : public ::testing::Test {
 public:
  RayEventRecorderTest() {
    fake_client_ = std::make_unique<FakeEventAggregatorClient>();
    fake_dropped_events_counter_ = std::make_unique<FakeCounter>();
    test_node_id_ = NodeID::FromRandom();
    recorder_ = std::make_unique<RayEventRecorder>(*fake_client_,
                                                   io_service_,
                                                   max_buffer_size_,
                                                   "gcs",
                                                   *fake_dropped_events_counter_,
                                                   test_node_id_);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<FakeEventAggregatorClient> fake_client_;
  std::unique_ptr<FakeCounter> fake_dropped_events_counter_;
  std::unique_ptr<RayEventRecorder> recorder_;
  size_t max_buffer_size_ = 5;
  NodeID test_node_id_;
};

TEST_F(RayEventRecorderTest, TestMergeEvents) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true
}
)");
  recorder_->StartExportingEvents();
  rpc::JobTableData data;
  data.set_job_id("test_job_id");

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(std::make_unique<RayDriverJobLifecycleEvent>(
      data, rpc::events::DriverJobLifecycleEvent::CREATED, "test_session_name"));
  events.push_back(std::make_unique<RayDriverJobLifecycleEvent>(
      data, rpc::events::DriverJobLifecycleEvent::FINISHED, "test_session_name"));
  recorder_->AddEvents(std::move(events));
  io_service_.run_one();

  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  // Only one event should be recorded because the two events are merged into one.
  ASSERT_EQ(recorded_events.size(), 1);
  ASSERT_EQ(recorded_events[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[0].session_name(), "test_session_name");
  ASSERT_EQ(recorded_events[0].node_id(), test_node_id_.Binary());
  auto state_transitions =
      recorded_events[0].driver_job_lifecycle_event().state_transitions();
  ASSERT_EQ(state_transitions.size(), 2);
  ASSERT_EQ(state_transitions[0].state(), rpc::events::DriverJobLifecycleEvent::CREATED);
  ASSERT_EQ(state_transitions[1].state(), rpc::events::DriverJobLifecycleEvent::FINISHED);
}

TEST_F(RayEventRecorderTest, TestRecordEvents) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true
}
)");
  recorder_->StartExportingEvents();
  rpc::JobTableData data1;
  data1.set_job_id("test_job_id_1");
  data1.set_is_dead(false);
  data1.set_driver_pid(12345);
  data1.set_start_time(absl::ToUnixSeconds(absl::Now()));
  data1.set_end_time(0);
  data1.set_entrypoint("python test_script.py");
  data1.mutable_driver_address()->set_ip_address("127.0.0.1");

  rpc::JobTableData data2;
  data2.set_job_id("test_job_id_2");
  data2.set_is_dead(true);
  data2.set_driver_pid(67890);
  data2.set_start_time(absl::ToUnixSeconds(absl::Now()) - 3600);  // 1 hour ago
  data2.set_end_time(absl::ToUnixSeconds(absl::Now()));
  data2.set_entrypoint("python another_script.py");
  data2.mutable_driver_address()->set_ip_address("192.168.1.100");

  rpc::ActorTableData actor_def_data;
  actor_def_data.set_actor_id("actor_1");
  actor_def_data.set_job_id("test_job_id_1");
  actor_def_data.set_is_detached(true);
  actor_def_data.set_name("ActorOne");
  actor_def_data.set_ray_namespace("ns1");
  actor_def_data.set_serialized_runtime_env("{}");
  actor_def_data.set_class_name("Cls");
  (*actor_def_data.mutable_required_resources())["CPU"] = 1.0;

  rpc::ActorTableData actor_life_data;
  actor_life_data.set_actor_id("actor_2");
  actor_life_data.set_job_id("test_job_id_2");
  actor_life_data.set_node_id("node-xyz");

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data1, "test_session_name_1"));
  events.push_back(std::make_unique<RayDriverJobLifecycleEvent>(
      data2, rpc::events::DriverJobLifecycleEvent::FINISHED, "test_session_name_2"));
  events.push_back(
      std::make_unique<RayActorDefinitionEvent>(actor_def_data, "test_session_name_3"));
  events.push_back(std::make_unique<RayActorLifecycleEvent>(
      actor_life_data, rpc::events::ActorLifecycleEvent::ALIVE, "test_session_name_4"));
  recorder_->AddEvents(std::move(events));
  io_service_.run_one();

  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  std::sort(recorded_events.begin(),
            recorded_events.end(),
            [](const rpc::events::RayEvent &a, const rpc::events::RayEvent &b) {
              return a.session_name() < b.session_name();
            });

  // Verify events
  ASSERT_EQ(recorded_events.size(), 4);

  // Verify first event
  ASSERT_EQ(recorded_events[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[0].session_name(), "test_session_name_1");
  ASSERT_EQ(recorded_events[0].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  ASSERT_EQ(recorded_events[0].severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(recorded_events[0].node_id(), test_node_id_.Binary());
  ASSERT_TRUE(recorded_events[0].has_driver_job_definition_event());
  ASSERT_EQ(recorded_events[0].driver_job_definition_event().job_id(), "test_job_id_1");

  // Verify second event
  ASSERT_EQ(recorded_events[1].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[1].session_name(), "test_session_name_2");
  ASSERT_EQ(recorded_events[1].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_LIFECYCLE_EVENT);
  ASSERT_EQ(recorded_events[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(recorded_events[1].node_id(), test_node_id_.Binary());
  ASSERT_TRUE(recorded_events[1].has_driver_job_lifecycle_event());
  ASSERT_EQ(recorded_events[1].driver_job_lifecycle_event().job_id(), "test_job_id_2");

  // Verify third event (actor definition)
  ASSERT_EQ(recorded_events[2].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[2].session_name(), "test_session_name_3");
  ASSERT_EQ(recorded_events[2].event_type(),
            rpc::events::RayEvent::ACTOR_DEFINITION_EVENT);
  ASSERT_EQ(recorded_events[2].node_id(), test_node_id_.Binary());
  ASSERT_TRUE(recorded_events[2].has_actor_definition_event());
  ASSERT_EQ(recorded_events[2].actor_definition_event().actor_id(), "actor_1");
  ASSERT_EQ(recorded_events[2].actor_definition_event().job_id(), "test_job_id_1");

  // Verify fourth event (actor lifecycle)
  ASSERT_EQ(recorded_events[3].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[3].session_name(), "test_session_name_4");
  ASSERT_EQ(recorded_events[3].event_type(),
            rpc::events::RayEvent::ACTOR_LIFECYCLE_EVENT);
  ASSERT_EQ(recorded_events[3].node_id(), test_node_id_.Binary());
  ASSERT_TRUE(recorded_events[3].has_actor_lifecycle_event());
  ASSERT_EQ(recorded_events[3].actor_lifecycle_event().actor_id(), "actor_2");
  ASSERT_EQ(recorded_events[3].actor_lifecycle_event().state_transitions_size(), 1);
  ASSERT_EQ(recorded_events[3].actor_lifecycle_event().state_transitions(0).state(),
            rpc::events::ActorLifecycleEvent::ALIVE);
}

TEST_F(RayEventRecorderTest, TestDropEvents) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true
}
)");
  recorder_->StartExportingEvents();
  size_t expected_num_dropped_events = 3;

  // Add more events than the buffer size
  std::vector<std::unique_ptr<RayEventInterface>> events_01;
  for (size_t i = 0; i < max_buffer_size_ + 1; i++) {
    rpc::JobTableData data;
    data.set_job_id("test_job_id");
    events_01.push_back(
        std::make_unique<RayDriverJobDefinitionEvent>(data, "test_session"));
  }
  recorder_->AddEvents(std::move(events_01));

  // The buffer is full now, add more events to test the overflow handling
  std::vector<std::unique_ptr<RayEventInterface>> events_02;
  for (size_t i = 0; i < expected_num_dropped_events - 1; i++) {
    rpc::JobTableData data;
    data.set_job_id("test_job_id_" + std::to_string(i));
    events_02.push_back(
        std::make_unique<RayDriverJobDefinitionEvent>(data, "test_session"));
  }
  recorder_->AddEvents(std::move(events_02));
  io_service_.run_one();

  auto tag_to_value = fake_dropped_events_counter_->GetTagToValue();
  size_t num_dropped_events = 0;
  for (const auto &[tags, value] : tag_to_value) {
    num_dropped_events += value;
  }
  ASSERT_EQ(num_dropped_events, expected_num_dropped_events);
}

TEST_F(RayEventRecorderTest, TestDisabled) {
  RayConfig::instance().initialize(
      R"(
{
  "enable_ray_event": false
}
  )");
  recorder_->StartExportingEvents();
  rpc::JobTableData data;
  data.set_job_id("test_job_id_1");
  data.set_is_dead(false);
  data.set_driver_pid(12345);
  data.set_start_time(absl::ToUnixSeconds(absl::Now()));
  data.set_end_time(0);
  data.set_entrypoint("python test_script.py");
  data.mutable_driver_address()->set_ip_address("127.0.0.1");

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data, "test_session_name"));
  recorder_->AddEvents(std::move(events));
  io_service_.run_one();
  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded_events.size(), 0);
}

// Test that StopExportingEvents() flushes all buffered events.
TEST_F(RayEventRecorderTest, TestStopFlushesEvents) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true,
"task_events_shutdown_flush_timeout_ms": 100
}
)");
  recorder_->StartExportingEvents();

  // Add events without running the io service (simulating buffered events)
  rpc::JobTableData data;
  data.set_job_id("test_job_id_1");
  data.set_is_dead(false);
  data.set_driver_pid(12345);
  data.set_start_time(absl::ToUnixSeconds(absl::Now()));
  data.set_end_time(0);
  data.set_entrypoint("python test_script.py");
  data.mutable_driver_address()->set_ip_address("127.0.0.1");

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data, "test_session_name"));
  recorder_->AddEvents(std::move(events));

  // Don't run the io_service yet - events should still be in the buffer

  // Now call StopExportingEvents() - this should flush the buffered events
  recorder_->StopExportingEvents();

  // Run the io_service to process the flush
  io_service_.run_one();

  // Verify that events were flushed
  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded_events.size(), 1);
  ASSERT_EQ(recorded_events[0].driver_job_definition_event().job_id(), "test_job_id_1");
}

// Test that StopExportingEvents() waits for an in-flight export and then flushes
// remaining events once the in-flight gRPC completes.
TEST_F(RayEventRecorderTest, TestStopWaitsForInflightThenFlushes) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true,
"task_events_shutdown_flush_timeout_ms": 100
}
)");
  recorder_->StartExportingEvents();
  fake_client_->HoldCallbacks();

  // Add one event and trigger the periodic export to create an in-flight gRPC.
  rpc::JobTableData data_1;
  data_1.set_job_id("test_job_id_inflight");
  std::vector<std::unique_ptr<RayEventInterface>> events_1;
  events_1.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data_1, "test_session_name"));
  recorder_->AddEvents(std::move(events_1));
  io_service_.run_one();

  // Add a second event that should be flushed after the in-flight gRPC completes.
  rpc::JobTableData data_2;
  data_2.set_job_id("test_job_id_after");
  std::vector<std::unique_ptr<RayEventInterface>> events_2;
  events_2.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data_2, "test_session_name"));
  recorder_->AddEvents(std::move(events_2));

  std::thread stop_thread([&]() { recorder_->StopExportingEvents(); });

  // Ensure the in-flight callback is released so StopExportingEvents can flush remaining
  // events.
  ASSERT_TRUE(fake_client_->HasPendingCallback());
  fake_client_->ReleaseCallbacks();
  stop_thread.join();

  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded_events.size(), 2);
  EXPECT_EQ(recorded_events[0].driver_job_definition_event().job_id(),
            "test_job_id_inflight");
  EXPECT_EQ(recorded_events[1].driver_job_definition_event().job_id(),
            "test_job_id_after");
}

// Test that ExportEvents() skips if there's already an in-flight gRPC call.
// This prevents overlapping exports which could cause StopExportingEvents() to
// return while a gRPC is still in flight.
TEST_F(RayEventRecorderTest, TestExportSkipsWhenGrpcInProgress) {
  RayConfig::instance().initialize(
      R"(
{
"enable_ray_event": true,
"task_events_shutdown_flush_timeout_ms": 100
}
)");
  recorder_->StartExportingEvents();
  fake_client_->HoldCallbacks();

  // Add first event and trigger export - this creates an in-flight gRPC
  rpc::JobTableData data_1;
  data_1.set_job_id("test_job_id_first");
  std::vector<std::unique_ptr<RayEventInterface>> events_1;
  events_1.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data_1, "test_session_name"));
  recorder_->AddEvents(std::move(events_1));
  io_service_.run_one();  // This triggers the export

  ASSERT_TRUE(fake_client_->HasPendingCallback());

  // Add second event while gRPC is in progress
  rpc::JobTableData data_2;
  data_2.set_job_id("test_job_id_second");
  std::vector<std::unique_ptr<RayEventInterface>> events_2;
  events_2.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data_2, "test_session_name"));
  recorder_->AddEvents(std::move(events_2));

  // Try to trigger another export - this should be skipped because gRPC is in progress
  io_service_.run_one();

  // Release the first callback
  fake_client_->ReleaseCallbacks();

  // Now trigger export again - this should send the second event
  io_service_.run_one();

  // Verify both events were eventually sent (first before skip, second after)
  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  ASSERT_EQ(recorded_events.size(), 2);
  EXPECT_EQ(recorded_events[0].driver_job_definition_event().job_id(),
            "test_job_id_first");
  EXPECT_EQ(recorded_events[1].driver_job_definition_event().job_id(),
            "test_job_id_second");
}

}  // namespace observability
}  // namespace ray
