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
    callback(Status::OK(), rpc::events::AddEventsReply{});
  }

  std::vector<rpc::events::RayEvent> GetRecordedEvents() {
    absl::MutexLock lock(&mutex_);
    return recorded_events_;
  }

 private:
  std::vector<rpc::events::RayEvent> recorded_events_ ABSL_GUARDED_BY(mutex_);
  absl::Mutex mutex_;
};

class RayEventRecorderTest : public ::testing::Test {
 public:
  RayEventRecorderTest() {
    fake_client_ = std::make_unique<FakeEventAggregatorClient>();
    fake_dropped_events_counter_ = std::make_unique<FakeCounter>();
    recorder_ = std::make_unique<RayEventRecorder>(*fake_client_,
                                                   io_service_,
                                                   max_buffer_size_,
                                                   "gcs",
                                                   *fake_dropped_events_counter_);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<FakeEventAggregatorClient> fake_client_;
  std::unique_ptr<FakeCounter> fake_dropped_events_counter_;
  std::unique_ptr<RayEventRecorder> recorder_;
  size_t max_buffer_size_ = 5;
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
  ASSERT_EQ(recorded_events[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[0].session_name(), "test_session_name_1");
  ASSERT_EQ(recorded_events[0].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  ASSERT_EQ(recorded_events[0].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events[0].has_driver_job_definition_event());
  ASSERT_EQ(recorded_events[0].driver_job_definition_event().job_id(), "test_job_id_1");

  // Verify second event
  ASSERT_EQ(recorded_events[1].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[1].session_name(), "test_session_name_2");
  ASSERT_EQ(recorded_events[1].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_LIFECYCLE_EVENT);
  ASSERT_EQ(recorded_events[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events[1].has_driver_job_lifecycle_event());
  ASSERT_EQ(recorded_events[1].driver_job_lifecycle_event().job_id(), "test_job_id_2");

  // Verify third event (actor definition)
  ASSERT_EQ(recorded_events[2].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[2].session_name(), "test_session_name_3");
  ASSERT_EQ(recorded_events[2].event_type(),
            rpc::events::RayEvent::ACTOR_DEFINITION_EVENT);
  ASSERT_TRUE(recorded_events[2].has_actor_definition_event());
  ASSERT_EQ(recorded_events[2].actor_definition_event().actor_id(), "actor_1");
  ASSERT_EQ(recorded_events[2].actor_definition_event().job_id(), "test_job_id_1");

  // Verify fourth event (actor lifecycle)
  ASSERT_EQ(recorded_events[3].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events[3].session_name(), "test_session_name_4");
  ASSERT_EQ(recorded_events[3].event_type(),
            rpc::events::RayEvent::ACTOR_LIFECYCLE_EVENT);
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

}  // namespace observability
}  // namespace ray
