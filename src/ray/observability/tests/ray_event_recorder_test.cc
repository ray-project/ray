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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/observability/ray_actor_definition_event.h"
#include "ray/observability/ray_actor_lifecycle_event.h"
#include "ray/observability/ray_driver_job_definition_event.h"
#include "ray/observability/ray_driver_job_execution_event.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"
#include "src/ray/protobuf/public/events_driver_job_execution_event.pb.h"

namespace ray {
namespace observability {

class MockEventAggregatorClient : public rpc::EventAggregatorClient {
 public:
  MockEventAggregatorClient(std::vector<rpc::events::RayEvent> &recorded_events)
      : recorded_events_(recorded_events) {}

  void AddEvents(
      const rpc::events::AddEventsRequest &request,
      const rpc::ClientCallback<rpc::events::AddEventsReply> &callback) override {
    for (const auto &event : request.events_data().events()) {
      recorded_events_.push_back(event);
    }
    callback(Status::OK(), rpc::events::AddEventsReply{});
  }

 private:
  std::vector<rpc::events::RayEvent> &recorded_events_;
};

class RayEventRecorderTest : public ::testing::Test {
 public:
  RayEventRecorderTest() {
    mock_client_ = std::make_unique<MockEventAggregatorClient>(recorded_events_);
    recorder_ = std::make_unique<RayEventRecorder>(*mock_client_, io_service_);
  }

  void SetUp() override {
    // Clean up recorded events before each test
    recorded_events_.clear();
    recorder_->StartExportingEvents();
  }

 protected:
  instrumented_io_context io_service_;
  std::vector<rpc::events::RayEvent> recorded_events_;
  std::unique_ptr<MockEventAggregatorClient> mock_client_;
  std::unique_ptr<RayEventRecorder> recorder_;
};

TEST_F(RayEventRecorderTest, TestRecordEvents) {
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

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(
      std::make_unique<RayDriverJobDefinitionEvent>(data1, "test_session_name_1"));
  events.push_back(std::make_unique<RayDriverJobExecutionEvent>(
      data2, rpc::events::DriverJobExecutionEvent::SUCCESS, "test_session_name_2"));
  recorder_->AddEvents(std::move(events));
  io_service_.run_for(
      std::chrono::milliseconds(RayConfig::instance().ray_events_report_interval_ms()));
  ASSERT_EQ(recorded_events_.size(), 2);

  // Verify first event
  ASSERT_EQ(recorded_events_[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[0].session_name(), "test_session_name_1");
  ASSERT_EQ(recorded_events_[0].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_DEFINITION_EVENT);
  ASSERT_EQ(recorded_events_[0].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events_[0].has_driver_job_definition_event());
  ASSERT_EQ(recorded_events_[0].driver_job_definition_event().job_id(), "test_job_id_1");

  // Verify second event
  ASSERT_EQ(recorded_events_[1].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[1].session_name(), "test_session_name_2");
  ASSERT_EQ(recorded_events_[1].event_type(),
            rpc::events::RayEvent::DRIVER_JOB_EXECUTION_EVENT);
  ASSERT_EQ(recorded_events_[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events_[1].has_driver_job_execution_event());
  ASSERT_EQ(recorded_events_[1].driver_job_execution_event().job_id(), "test_job_id_2");
}

TEST_F(RayEventRecorderTest, TestRecordActorEvents) {
  rpc::ActorTableData actor_data1;
  actor_data1.set_actor_id("test_actor_id_1");
  actor_data1.set_job_id("test_job_id_1");
  actor_data1.set_is_detached(false);
  actor_data1.set_name("test_actor_1");
  actor_data1.set_class_name("TestActorClass");
  actor_data1.set_pid(12345);
  actor_data1.set_state(rpc::ActorTableData::ALIVE);
  actor_data1.set_ray_namespace("test_namespace");

  rpc::ActorTableData actor_data2;
  actor_data2.set_actor_id("test_actor_id_2");
  actor_data2.set_job_id("test_job_id_2");
  actor_data2.set_is_detached(true);
  actor_data2.set_name("test_actor_2");
  actor_data2.set_class_name("AnotherActorClass");
  actor_data2.set_state(rpc::ActorTableData::DEAD);
  actor_data2.set_ray_namespace("another_namespace");
  actor_data2.mutable_death_cause()
      ->mutable_actor_died_error_context()
      ->set_error_message("test error message");

  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(
      std::make_unique<RayActorDefinitionEvent>(actor_data1, "test_session_name_1"));
  events.push_back(
      std::make_unique<RayActorLifecycleEvent>(actor_data2,
                                               rpc::ActorLifecycleEvent::DEAD,
                                               "test_worker_id_2",
                                               "test_session_name_2"));

  recorder_->AddEvents(std::move(events));

  io_service_.run_for(
      std::chrono::milliseconds(RayConfig::instance().ray_events_report_interval_ms()));

  ASSERT_EQ(recorded_events_.size(), 2);
  // Verify first event (actor definition)
  ASSERT_EQ(recorded_events_[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[0].session_name(), "test_session_name_1");
  ASSERT_EQ(recorded_events_[0].event_type(),
            rpc::events::RayEvent::ACTOR_DEFINITION_EVENT);
  ASSERT_EQ(recorded_events_[0].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events_[0].has_actor_definition_event());
  ASSERT_EQ(recorded_events_[0].actor_definition_event().actor_id(), "test_actor_id_1");
  ASSERT_EQ(recorded_events_[0].actor_definition_event().name(), "test_actor_1");
  ASSERT_EQ(recorded_events_[0].actor_definition_event().class_name(), "TestActorClass");
  ASSERT_EQ(recorded_events_[0].actor_definition_event().label_selector_size(), 0);

  // Verify second event (actor execution)
  ASSERT_EQ(recorded_events_[1].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[1].session_name(), "test_session_name_2");
  ASSERT_EQ(recorded_events_[1].event_type(),
            rpc::events::RayEvent::ACTOR_LIFECYCLE_EVENT);
  ASSERT_EQ(recorded_events_[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events_[1].has_actor_lifecycle_event());
  ASSERT_EQ(recorded_events_[1].actor_lifecycle_event().actor_id(), "test_actor_id_2");
  ASSERT_EQ(recorded_events_[1].actor_lifecycle_event().states_size(), 1);
  ASSERT_EQ(recorded_events_[1].actor_lifecycle_event().states(0).state(),
            rpc::ActorLifecycleEvent::DEAD);
  ASSERT_EQ(recorded_events_[1].actor_lifecycle_event().states(0).node_id(), "");
  ASSERT_EQ(recorded_events_[1]
                .actor_lifecycle_event()
                .states(0)
                .death_cause()
                .actor_died_error_context()
                .error_message(),
            "test error message");
}

TEST_F(RayEventRecorderTest, TestMergeActorLifecycleEvents) {
  rpc::ActorTableData actor_data;
  actor_data.set_actor_id("test_actor_id_merge");
  actor_data.set_job_id("test_job_id_merge");
  actor_data.set_node_id("test_node_id");
  actor_data.set_state(rpc::ActorTableData::ALIVE);

  // Create first event with ALIVE state
  auto event1 = std::make_unique<RayActorLifecycleEvent>(actor_data,
                                                         rpc::ActorLifecycleEvent::ALIVE,
                                                         "test_worker_id_1",
                                                         "test_session_name_merge");

  // Create second event with DEAD state
  actor_data.set_state(rpc::ActorTableData::DEAD);
  actor_data.mutable_death_cause()->mutable_actor_died_error_context()->set_error_message(
      "test error message");
  auto event2 = std::make_unique<RayActorLifecycleEvent>(actor_data,
                                                         rpc::ActorLifecycleEvent::DEAD,
                                                         "test_worker_id_2",
                                                         "test_session_name_merge");

  // Merge the events
  event1->Merge(std::move(*event2));

  // Record the merged event
  std::vector<std::unique_ptr<RayEventInterface>> events;
  events.push_back(std::move(event1));
  recorder_->AddEvents(std::move(events));
  io_service_.run_for(
      std::chrono::milliseconds(RayConfig::instance().ray_events_report_interval_ms()));

  ASSERT_EQ(recorded_events_.size(), 1);
  // Verify merged event contains both states
  ASSERT_TRUE(recorded_events_[0].has_actor_lifecycle_event());
  const auto &actor_event = recorded_events_[0].actor_lifecycle_event();
  ASSERT_EQ(actor_event.states_size(), 2);
  ASSERT_EQ(actor_event.states(0).state(), rpc::ActorLifecycleEvent::ALIVE);
  ASSERT_EQ(actor_event.states(1).state(), rpc::ActorLifecycleEvent::DEAD);
  ASSERT_EQ(actor_event.states(0).node_id(), "test_node_id");
  ASSERT_EQ(actor_event.states(0).worker_id(), "test_worker_id_1");
  ASSERT_EQ(actor_event.states(1).node_id(), "");
  ASSERT_EQ(actor_event.states(1).worker_id(),
            "");  // Worker ID is not set for DEAD state
  ASSERT_EQ(
      actor_event.states(1).death_cause().actor_died_error_context().error_message(),
      "test error message");
}
}  // namespace observability
}  // namespace ray
