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
#include "ray/observability/ray_driver_job_definition_event.h"
#include "ray/observability/ray_driver_job_execution_event.h"
#include "src/ray/protobuf/gcs.pb.h"
#include "src/ray/protobuf/public/events_base_event.pb.h"
#include "src/ray/protobuf/public/events_driver_job_execution_event.pb.h"

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
    recorder_ = std::make_unique<RayEventRecorder>(*fake_client_, io_service_);
    recorder_->StartExportingEvents();
  }

  instrumented_io_context io_service_;
  std::unique_ptr<FakeEventAggregatorClient> fake_client_;
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
  io_service_.run_one();

  std::vector<rpc::events::RayEvent> recorded_events = fake_client_->GetRecordedEvents();
  // Verify first event
  ASSERT_EQ(recorded_events.size(), 2);
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
            rpc::events::RayEvent::DRIVER_JOB_EXECUTION_EVENT);
  ASSERT_EQ(recorded_events[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_TRUE(recorded_events[1].has_driver_job_execution_event());
  ASSERT_EQ(recorded_events[1].driver_job_execution_event().job_id(), "test_job_id_2");
}

}  // namespace observability
}  // namespace ray
