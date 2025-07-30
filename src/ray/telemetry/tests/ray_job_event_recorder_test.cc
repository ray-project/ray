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

#include "ray/telemetry/ray_job_event_recorder.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "src/ray/protobuf/events_base_event.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace telemetry {

class MockEventAggregatorClient : public rpc::EventAggregatorClient {
 public:
  MockEventAggregatorClient(std::vector<rpc::events::RayEvent> &recorded_events)
      : recorded_events_(recorded_events) {}

  void AddEvents(
      const rpc::events::AddEventRequest &request,
      const rpc::ClientCallback<rpc::events::AddEventReply> &callback) override {
    for (const auto &event : request.events_data().events()) {
      recorded_events_.push_back(event);
    }
    callback(Status::OK(), rpc::events::AddEventReply{});
  }

 private:
  std::vector<rpc::events::RayEvent> &recorded_events_;
};

class RayJobEventRecorderTest : public ::testing::Test {
 public:
  RayJobEventRecorderTest() {
    mock_client_ = std::make_unique<MockEventAggregatorClient>(recorded_events_);
    recorder_ = std::make_unique<RayJobEventRecorder>(std::move(mock_client_));
  }

  void SetUp() override {
    // Clean up recorded events before each test
    recorded_events_.clear();
  }

 protected:
  std::vector<rpc::events::RayEvent> recorded_events_;
  std::unique_ptr<MockEventAggregatorClient> mock_client_;
  std::unique_ptr<RayJobEventRecorder> recorder_;
};

TEST_F(RayJobEventRecorderTest, TestRecordEvents) {
  rpc::JobTableData data1;
  data1.set_job_id("test_job_id_1");
  data1.set_is_dead(false);
  data1.set_driver_pid(12345);
  data1.set_start_time(absl::ToUnixSeconds(absl::Now()));
  data1.set_end_time(0);
  data1.set_entrypoint("python test_script.py");
  data1.set_driver_ip_address("127.0.0.1");

  rpc::JobTableData data2;
  data2.set_job_id("test_job_id_2");
  data2.set_is_dead(true);
  data2.set_driver_pid(67890);
  data2.set_start_time(absl::ToUnixSeconds(absl::Now()) - 3600);  // 1 hour ago
  data2.set_end_time(absl::ToUnixSeconds(absl::Now()));
  data2.set_entrypoint("python another_script.py");
  data2.set_driver_ip_address("192.168.1.100");

  std::vector<rpc::JobTableData> data_list = {data1, data2};
  recorder_->RecordEvents(data_list);

  ASSERT_EQ(recorded_events_.size(), 2);

  // Verify first event
  ASSERT_EQ(recorded_events_[0].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[0].event_type(), rpc::events::RayEvent::DRIVER_JOB_EVENT);
  ASSERT_EQ(recorded_events_[0].severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(recorded_events_[0].message(), "driver job event");
  ASSERT_TRUE(recorded_events_[0].has_driver_job_event());
  ASSERT_EQ(recorded_events_[0].driver_job_event().job_id(), "test_job_id_1");
  ASSERT_EQ(recorded_events_[0].driver_job_event().driver_ip_address(), "127.0.0.1");
  ASSERT_EQ(recorded_events_[0].driver_job_event().driver_pid(), 12345);
  ASSERT_EQ(recorded_events_[0].driver_job_event().entrypoint(), "python test_script.py");

  // Verify second event
  ASSERT_EQ(recorded_events_[1].source_type(), rpc::events::RayEvent::GCS);
  ASSERT_EQ(recorded_events_[1].event_type(), rpc::events::RayEvent::DRIVER_JOB_EVENT);
  ASSERT_EQ(recorded_events_[1].severity(), rpc::events::RayEvent::INFO);
  ASSERT_EQ(recorded_events_[1].message(), "driver job event");
  ASSERT_TRUE(recorded_events_[1].has_driver_job_event());
  ASSERT_EQ(recorded_events_[1].driver_job_event().job_id(), "test_job_id_2");
  ASSERT_EQ(recorded_events_[1].driver_job_event().driver_ip_address(), "192.168.1.100");
  ASSERT_EQ(recorded_events_[1].driver_job_event().driver_pid(), 67890);
  ASSERT_EQ(recorded_events_[1].driver_job_event().entrypoint(),
            "python another_script.py");
}

}  // namespace telemetry
}  // namespace ray
