// Copyright 2024 The Ray Authors.
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

#include <google/protobuf/util/message_differencer.h>

#include <filesystem>
#include <fstream>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock/ray/gcs/gcs_client/gcs_client.h"
#include "ray/common/task/task_spec.h"
#include "ray/common/test_util.h"
#include "ray/core_worker/task_event_buffer.h"
#include "ray/util/event.h"

using ::testing::_;
using ::testing::Return;

namespace ray {

namespace core {

namespace worker {

class TaskEventTestWriteExport : public ::testing::Test {
 public:
  TaskEventTestWriteExport() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_report_interval_ms": 1000,
  "task_events_max_num_status_events_buffer_on_worker": 10,
  "task_events_max_num_profile_events_buffer_on_worker": 5,
  "task_events_send_batch_size": 100,
  "export_task_events_write_batch_size": 1,
  "task_events_max_num_export_status_events_buffer_on_worker": 15,
  "enable_export_api_write": true
}
  )");

    task_event_buffer_ = std::make_unique<TaskEventBufferImpl>(
        std::make_unique<ray::gcs::MockGcsClient>());
  }

  virtual void SetUp() { RAY_CHECK_OK(task_event_buffer_->Start(/*auto_flush*/ false)); }

  virtual void TearDown() {
    if (task_event_buffer_) task_event_buffer_->Stop();
    std::filesystem::remove_all(log_dir_.c_str());
  };

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  std::unique_ptr<TaskEvent> GenStatusTaskEvent(
      TaskID task_id,
      int32_t attempt_num,
      int64_t running_ts = 1,
      absl::optional<const TaskStatusEvent::TaskStateUpdate> state_update =
          absl::nullopt) {
    return std::make_unique<TaskStatusEvent>(task_id,
                                             JobID::FromInt(0),
                                             attempt_num,
                                             rpc::TaskStatus::RUNNING,
                                             running_ts,
                                             nullptr,
                                             state_update);
  }

  std::unique_ptr<TaskEventBufferImpl> task_event_buffer_ = nullptr;
  std::string log_dir_ = "event_123";
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

TEST_F(TaskEventTestWriteExport, TestWriteTaskExportEvents) {
  /*
  Test writing task events to event_EXPORT_TASK_123.log as part of the export API.
  This test verifies the following cases:
    1. Task events that are dropped from being sent to GCS (because more events
      added than task_events_max_num_status_events_buffer_on_worker) will still
      be written for the export API, if the number of events is less than
      task_events_max_num_export_status_events_buffer_on_worker.
    2. Task events over task_events_max_num_status_events_buffer_on_worker
      will be dropped and not written for the export API.
    3. Each Flush() call only writes a max of export_task_events_write_batch_size
      export events.
    In this test, 20 events are added which is greater than the max of 10 status events
    that can be stored in buffer. The max export status events in buffer is 15,
    so 15 events will be written to event_EXPORT_TASK_123.log. Each Flush()
    call will write 1 new event because the batch size is 1.
  */

  // {"task_events_max_num_status_events_buffer_on_worker": 10} and
  // {"task_events_max_num_export_status_events_buffer_on_worker": 15}
  // in TaskEventTestWriteExport so set num_events > 15.
  size_t num_events = 20;
  // Value of export_task_events_write_batch_size
  size_t batch_size = 1;
  // Value of task_events_max_num_export_status_events_buffer_on_worker
  size_t max_export_events_on_buffer = 15;
  auto task_ids = GenTaskIDs(num_events);
  google::protobuf::util::JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  options.always_print_primitive_fields = true;

  std::vector<SourceTypeVariant> source_types = {
      rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_TASK};
  RayEventInit_(source_types,
                absl::flat_hash_map<std::string, std::string>(),
                log_dir_,
                "warning",
                false);

  std::vector<std::unique_ptr<TaskEvent>> task_events;
  for (const auto &task_id : task_ids) {
    task_events.push_back(GenStatusTaskEvent(task_id, 0));
  }

  // Convert all task_events that were added with AddTaskEvent
  // to the ExportTaskEventData proto message
  std::vector<std::shared_ptr<rpc::ExportTaskEventData>> task_event_data_protos;
  for (const auto &task_event : task_events) {
    std::shared_ptr<rpc::ExportTaskEventData> event =
        std::make_shared<rpc::ExportTaskEventData>();
    task_event->ToRpcTaskExportEvents(event);
    task_event_data_protos.push_back(event);
  }

  // Add all num_events tasks
  for (auto &task_event : task_events) {
    task_event_buffer_->AddTaskEvent(std::move(task_event));
  }

  // Verify that batch_size events are being written for each flush
  std::vector<std::string> vc;
  for (int i = 0; i * batch_size < max_export_events_on_buffer; i++) {
    task_event_buffer_->FlushEvents(true);
    ReadContentFromFile(vc,
                        log_dir_ + "/export_events/event_EXPORT_TASK_" +
                            std::to_string(getpid()) + ".log");
    EXPECT_EQ((int)vc.size(), (i + 1) * batch_size);
    vc.clear();
  }

  // Verify that all max_export_events_on_buffer events are written to file even though
  // max_export_events_on_buffer > task_events_max_num_status_events_buffer_on_worker
  vc.clear();
  ReadContentFromFile(
      vc,
      log_dir_ + "/export_events/event_EXPORT_TASK_" + std::to_string(getpid()) + ".log");
  EXPECT_EQ((int)vc.size(), max_export_events_on_buffer);
  for (size_t i = 0; i < max_export_events_on_buffer; i++) {
    json export_event_as_json = json::parse(vc[i]);
    EXPECT_EQ(export_event_as_json["source_type"].get<std::string>(), "EXPORT_TASK");
    EXPECT_EQ(export_event_as_json.contains("event_id"), true);
    EXPECT_EQ(export_event_as_json.contains("timestamp"), true);
    EXPECT_EQ(export_event_as_json.contains("event_data"), true);

    json event_data = export_event_as_json["event_data"].get<json>();

    // The events written are the last max_export_events_on_buffer added
    // in AddTaskEvent because (num_events-max_export_events_on_buffer)
    // were dropped.
    std::string expected_event_data_str;
    RAY_CHECK(google::protobuf::util::MessageToJsonString(
                  *task_event_data_protos[i + (num_events - max_export_events_on_buffer)],
                  &expected_event_data_str,
                  options)
                  .ok());
    json expected_event_data = json::parse(expected_event_data_str);
    EXPECT_EQ(event_data, expected_event_data);
  }

  // Expect no more events.
  ASSERT_EQ(task_event_buffer_->GetNumTaskEventsStored(), 0);
}

}  // namespace worker

}  // namespace core

}  // namespace ray
