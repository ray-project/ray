// Copyright 2017 The Ray Authors.
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

#include "ray/util/event.h"

#include <boost/range.hpp>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/util/event_label.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

class TestEventReporter : public BaseEventReporter {
 public:
  virtual void Init() override {}
  virtual void Report(const rpc::Event &event, const json &custom_fields) override {
    event_list.push_back(event);
  }
  virtual void ReportExportEvent(const rpc::ExportEvent &export_event) override {}
  virtual void Close() override {}
  virtual ~TestEventReporter() {}
  virtual std::string GetReporterKey() override { return "test.event.reporter"; }

 public:
  static std::vector<rpc::Event> event_list;
};

std::vector<rpc::Event> TestEventReporter::event_list = std::vector<rpc::Event>();

void CheckEventDetail(rpc::Event &event,
                      std::string job_id,
                      std::string node_id,
                      std::string task_id,
                      std::string source_type,
                      std::string severity,
                      std::string label,
                      std::string message) {
  int custom_key_num = 0;
  auto mp = (*event.mutable_custom_fields());

  if (job_id != "") {
    EXPECT_EQ(mp["job_id"], job_id);
    custom_key_num++;
  }
  if (node_id != "") {
    EXPECT_EQ(mp["node_id"], node_id);
    custom_key_num++;
  }
  if (task_id != "") {
    EXPECT_EQ(mp["task_id"], task_id);
    custom_key_num++;
  }
  EXPECT_EQ(mp.size(), custom_key_num);
  EXPECT_EQ(rpc::Event_SourceType_Name(event.source_type()), source_type);
  EXPECT_EQ(rpc::Event_Severity_Name(event.severity()), severity);

  if (label != "NULL") {
    EXPECT_EQ(event.label(), label);
  }

  if (message != "NULL") {
    EXPECT_EQ(event.message(), message);
  }

  EXPECT_EQ(event.source_pid(), getpid());
}

rpc::Event GetEventFromString(std::string seq, json *custom_fields) {
  rpc::Event event;
  json j = json::parse(seq);

  for (auto const &pair : j.items()) {
    if (pair.key() == "severity") {
      rpc::Event_Severity severity_ele =
          rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
      RAY_CHECK(
          rpc::Event_Severity_Parse(pair.value().get<std::string>(), &severity_ele));
      event.set_severity(severity_ele);
    } else if (pair.key() == "label") {
      event.set_label(pair.value().get<std::string>());
    } else if (pair.key() == "event_id") {
      event.set_event_id(pair.value().get<std::string>());
    } else if (pair.key() == "source_type") {
      rpc::Event_SourceType source_type_ele = rpc::Event_SourceType::
          Event_SourceType_Event_SourceType_INT_MIN_SENTINEL_DO_NOT_USE_;
      RAY_CHECK(
          rpc::Event_SourceType_Parse(pair.value().get<std::string>(), &source_type_ele));
      event.set_source_type(source_type_ele);
    } else if (pair.key() == "host_name") {
      event.set_source_hostname(pair.value().get<std::string>());
    } else if (pair.key() == "pid") {
      event.set_source_pid(std::stoi(pair.value().get<std::string>().c_str()));
    } else if (pair.key() == "message") {
      event.set_message(pair.value().get<std::string>());
    }
  }

  absl::flat_hash_map<std::string, std::string> mutable_custom_fields;
  *custom_fields = j["custom_fields"];
  for (auto const &pair : (*custom_fields).items()) {
    if (pair.key() == "job_id" || pair.key() == "node_id" || pair.key() == "task_id") {
      mutable_custom_fields[pair.key()] = pair.value().get<std::string>();
    }
  }
  event.mutable_custom_fields()->insert(mutable_custom_fields.begin(),
                                        mutable_custom_fields.end());
  return event;
}

void ParallelRunning(int nthreads,
                     int loop_times,
                     std::function<void()> event_context_init,
                     std::function<void(int)> loop_function) {
  if (nthreads > 1) {
    std::vector<std::thread> threads(nthreads);
    for (int t = 0; t < nthreads; t++) {
      threads[t] = std::thread(std::bind(
          [&](const int bi, const int ei, const int t) {
            event_context_init();
            for (int loop_i = bi; loop_i < ei; loop_i++) {
              loop_function(loop_i);
            }
          },
          t * loop_times / nthreads,
          (t + 1) == nthreads ? loop_times : (t + 1) * loop_times / nthreads,
          t));
    }
    std::for_each(threads.begin(), threads.end(), [](std::thread &x) { x.join(); });
  } else {
    event_context_init();
    for (int loop_i = 0; loop_i < loop_times; loop_i++) {
      loop_function(loop_i);
    }
  }
}

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

std::string GenerateLogDir() {
  std::string log_dir_generate = std::string(5, ' ');
  FillRandom(&log_dir_generate);
  std::string log_dir = "event" + StringToHex(log_dir_generate);
  return log_dir;
}

class EventTest : public ::testing::Test {
 public:
  virtual void SetUp() { log_dir = GenerateLogDir(); }

  virtual void TearDown() {
    TestEventReporter::event_list.clear();
    std::filesystem::remove_all(log_dir.c_str());
    EventManager::Instance().ClearReporters();
    ray::RayEventContext::Instance().ResetEventContext();
  }

  std::string log_dir;
};

TEST_F(EventTest, TestBasic) {
  RAY_EVENT(WARNING, "label") << "test for empty reporters";

  // If there are no reporters, it would not Publish event
  EXPECT_EQ(TestEventReporter::event_list.size(), 0);

  EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());

  RAY_EVENT(WARNING, "label 0") << "send message 0";

  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  RAY_EVENT(INFO, "label 1") << "send message 1";

  RayEventContext::Instance().ResetEventContext();

  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 2"}, {"job_id", "job 2"}}));
  RAY_EVENT(ERROR, "label 2") << "send message 2 "
                              << "send message again";

  RayEventContext::Instance().ResetEventContext();

  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS);
  RAY_EVENT(FATAL, "") << "";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 4);

  CheckEventDetail(
      result[0], "", "", "", "COMMON", "WARNING", "label 0", "send message 0");

  CheckEventDetail(result[1],
                   "job 1",
                   "node 1",
                   "task 1",
                   "CORE_WORKER",
                   "INFO",
                   "label 1",
                   "send message 1");

  CheckEventDetail(result[2],
                   "job 2",
                   "node 2",
                   "",
                   "RAYLET",
                   "ERROR",
                   "label 2",
                   "send message 2 send message again");

  CheckEventDetail(result[3], "", "", "", "GCS", "FATAL", "", "");
}

TEST_F(EventTest, TestUpdateCustomFields) {
  EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());

  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}}));

  RAY_EVENT(INFO, "label 1") << "send message 1";

  // Replace the value of existing key: "job_id"
  // Insert new item of key: "task_id"
  RayEventContext::Instance().UpdateCustomFields(
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 2"}, {"task_id", "task 2"}}));
  RAY_EVENT(ERROR, "label 2") << "send message 2 "
                              << "send message again";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 2);

  CheckEventDetail(result[0],
                   "job 1",
                   "node 1",
                   "",
                   "CORE_WORKER",
                   "INFO",
                   "label 1",
                   "send message 1");

  CheckEventDetail(result[1],
                   "job 2",
                   "node 1",
                   "task 2",
                   "CORE_WORKER",
                   "ERROR",
                   "label 2",
                   "send message 2 send message again");
}

TEST_F(EventTest, TestLogOneThread) {
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  int print_times = 1000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir + "/event_RAYLET.log");

  EXPECT_EQ((int)vc.size(), 1000);

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    json custom_fields;
    rpc::Event ele = GetEventFromString(vc[len - print_times + i], &custom_fields);
    CheckEventDetail(ele,
                     "job 1",
                     "node 1",
                     "task 1",
                     "RAYLET",
                     "INFO",
                     "label " + std::to_string(i + 1),
                     "send message " + std::to_string(i + 1));
  }
}

TEST_F(EventTest, TestMultiThreadContextCopy) {
  ray::RayEventContext::Instance().ResetEventContext();
  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RAY_EVENT(INFO, "label 0") << "send message 0";

  std::thread private_thread = std::thread(std::bind([&]() {
    auto custom_fields = absl::flat_hash_map<std::string, std::string>();
    custom_fields.emplace("node_id", "node 1");
    custom_fields.emplace("job_id", "job 1");
    custom_fields.emplace("task_id", "task 1");
    ray::RayEventContext::Instance().SetEventContext(
        rpc::Event_SourceType::Event_SourceType_GCS, custom_fields);
    RAY_EVENT(INFO, "label 2") << "send message 2";
  }));

  private_thread.join();

  RAY_EVENT(INFO, "label 1") << "send message 1";
  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 3);
  CheckEventDetail(result[0], "", "", "", "COMMON", "INFO", "label 0", "send message 0");
  CheckEventDetail(
      result[1], "job 1", "node 1", "task 1", "GCS", "INFO", "label 2", "send message 2");
  CheckEventDetail(
      result[2], "job 1", "node 1", "task 1", "GCS", "INFO", "label 1", "send message 1");

  ray::RayEventContext::Instance().ResetEventContext();
  TestEventReporter::event_list.clear();

  std::thread private_thread_2 = std::thread(std::bind([&]() {
    ray::RayEventContext::Instance().SetSourceType(
        rpc::Event_SourceType::Event_SourceType_RAYLET);
    ray::RayEventContext::Instance().UpdateCustomField("job_id", "job 1");
    RAY_EVENT(INFO, "label 2") << "send message 2";
  }));

  private_thread_2.join();

  RAY_EVENT(INFO, "label 3") << "send message 3";

  EXPECT_EQ(result.size(), 2);
  CheckEventDetail(
      result[0], "job 1", "", "", "RAYLET", "INFO", "label 2", "send message 2");
  CheckEventDetail(result[1], "", "", "", "COMMON", "INFO", "label 3", "send message 3");
}

TEST_F(EventTest, TestLogMultiThread) {
  EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_GCS, log_dir));
  int nthreads = 80;
  int print_times = 1000;

  ParallelRunning(
      nthreads,
      print_times,
      []() {
        RayEventContext::Instance().SetEventContext(
            rpc::Event_SourceType::Event_SourceType_GCS,
            absl::flat_hash_map<std::string, std::string>(
                {{"node_id", "node 2"}, {"job_id", "job 2"}, {"task_id", "task 2"}}));
      },
      [](int loop_i) {
        RAY_EVENT(WARNING, "label " + std::to_string(loop_i))
            << "send message " + std::to_string(loop_i);
      });

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir + "/event_GCS.log");

  std::set<std::string> label_set;
  std::set<std::string> message_set;

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    json custom_fields;
    rpc::Event ele = GetEventFromString(vc[len - print_times + i], &custom_fields);
    CheckEventDetail(ele, "job 2", "node 2", "task 2", "GCS", "WARNING", "NULL", "NULL");
    message_set.insert(ele.message());
    label_set.insert(ele.label());
  }

  EXPECT_EQ(message_set.size(), print_times);
  EXPECT_EQ(*(message_set.begin()), "send message 0");
  EXPECT_EQ(*(--message_set.end()), "send message " + std::to_string(print_times - 1));
  EXPECT_EQ(label_set.size(), print_times);
  EXPECT_EQ(*(label_set.begin()), "label 0");
  EXPECT_EQ(*(--label_set.end()), "label " + std::to_string(print_times - 1));
}

TEST_F(EventTest, TestLogRotate) {
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir, true, 1, 20));

  int print_times = 100000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  int cnt = 0;
  for (auto &entry :
       boost::make_iterator_range(std::filesystem::directory_iterator(log_dir), {})) {
    if (entry.path().string().find("event_RAYLET") != std::string::npos) {
      cnt++;
    }
  }

  EXPECT_EQ(cnt, 21);
}

TEST_F(EventTest, TestWithField) {
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      absl::flat_hash_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  RAY_EVENT(INFO, "label 1")
          .WithField("string", "test string")
          .WithField("int", 123)
          .WithField("double", 0.123)
          .WithField("bool", true)
      << "send message 1";

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir + "/event_RAYLET.log");

  EXPECT_EQ((int)vc.size(), 1);

  json custom_fields;
  rpc::Event ele = GetEventFromString(vc[0], &custom_fields);
  CheckEventDetail(
      ele, "job 1", "node 1", "task 1", "RAYLET", "INFO", "label 1", "send message 1");
  auto string_value = custom_fields["string"].get<std::string>();
  EXPECT_EQ(string_value, "test string");
  auto int_value = custom_fields["int"].get<int>();
  EXPECT_EQ(int_value, 123);
  auto double_value = custom_fields["double"].get<double>();
  EXPECT_EQ(double_value, 0.123);
  auto bool_value = custom_fields["bool"].get<bool>();
  EXPECT_EQ(bool_value, true);
}

TEST_F(EventTest, TestExportEvent) {
  std::vector<SourceTypeVariant> source_types = {
      rpc::ExportEvent_SourceType::ExportEvent_SourceType_EXPORT_TASK,
      rpc::Event_SourceType::Event_SourceType_RAYLET};
  RayEventInit_(source_types,
                absl::flat_hash_map<std::string, std::string>(),
                log_dir,
                "warning",
                false);

  std::shared_ptr<rpc::ExportTaskEventData> task_event_ptr =
      std::make_shared<rpc::ExportTaskEventData>();
  task_event_ptr->set_task_id("task_id0");
  task_event_ptr->set_attempt_number(1);
  task_event_ptr->set_job_id("job_id0");

  std::string export_event_data_str;
  google::protobuf::util::JsonPrintOptions options;
  options.preserve_proto_field_names = true;
  RAY_CHECK(google::protobuf::util::MessageToJsonString(
                *task_event_ptr, &export_event_data_str, options)
                .ok());
  json event_data_as_json = json::parse(export_event_data_str);

  RayExportEvent(task_event_ptr).SendEvent();
  // Verify this event doesn't show up in the event_EXPORT_TASK_123.log file.
  // It should only show up in the event_RAYLET.log file.
  RAY_EVENT(WARNING, "label") << "test warning";

  std::vector<std::string> vc;
  ReadContentFromFile(
      vc,
      log_dir + "/export_events/event_EXPORT_TASK_" + std::to_string(getpid()) + ".log");

  EXPECT_EQ((int)vc.size(), 1);

  std::cout << vc[0];
  json export_event_as_json = json::parse(vc[0]);
  EXPECT_EQ(export_event_as_json["source_type"].get<std::string>(), "EXPORT_TASK");
  EXPECT_EQ(export_event_as_json.contains("event_id"), true);
  EXPECT_EQ(export_event_as_json.contains("timestamp"), true);
  EXPECT_EQ(export_event_as_json.contains("event_data"), true);
  // Fields that shouldn't exist for export events but do exist for standard events
  EXPECT_EQ(export_event_as_json.contains("severity"), false);
  EXPECT_EQ(export_event_as_json.contains("label"), false);
  EXPECT_EQ(export_event_as_json.contains("message"), false);

  json event_data = export_event_as_json["event_data"].get<json>();
  EXPECT_EQ(event_data, event_data_as_json);

  // Verify "test warning" event was written to event_RAYLET.log file
  std::vector<std::string> vc1;
  ReadContentFromFile(vc1, log_dir + "/events/event_RAYLET.log");
  EXPECT_EQ((int)vc1.size(), 1);
  json raylet_event_as_json = json::parse(vc1[0]);
  EXPECT_EQ(raylet_event_as_json["source_type"].get<std::string>(), "RAYLET");
  EXPECT_EQ(raylet_event_as_json["message"].get<std::string>(), "test warning");
}

TEST_F(EventTest, TestRayCheckAbort) {
  auto custom_fields = absl::flat_hash_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  // RayEventInit(rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields, log_dir,
  // "info", true);
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, custom_fields);
  EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  RAY_CHECK(1 > 0) << "correct test case";

  ASSERT_DEATH({ RAY_CHECK(1 < 0) << "incorrect test case"; }, "");

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir + "/event_RAYLET.log");
  json out_custom_fields;
  rpc::Event ele_1 = GetEventFromString(vc.back(), &out_custom_fields);

  CheckEventDetail(ele_1,
                   "job 1",
                   "node 1",
                   "task 1",
                   "RAYLET",
                   "FATAL",
                   EL_RAY_FATAL_CHECK_FAILED,
                   "NULL");
  EXPECT_THAT(ele_1.message(),
              testing::HasSubstr("Check failed: 1 < 0 incorrect test case"));
  EXPECT_THAT(ele_1.message(), testing::HasSubstr("*** StackTrace Information ***"));
  EXPECT_THAT(ele_1.message(), testing::HasSubstr("ray::RayLog::~RayLog()"));
}

TEST_F(EventTest, TestRayEventInit) {
  auto custom_fields = absl::flat_hash_map<std::string, std::string>();
  custom_fields.emplace("node_id", "node 1");
  custom_fields.emplace("job_id", "job 1");
  custom_fields.emplace("task_id", "task 1");
  const std::vector<SourceTypeVariant> source_types = {
      rpc::Event_SourceType::Event_SourceType_RAYLET};
  RayEventInit_(source_types, custom_fields, log_dir, "warning", false);

  RAY_EVENT(FATAL, "label") << "test error event";

  std::vector<std::string> vc;
  ReadContentFromFile(vc, log_dir + "/events/event_RAYLET.log");
  EXPECT_EQ((int)vc.size(), 1);
  json out_custom_fields;
  rpc::Event ele_1 = GetEventFromString(vc.back(), &out_custom_fields);

  CheckEventDetail(
      ele_1, "job 1", "node 1", "task 1", "RAYLET", "FATAL", "label", "NULL");
}

TEST_F(EventTest, TestLogLevel) {
  EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER, {});
  EXPECT_EQ(TestEventReporter::event_list.size(), 0);

  // Test info level
  ray::RayEvent::SetLevel("info");
  RAY_EVENT(INFO, "label") << "test info";
  RAY_EVENT(WARNING, "label") << "test warning";
  RAY_EVENT(ERROR, "label") << "test error";
  RAY_EVENT(FATAL, "label") << "test fatal";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;
  EXPECT_EQ(result.size(), 4);
  CheckEventDetail(result[0], "", "", "", "CORE_WORKER", "INFO", "label", "test info");
  CheckEventDetail(
      result[1], "", "", "", "CORE_WORKER", "WARNING", "label", "test warning");
  CheckEventDetail(result[2], "", "", "", "CORE_WORKER", "ERROR", "label", "test error");
  CheckEventDetail(result[3], "", "", "", "CORE_WORKER", "FATAL", "label", "test fatal");
  result.clear();

  // Test warning level
  ray::RayEvent::SetLevel("warning");
  RAY_EVENT(INFO, "label") << "test info";
  RAY_EVENT(WARNING, "label") << "test warning";
  RAY_EVENT(ERROR, "label") << "test error";
  RAY_EVENT(FATAL, "label") << "test fatal";

  EXPECT_EQ(result.size(), 3);
  CheckEventDetail(
      result[0], "", "", "", "CORE_WORKER", "WARNING", "label", "test warning");
  CheckEventDetail(result[1], "", "", "", "CORE_WORKER", "ERROR", "label", "test error");
  CheckEventDetail(result[2], "", "", "", "CORE_WORKER", "FATAL", "label", "test fatal");
  result.clear();

  // Test error level
  ray::RayEvent::SetLevel("error");
  RAY_EVENT(INFO, "label") << "test info";
  RAY_EVENT(WARNING, "label") << "test warning";
  RAY_EVENT(ERROR, "label") << "test error";
  RAY_EVENT(FATAL, "label") << "test fatal";

  EXPECT_EQ(result.size(), 2);
  CheckEventDetail(result[0], "", "", "", "CORE_WORKER", "ERROR", "label", "test error");
  CheckEventDetail(result[1], "", "", "", "CORE_WORKER", "FATAL", "label", "test fatal");
  result.clear();

  // Test fatal level
  ray::RayEvent::SetLevel("FATAL");
  RAY_EVENT(INFO, "label") << "test info";
  RAY_EVENT(WARNING, "label") << "test warning";
  RAY_EVENT(ERROR, "label") << "test error";
  RAY_EVENT(FATAL, "label") << "test fatal";

  EXPECT_EQ(result.size(), 1);
  CheckEventDetail(result[0], "", "", "", "CORE_WORKER", "FATAL", "label", "test fatal");
  result.clear();
}

TEST_F(EventTest, TestLogEvent) {
  ray::RayEvent::SetEmitToLogFile(true);
  // Initialize log level to error
  ray::RayLog::StartRayLog("event_test", ray::RayLogLevel::ERROR, log_dir);
  EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER, {});
  EXPECT_EQ(TestEventReporter::event_list.size(), 0);

  // Add some events. Only ERROR and FATAL events would be printed in general log.
  RAY_EVENT(INFO, "label") << "test info";
  RAY_EVENT(WARNING, "label") << "test warning";
  RAY_EVENT(ERROR, "label") << "test error";
  RAY_EVENT(FATAL, "label") << "test fatal";

  std::vector<std::string> vc;
  ReadContentFromFile(
      vc, log_dir + "/event_test_" + std::to_string(getpid()) + ".log", "[ Event ");
  EXPECT_EQ((int)vc.size(), 2);
  // Check ERROR event
  EXPECT_THAT(vc[0], testing::HasSubstr(" E "));
  EXPECT_THAT(vc[0], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[0], testing::HasSubstr("test error"));
  // Check FATAL event. We convert fatal events to error logs.
  EXPECT_THAT(vc[1], testing::HasSubstr(" E "));
  EXPECT_THAT(vc[1], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[1], testing::HasSubstr("test fatal"));

  std::filesystem::remove_all(log_dir.c_str());

  // Set log level smaller than event level.
  ray::RayLog::StartRayLog("event_test", ray::RayLogLevel::INFO, log_dir);
  ray::RayEvent::SetLevel("error");

  // Add some events. All events would be printed in general log.
  RAY_EVENT(INFO, "label") << "test info 2";
  RAY_EVENT(WARNING, "label") << "test warning 2";
  RAY_EVENT(ERROR, "label") << "test error 2";
  RAY_EVENT(FATAL, "label") << "test fatal 2";

  vc.clear();
  ReadContentFromFile(
      vc, log_dir + "/event_test_" + std::to_string(getpid()) + ".log", "[ Event ");
  EXPECT_EQ((int)vc.size(), 4);
  // Check INFO event
  EXPECT_THAT(vc[0], testing::HasSubstr(" I "));
  EXPECT_THAT(vc[0], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[0], testing::HasSubstr("test info 2"));
  // Check WARNING event
  EXPECT_THAT(vc[1], testing::HasSubstr(" W "));
  EXPECT_THAT(vc[1], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[1], testing::HasSubstr("test warning 2"));
  // Check ERROR event
  EXPECT_THAT(vc[2], testing::HasSubstr(" E "));
  EXPECT_THAT(vc[2], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[2], testing::HasSubstr("test error 2"));
  // Check FATAL event. We convert fatal events to error logs.
  EXPECT_THAT(vc[3], testing::HasSubstr(" E "));
  EXPECT_THAT(vc[3], testing::HasSubstr("Event"));
  EXPECT_THAT(vc[3], testing::HasSubstr("test fatal 2"));
  // Can't add this to teardown because they are friend  class.
  ray::RayEvent::SetEmitToLogFile(false);
  ray::RayEvent::SetLevel("info");
}

TEST_F(EventTest, VerifyOnlyNthOccurenceEventLogged) {
  EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  const std::string kLogStr = "this is a test log";
  auto start_time = std::chrono::steady_clock::now().time_since_epoch();
  // printed 30, 60, 90, 120ms 4 times.
  while (std::chrono::steady_clock::now().time_since_epoch() - start_time <
         std::chrono::milliseconds(100)) {
    RAY_EVENT_EVERY_MS(INFO, "label", 30) << kLogStr;
  }

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 4);
  // RAY_LOG(INFO) << result[0];

  for (auto &event_log : result) {
    CheckEventDetail(event_log, "", "", "", "COMMON", "INFO", "label", kLogStr);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Use ERROR type logger by default to avoid printing large scale logs in current test.
  ray::RayLog::StartRayLog("event_test", ray::RayLogLevel::ERROR);
  return RUN_ALL_TESTS();
}
