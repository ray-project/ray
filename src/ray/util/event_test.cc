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
#include <boost/filesystem.hpp>
#include <fstream>
#include <set>
#include <thread>
#include "gtest/gtest.h"

namespace ray {

class TestEventReporter : public BaseEventReporter {
 public:
  virtual void Init() override {}
  virtual void Report(const rpc::Event &event) override { event_list.push_back(event); }
  virtual void Close() override {}
  virtual ~TestEventReporter() {}
  virtual std::string GetReporterKey() override { return "test.event.reporter"; }

 public:
  static std::vector<rpc::Event> event_list;
};

std::vector<rpc::Event> TestEventReporter::event_list = std::vector<rpc::Event>();

void CheckEventDetail(rpc::Event &event, std::string job_id, std::string node_id,
                      std::string task_id, std::string source_type, std::string severity,
                      std::string label, std::string message) {
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

rpc::Event GetEventFromString(std::string seq) {
  std::stringstream ss;
  ss << seq << '\n';
  rpc::Event event;
  boost::property_tree::ptree pt;
  boost::property_tree::read_json(ss, pt);

  std::vector<std::string> splitArray;
  for (auto it = pt.begin(); it != pt.end(); ++it) {
    splitArray.push_back(it->first);
    splitArray.push_back(it->second.get_value<std::string>());
  }

  auto pt_custom_fields = pt.get_child_optional("custom_fields");
  for (auto it = pt_custom_fields->begin(); it != pt_custom_fields->end(); ++it) {
    splitArray.push_back(it->first);
    splitArray.push_back(it->second.get_value<std::string>());
  }

  EXPECT_EQ(splitArray[2], "severity");
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(splitArray[3], &severity_ele));
  event.set_severity(severity_ele);
  EXPECT_EQ(splitArray[4], "label");
  event.set_label(splitArray[5]);
  EXPECT_EQ(splitArray[6], "event_id");
  event.set_event_id(splitArray[7]);
  EXPECT_EQ(splitArray[8], "source_type");
  rpc::Event_SourceType source_type_ele = rpc::Event_SourceType::
      Event_SourceType_Event_SourceType_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_SourceType_Parse(splitArray[9], &source_type_ele));
  event.set_source_type(source_type_ele);
  EXPECT_EQ(splitArray[10], "host_name");
  event.set_source_hostname(splitArray[11]);
  EXPECT_EQ(splitArray[12], "pid");
  event.set_source_pid(std::stoi(splitArray[13].c_str()));
  EXPECT_EQ(splitArray[14], "message");
  event.set_message(splitArray[15]);
  EXPECT_EQ(splitArray[16], "custom_fields");
  std::unordered_map<std::string, std::string> custom_fields;
  for (int i = 18, len = splitArray.size(); i < len; i += 2) {
    custom_fields[splitArray[i]] = splitArray[i + 1];
  }
  event.mutable_custom_fields()->insert(custom_fields.begin(), custom_fields.end());
  return event;
}

void ParallelRunning(int nthreads, int loop_times,
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
          (t + 1) == nthreads ? loop_times : (t + 1) * loop_times / nthreads, t));
    }
    std::for_each(threads.begin(), threads.end(), [](std::thread &x) { x.join(); });
  } else {
    event_context_init();
    for (int loop_i = 0; loop_i < loop_times; loop_i++) {
      loop_function(loop_i);
    }
  }
}

void ReadEventFromFile(std::vector<std::string> &vc, std::string log_file) {
  std::string line;
  std::ifstream read_file;
  read_file.open(log_file, std::ios::binary);
  while (std::getline(read_file, line)) {
    vc.push_back(line);
  }
  read_file.close();
}

std::string GenerateLogDir() {
  std::string log_dir_generate = std::string(5, ' ');
  FillRandom(&log_dir_generate);
  std::string log_dir = "event" + StringToHex(log_dir_generate);
  return log_dir;
}

TEST(EVENT_TEST, TEST_BASIC) {
  TestEventReporter::event_list.clear();
  ray::EventManager::Instance().ClearReporters();

  RAY_EVENT(WARNING, "label") << "test for empty reporters";

  // If there are no reporters, it would not Publish event
  EXPECT_EQ(TestEventReporter::event_list.size(), 0);

  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());

  RAY_EVENT(WARNING, "label 0") << "send message 0";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER,
      std::unordered_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  RAY_EVENT(INFO, "label 1") << "send message 1";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      std::unordered_map<std::string, std::string>(
          {{"node_id", "node 2"}, {"job_id", "job 2"}}));
  RAY_EVENT(ERROR, "label 2") << "send message 2 "
                              << "send message again";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS);
  RAY_EVENT(FATAL, "") << "";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result.size(), 4);

  CheckEventDetail(result[0], "", "", "", "COMMON", "WARNING", "label 0",
                   "send message 0");

  CheckEventDetail(result[1], "job 1", "node 1", "task 1", "CORE_WORKER", "INFO",
                   "label 1", "send message 1");

  CheckEventDetail(result[2], "job 2", "node 2", "", "RAYLET", "ERROR", "label 2",
                   "send message 2 send message again");

  CheckEventDetail(result[3], "", "", "", "GCS", "FATAL", "", "");
}

TEST(EVENT_TEST, LOG_ONE_THREAD) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      std::unordered_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir));

  int print_times = 1000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  std::vector<std::string> vc;
  ReadEventFromFile(vc, log_dir + "/event_RAYLET.log");

  EXPECT_EQ((int)vc.size(), 1000);

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    rpc::Event ele = GetEventFromString(vc[len - print_times + i]);
    CheckEventDetail(ele, "job 1", "node 1", "task 1", "RAYLET", "INFO",
                     "label " + std::to_string(i + 1),
                     "send message " + std::to_string(i + 1));
  }

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, LOG_MULTI_THREAD) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();

  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_GCS, log_dir));
  int nthreads = 80;
  int print_times = 1000;

  ParallelRunning(
      nthreads, print_times,
      []() {
        ray::RayEventContext::Instance().SetEventContext(
            rpc::Event_SourceType::Event_SourceType_GCS,
            std::unordered_map<std::string, std::string>(
                {{"node_id", "node 2"}, {"job_id", "job 2"}, {"task_id", "task 2"}}));
      },
      [](int loop_i) {
        RAY_EVENT(WARNING, "label " + std::to_string(loop_i))
            << "send message " + std::to_string(loop_i);
      });

  std::vector<std::string> vc;
  ReadEventFromFile(vc, log_dir + "/event_GCS.log");

  std::set<std::string> label_set;
  std::set<std::string> message_set;

  for (int i = 0, len = vc.size(); i < print_times; ++i) {
    rpc::Event ele = GetEventFromString(vc[len - print_times + i]);
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

  boost::filesystem::remove_all(log_dir.c_str());
}

TEST(EVENT_TEST, LOG_ROTATE) {
  std::string log_dir = GenerateLogDir();

  ray::EventManager::Instance().ClearReporters();
  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET,
      std::unordered_map<std::string, std::string>(
          {{"node_id", "node 1"}, {"job_id", "job 1"}, {"task_id", "task 1"}}));

  ray::EventManager::Instance().AddReporter(std::make_shared<LogEventReporter>(
      rpc::Event_SourceType::Event_SourceType_RAYLET, log_dir, true, 1, 20));

  int print_times = 100000;
  for (int i = 1; i <= print_times; ++i) {
    RAY_EVENT(INFO, "label " + std::to_string(i)) << "send message " + std::to_string(i);
  }

  int cnt = 0;
  for (auto &entry :
       boost::make_iterator_range(boost::filesystem::directory_iterator(log_dir), {})) {
    if (entry.path().string().find("event_RAYLET") != std::string::npos) {
      cnt++;
    }
  }

  EXPECT_EQ(cnt, 21);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
