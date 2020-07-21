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

#include <fstream>
#include <set>
#include <thread>
#include "ray/util/event.h"

#include "gtest/gtest.h"

namespace ray {

class TestEventReporter : public LogBasedEventReporter {
 public:
  TestEventReporter():LogBasedEventReporter(rpc::Event_SourceType::Event_SourceType_CORE_WORKER,"test.txt") {}
  static std::vector<rpc::Event> event_list;
  virtual void Init() override {}
  virtual void Report(rpc::Event &event) override { event_list.push_back(event); }
  virtual void Close() override {}
  virtual ~TestEventReporter() {}

  virtual std::string GetReporterKey() override { return "test.event.reporter"; }
};

std::vector<rpc::Event> TestEventReporter::event_list = std::vector<rpc::Event>();

rpc::Event GetEventFromString(std::string seq) {
  rpc::Event event;
  std::string tmp;
  std::vector<std::string> splitArray;
  for (int i = 0, len = seq.length(); i < len; ++i) {
    tmp += seq[i];
    if (tmp.size() >= 3 && tmp.substr(tmp.size() - 3, 3) == "|||") {
      splitArray.push_back(tmp.substr(0, tmp.size() - 3));
      tmp.clear();
    }
  }
  splitArray.push_back(tmp);
  event.set_event_id(splitArray[1]);
  event.set_job_id(splitArray[2]);
  event.set_node_id(splitArray[3]);
  event.set_task_id(splitArray[4]);
  rpc::Event_SourceType source_type_ele = rpc::Event_SourceType::
      Event_SourceType_Event_SourceType_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_SourceType_Parse(splitArray[5], &source_type_ele));
  event.set_source_type(source_type_ele);
  event.set_source_hostname(splitArray[6]);
  event.set_source_pid(std::stoi(splitArray[7].c_str()));
  rpc::Event_Severity severity_ele =
      rpc::Event_Severity::Event_Severity_Event_Severity_INT_MIN_SENTINEL_DO_NOT_USE_;
  RAY_CHECK(rpc::Event_Severity_Parse(splitArray[8], &severity_ele));
  event.set_severity(severity_ele);
  event.set_label(splitArray[9]);
  event.set_message(splitArray[10]);
  return event;
}

TEST(EVENT_TEST, TEST_BASIC) {
  ray::EventManager::Instance().ClearReporters();
  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  TestEventReporter::event_list.clear();

  RAY_EVENT(WARNING, "label 0") << "send message 0";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_CORE_WORKER, "node1", "job1", "task 1");

  RAY_EVENT(INFO, "label 1") << "send message 1";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_RAYLET, "node2", "job2");
  RAY_EVENT(ERROR, "label 2") << "send message 2 "
                              << "send message 3 ";

  ray::RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS);
  RAY_EVENT(INFO, "label 3") << "send message 3";

  ray::EventManager::Instance().ClearReporters();
  RayEventContext::Instance().SetEventContext(
      rpc::Event_SourceType::Event_SourceType_GCS);
  ray::EventManager::Instance().AddReporter(std::make_shared<TestEventReporter>());
  RAY_EVENT(INFO, "label 3") << "send message 3";

  std::vector<rpc::Event> &result = TestEventReporter::event_list;

  EXPECT_EQ(result[0].job_id(), "");
  EXPECT_EQ(result[0].node_id(), "");
  EXPECT_EQ(result[0].task_id(), "");
  EXPECT_EQ(rpc::Event_SourceType_Name(result[0].source_type()), "COMMON");
  EXPECT_EQ(rpc::Event_Severity_Name(result[0].severity()), "WARNING");
  EXPECT_EQ(result[0].message(), "send message 0");
  EXPECT_EQ(result[0].label(), "label 0");

  EXPECT_EQ(result[1].job_id(), "job1");
  EXPECT_EQ(result[1].node_id(), "node1");
  EXPECT_EQ(result[1].task_id(), "task 1");
  EXPECT_EQ(rpc::Event_SourceType_Name(result[1].source_type()), "CORE_WORKER");
  EXPECT_EQ(rpc::Event_Severity_Name(result[1].severity()), "INFO");
  EXPECT_EQ(result[1].message(), "send message 1");
  EXPECT_EQ(result[1].label(), "label 1");

  EXPECT_EQ(result[2].job_id(), "job2");
  EXPECT_EQ(result[2].node_id(), "node2");
  EXPECT_EQ(result[2].task_id(), "");
  EXPECT_EQ(rpc::Event_SourceType_Name(result[2].source_type()), "RAYLET");
  EXPECT_EQ(rpc::Event_Severity_Name(result[2].severity()), "ERROR");
  EXPECT_EQ(result[2].message(), "send message 2 send message 3 ");
  EXPECT_EQ(result[2].label(), "label 2");
  EXPECT_EQ(result[2].source_pid(), getpid());

  EXPECT_EQ(result[3].job_id(), "");
  EXPECT_EQ(result[3].node_id(), "");
  EXPECT_EQ(result[3].task_id(), "");
  EXPECT_EQ(rpc::Event_SourceType_Name(result[3].source_type()), "GCS");
  EXPECT_EQ(rpc::Event_Severity_Name(result[3].severity()), "INFO");
  EXPECT_EQ(result[3].message(), "send message 3");
  EXPECT_EQ(result[3].label(), "label 3");
  EXPECT_EQ(result[3].source_pid(), getpid());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}