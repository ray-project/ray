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
  EXPECT_EQ(event.label(), label);
  EXPECT_EQ(event.message(), message);
  EXPECT_EQ(event.source_pid(), getpid());
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

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
