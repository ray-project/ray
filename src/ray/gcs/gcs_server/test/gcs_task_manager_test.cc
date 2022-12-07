// Copyright 2022 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_task_manager.h"

#include <google/protobuf/util/message_differencer.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

class GcsTaskManagerTest : public ::testing::Test {
 public:
  GcsTaskManagerTest() : task_manager(std::make_unique<GcsTaskManager>()) {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 1000
}
  )");
  }

  virtual void TearDown() { task_manager->Stop(); }

  std::vector<TaskID> GenTaskIDs(size_t num_tasks) {
    std::vector<TaskID> task_ids;
    for (size_t i = 0; i < num_tasks; ++i) {
      task_ids.push_back(RandomTaskId());
    }
    return task_ids;
  }

  std::unique_ptr<GcsTaskManager> task_manager;
};

class GcsTaskManagerMemoryLimitedTest : public GcsTaskManagerTest {
 public:
  GcsTaskManagerMemoryLimitedTest() : GcsTaskManagerTest() {
    RayConfig::instance().initialize(
        R"(
{
  "task_events_max_num_task_in_gcs": 100
}
  )");
  }
};

TEST_F(GcsTaskManagerTest, TestHandleAddTaskEventBasic) {
  size_t num_task_events = 100;
  int32_t num_dropped = 10;
  auto task_ids = GenTaskIDs(num_task_events);
  auto events_data = Mocker::GenTaskEventsData(task_ids, 0, num_dropped);

  rpc::AddTaskEventDataRequest request;
  rpc::AddTaskEventDataReply reply;
  std::promise<bool> promise;

  request.mutable_data()->CopyFrom(*events_data);

  task_manager->HandleAddTaskEventData(
      request, &reply, [&promise](Status, std::function<void()>, std::function<void()>) {
        promise.set_value(true);
      });

  promise.get_future().get();

  // Assert on RPC reply.
  ASSERT_EQ(StatusCode(reply.status().code()), StatusCode::OK);

  // Assert on actual data.
  ASSERT_EQ(task_manager->task_events_.size(), num_task_events);
  ASSERT_EQ(task_manager->total_num_task_events_reported_, num_task_events);
  ASSERT_EQ(task_manager->total_num_task_events_dropped_, num_dropped);
}

TEST_F(GcsTaskManagerMemoryLimitedTest, TestLimitTaskEvents) {
  size_t num_limit = 100;
  size_t num_batch1 = 100;
  size_t num_batch2 = 100;

  auto task_ids1 = GenTaskIDs(num_batch1);
  auto task_ids2 = GenTaskIDs(num_batch2);

  std::vector<std::vector<TaskID>> task_ids_sets = {task_ids1, task_ids2};

  for (auto const &task_ids : task_ids_sets) {
    rpc::AddTaskEventDataRequest request;
    rpc::AddTaskEventDataReply reply;
    std::promise<bool> promise;

    auto event_data = Mocker::GenTaskEventsData(task_ids);
    request.mutable_data()->CopyFrom(*event_data);

    task_manager->HandleAddTaskEventData(
        request,
        &reply,
        [&promise](Status, std::function<void()>, std::function<void()>) {
          promise.set_value(true);
        });

    promise.get_future().get();

    // Assert on RPC reply.
    ASSERT_EQ(StatusCode(reply.status().code()), StatusCode::OK);
  }

  // Assert on actual data.
  ASSERT_EQ(task_manager->task_events_.size(), num_limit);
  ASSERT_EQ(task_manager->total_num_task_events_reported_, num_batch1 + num_batch2);

  // Assert all later task events
  std::unordered_set<TaskID> task_ids2_set(task_ids2.begin(), task_ids2.end());
  for (auto &task_event : task_manager->task_events_) {
    EXPECT_EQ(task_ids2_set.count(TaskID::FromBinary(task_event.task_id())), 1);
  }
}

TEST_F(GcsTaskManagerTest, TestGetAllTaskEvents) {
  size_t num_task_events = 100;
  int32_t num_dropped = 10;
  auto task_ids = GenTaskIDs(num_task_events);
  auto expected_data = Mocker::GenTaskEventsData(task_ids, 0, num_dropped);

  SyncAddTaskEventData(expected_data);

  // Assert on the results get
  auto actual_data = SyncGetAllTaskEvents();
}

}  // namespace gcs
}  // namespace ray
