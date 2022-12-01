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
  auto task_ids = GenTaskIDs(num_task_events);
  auto events_data = Mocker::GenTaskStatusEvents(task_ids);

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
  ASSERT_EQ(reply.num_success(), num_task_events);
  ASSERT_EQ(reply.num_failure(), 0);

  // Assert on actual data.
  absl::MutexLock lock(&task_manager->mutex_);
  ASSERT_EQ(task_manager->task_events_.size(), num_task_events);
  ASSERT_EQ(task_manager->all_tasks_reported_.size(), num_task_events);
}

TEST_F(GcsTaskManagerTest, TestAddTaskEventMerge) {
  size_t num_task_events = 100;
  auto task_ids = GenTaskIDs(num_task_events);
  std::vector<rpc::TaskStatus> all_status = {rpc::TaskStatus::PENDING_ARGS_AVAIL,
                                             rpc::TaskStatus::RUNNING,
                                             rpc::TaskStatus::FINISHED};

  for (auto status : all_status) {
    auto event_data = Mocker::GenTaskStatusEvents(task_ids, status);

    rpc::AddTaskEventDataRequest request;
    rpc::AddTaskEventDataReply reply;
    std::promise<bool> promise;

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
    ASSERT_EQ(reply.num_success(), num_task_events);
    ASSERT_EQ(reply.num_failure(), 0);
  }

  // Assert on actual data, should only contain a single copy of tasks.
  absl::MutexLock lock(&task_manager->mutex_);
  ASSERT_EQ(task_manager->task_events_.size(), num_task_events);
  ASSERT_EQ(task_manager->all_tasks_reported_.size(), num_task_events);
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

    auto event_data = Mocker::GenTaskStatusEvents(task_ids);
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
    ASSERT_EQ(reply.num_success(), task_ids.size());
    ASSERT_EQ(reply.num_failure(), 0);
  }

  // Assert on actual data.
  absl::MutexLock lock(&task_manager->mutex_);
  ASSERT_EQ(task_manager->task_events_.size(), num_limit);
  ASSERT_EQ(task_manager->all_tasks_reported_.size(), num_batch1 + num_batch2);

  // Assert all later task events
  for (auto const &task_id : task_ids2) {
    EXPECT_EQ(task_manager->task_events_.count(task_id), 1);
  }
}

}  // namespace gcs
}  // namespace ray