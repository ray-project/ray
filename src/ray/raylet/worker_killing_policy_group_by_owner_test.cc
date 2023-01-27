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

#include "ray/raylet/worker_killing_policy_group_by_owner.h"

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/test/util.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

class WorkerKillingGroupByOwnerTest : public ::testing::Test {
 protected:
  instrumented_io_context io_context_;
  int32_t port_ = 2389;
  JobID job_id_ = JobID::FromInt(75);
  bool should_retry_ = true;
  bool should_not_retry_ = false;
  int32_t no_retry_ = 0;
  int32_t has_retry_ = 1;
  GroupByOwnerIdWorkerKillingPolicy worker_killing_policy_;

  std::shared_ptr<WorkerInterface> CreateActorCreationWorker(TaskID owner_id,
                                                             int32_t max_restarts) {
    rpc::TaskSpec message;
    message.set_task_id(TaskID::FromRandom(job_id_).Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.mutable_actor_creation_task_spec()->set_max_actor_restarts(max_restarts);
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    worker->AssignTaskId(task.GetTaskSpecification().TaskId());
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateTaskWorker(TaskID owner_id,
                                                    int32_t max_retries) {
    rpc::TaskSpec message;
    message.set_task_id(TaskID::FromRandom(job_id_).Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.set_max_retries(max_retries);
    message.set_type(ray::rpc::TaskType::NORMAL_TASK);
    TaskSpecification task_spec(message);
    RayTask task(task_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->SetAssignedTask(task);
    worker->AssignTaskId(task.GetTaskSpecification().TaskId());
    return worker;
  }
};

TEST_F(WorkerKillingGroupByOwnerTest, TestEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto worker_to_kill_and_should_retry_ =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  auto worker_to_kill = worker_to_kill_and_should_retry_.first;
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillingGroupByOwnerTest, TestLastWorkerInGroupShouldNotRetry) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  auto owner_id = TaskID::ForDriverTask(job_id_);
  auto first_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(owner_id, has_retry_);
  auto second_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(owner_id, has_retry_);

  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestNonRetriableBelongsToItsOwnGroupAndLIFOKill) {
  auto owner_id = TaskID::ForDriverTask(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(owner_id, no_retry_);
  auto second_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(owner_id, no_retry_);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));

  auto worker_to_kill_and_should_retry_ =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());

  auto worker_to_kill = worker_to_kill_and_should_retry_.first;
  bool retry = worker_to_kill_and_should_retry_.second;
  ASSERT_EQ(worker_to_kill->WorkerId(), second_submitted->WorkerId());
  ASSERT_EQ(retry, should_not_retry_);
}

TEST_F(WorkerKillingGroupByOwnerTest, TestGroupSortedByGroupSizeThenFirstSubmittedTask) {
  auto first_group_owner_id = TaskID::FromRandom(job_id_);
  auto second_group_owner_id = TaskID::FromRandom(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      first_group_owner_id, has_retry_);
  auto second_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(second_group_owner_id, has_retry_);
  auto third_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      second_group_owner_id, has_retry_);
  auto fourth_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      second_group_owner_id, has_retry_);
  auto fifth_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(first_group_owner_id, has_retry_);
  auto sixth_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(first_group_owner_id, has_retry_);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(fourth_submitted, should_retry_));
  expected.push_back(std::make_pair(sixth_submitted, should_retry_));
  expected.push_back(std::make_pair(third_submitted, should_retry_));
  expected.push_back(std::make_pair(fifth_submitted, should_retry_));
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestGroupSortedByRetriableLifo) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  auto second_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), has_retry_);
  auto third_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));
  expected.push_back(std::make_pair(third_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest,
       TestMultipleNonRetriableTaskSameGroupAndNotRetried) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  auto second_submitted = WorkerKillingGroupByOwnerTest::CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));

  for (const auto &entry : expected) {
    auto worker_to_kill_and_should_retry_ =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry_.first;
    bool retry = worker_to_kill_and_should_retry_.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
