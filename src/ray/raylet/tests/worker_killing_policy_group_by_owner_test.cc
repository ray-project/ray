// Copyright 2026 The Ray Authors.
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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/lease/lease.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/raylet/tests/util.h"
#include "ray/util/fake_process.h"

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
  int64_t idle_threshold_bytes_ = 1000;
  GroupByOwnerIdWorkerKillingPolicy worker_killing_policy_ =
      GroupByOwnerIdWorkerKillingPolicy(idle_threshold_bytes_);
};

TEST_F(WorkerKillingGroupByOwnerTest, TestEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
      worker_to_kill_and_should_retry = worker_killing_policy_.SelectWorkersToKill(
          workers, ProcessesMemorySnapshot(), SystemMemorySnapshot());
  ASSERT_TRUE(worker_to_kill_and_should_retry.empty());
}

TEST_F(WorkerKillingGroupByOwnerTest, TestLastWorkerInGroupShouldNotRetry) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> first_submitted =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> second_submitted =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);

  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));

  for (const std::pair<std::shared_ptr<WorkerInterface>, bool> &entry : expected) {
    std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
        worker_killing_policy_
            .SelectWorkersToKill(
                workers, ProcessesMemorySnapshot(), SystemMemorySnapshot())
            .front();
    std::shared_ptr<WorkerInterface> worker_to_kill =
        worker_to_kill_and_should_retry.first;
    bool retry = worker_to_kill_and_should_retry.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    KillWorkerProcess(worker_to_kill);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestNonRetriableBelongsToItsOwnGroupAndLIFOKill) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> first_submitted =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> second_submitted =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::NORMAL_TASK);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));

  std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
      worker_killing_policy_
          .SelectWorkersToKill(workers, ProcessesMemorySnapshot(), SystemMemorySnapshot())
          .front();

  std::shared_ptr<WorkerInterface> worker_to_kill = worker_to_kill_and_should_retry.first;
  bool retry = worker_to_kill_and_should_retry.second;
  ASSERT_EQ(worker_to_kill->WorkerId(), second_submitted->WorkerId());
  ASSERT_EQ(retry, should_not_retry_);
}

TEST_F(WorkerKillingGroupByOwnerTest, TestGroupSortedByGroupSizeThenFirstSubmittedTask) {
  TaskID first_group_owner_id = TaskID::FromRandom(job_id_);
  TaskID second_group_owner_id = TaskID::FromRandom(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> first_submitted = CreateTaskWorker(
      first_group_owner_id, has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> second_submitted = CreateTaskWorker(
      second_group_owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> third_submitted = CreateTaskWorker(
      second_group_owner_id, has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> fourth_submitted = CreateTaskWorker(
      second_group_owner_id, has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> fifth_submitted = CreateTaskWorker(
      first_group_owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> sixth_submitted = CreateTaskWorker(
      first_group_owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
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

  for (const std::pair<std::shared_ptr<WorkerInterface>, bool> &entry : expected) {
    std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
        worker_killing_policy_
            .SelectWorkersToKill(
                workers, ProcessesMemorySnapshot(), SystemMemorySnapshot())
            .front();
    std::shared_ptr<WorkerInterface> worker_to_kill =
        worker_to_kill_and_should_retry.first;
    bool retry = worker_to_kill_and_should_retry.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    KillWorkerProcess(worker_to_kill);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestGroupSortedByRetriableLifo) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> first_submitted = CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> second_submitted = CreateTaskWorker(
      TaskID::FromRandom(job_id_), has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> third_submitted = CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));
  expected.push_back(std::make_pair(third_submitted, should_not_retry_));

  for (const std::pair<std::shared_ptr<WorkerInterface>, bool> &entry : expected) {
    std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
        worker_killing_policy_
            .SelectWorkersToKill(
                workers, ProcessesMemorySnapshot(), SystemMemorySnapshot())
            .front();
    std::shared_ptr<WorkerInterface> worker_to_kill =
        worker_to_kill_and_should_retry.first;
    bool retry = worker_to_kill_and_should_retry.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    KillWorkerProcess(worker_to_kill);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest,
       TestMultipleNonRetriableTaskSameGroupAndNotRetried) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> first_submitted = CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> second_submitted = CreateTaskWorker(
      TaskID::FromRandom(job_id_), no_retry_, port_, rpc::TaskType::NORMAL_TASK);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(second_submitted, should_not_retry_));
  expected.push_back(std::make_pair(first_submitted, should_not_retry_));

  for (const std::pair<std::shared_ptr<WorkerInterface>, bool> &entry : expected) {
    std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
        worker_killing_policy_
            .SelectWorkersToKill(
                workers, ProcessesMemorySnapshot(), SystemMemorySnapshot())
            .front();
    std::shared_ptr<WorkerInterface> worker_to_kill =
        worker_to_kill_and_should_retry.first;
    bool retry = worker_to_kill_and_should_retry.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    KillWorkerProcess(worker_to_kill);
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestNoKillWorkerWithNoLeaseBelowMemoryThreshold) {
  pid_t current_pid = 1000;
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> worker_with_lease =
      CreateTaskWorker(TaskID::FromRandom(job_id_),
                       has_retry_,
                       port_,
                       rpc::TaskType::NORMAL_TASK,
                       ++current_pid);
  std::shared_ptr<WorkerInterface> worker_without_lease =
      CreateWorkerWithNoLease(port_, ++current_pid);
  workers.push_back(worker_with_lease);
  workers.push_back(worker_without_lease);
  ProcessesMemorySnapshot process_memory_snapshot = {
      // worker without lease below memory threshold
      {worker_without_lease->GetProcess().GetId(), idle_threshold_bytes_ - 1},
      // random memory usage for worker with lease
      {worker_with_lease->GetProcess().GetId(), 10}};

  // worker with lease should be killed
  std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
      worker_killing_policy_
          .SelectWorkersToKill(workers, process_memory_snapshot, SystemMemorySnapshot())
          .front();
  std::shared_ptr<WorkerInterface> worker_to_kill = worker_to_kill_and_should_retry.first;
  bool retry = worker_to_kill_and_should_retry.second;
  ASSERT_EQ(worker_to_kill->WorkerId(), worker_with_lease->WorkerId());
  ASSERT_EQ(retry, should_not_retry_);
  KillWorkerProcess(worker_to_kill);
  workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                workers.end());

  // worker without lease will not be selected for killing
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
      worker_to_kill_and_should_retry_list = worker_killing_policy_.SelectWorkersToKill(
          workers, process_memory_snapshot, SystemMemorySnapshot());
  ASSERT_EQ(worker_to_kill_and_should_retry_list.size(), 0);
}

TEST_F(WorkerKillingGroupByOwnerTest, TestKillingWorkerWithNoLeaseIfMemoryExceeded) {
  pid_t current_pid = 1000;
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> worker_with_lease =
      CreateTaskWorker(TaskID::FromRandom(job_id_),
                       has_retry_,
                       port_,
                       rpc::TaskType::NORMAL_TASK,
                       ++current_pid);
  std::shared_ptr<WorkerInterface> worker_without_lease =
      CreateWorkerWithNoLease(port_, ++current_pid);
  workers.push_back(worker_with_lease);
  workers.push_back(worker_without_lease);
  ProcessesMemorySnapshot process_memory_snapshot = {
      // worker without lease above memory threshold
      {worker_without_lease->GetProcess().GetId(), idle_threshold_bytes_ + 1},
      // random memory usage for worker with lease
      {worker_with_lease->GetProcess().GetId(), 10}};

  // worker without lease should be killed first and then worker with lease should
  // be killed
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> expected;
  expected.push_back(std::make_pair(worker_without_lease, should_not_retry_));
  expected.push_back(std::make_pair(worker_with_lease, should_not_retry_));

  for (const std::pair<std::shared_ptr<WorkerInterface>, bool> &entry : expected) {
    std::pair<std::shared_ptr<WorkerInterface>, bool> worker_to_kill_and_should_retry =
        worker_killing_policy_
            .SelectWorkersToKill(workers, process_memory_snapshot, SystemMemorySnapshot())
            .front();
    std::shared_ptr<WorkerInterface> worker_to_kill =
        worker_to_kill_and_should_retry.first;
    bool retry = worker_to_kill_and_should_retry.second;
    ASSERT_EQ(worker_to_kill->WorkerId(), entry.first->WorkerId());
    ASSERT_EQ(retry, entry.second);
    KillWorkerProcess(worker_to_kill);
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
