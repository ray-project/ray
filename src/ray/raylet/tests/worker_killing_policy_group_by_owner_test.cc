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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/raylet/tests/util.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

class WorkerKillingGroupByOwnerTest : public ::testing::Test {
 protected:
  instrumented_io_context io_context_;
  int32_t port_ = 2389;
  JobID job_id_ = JobID::FromInt(75);
  GroupByOwnerIdWorkerKillingPolicy worker_killing_policy_;

  std::shared_ptr<WorkerInterface> CreateActorCreationWorker(TaskID owner_id) {
    rpc::LeaseSpec message;
    message.set_lease_id(LeaseID::FromRandom().Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    LeaseSpecification lease_spec(message);
    RayLease lease(lease_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->GrantLease(lease);
    worker->GrantLeaseId(lease.GetLeaseSpecification().LeaseId());
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateTaskWorker(TaskID owner_id) {
    rpc::LeaseSpec message;
    message.set_lease_id(LeaseID::FromRandom().Binary());
    message.set_parent_task_id(owner_id.Binary());
    message.set_type(ray::rpc::TaskType::NORMAL_TASK);
    LeaseSpecification lease_spec(message);
    RayLease lease(lease_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->GrantLease(lease);
    worker->GrantLeaseId(lease.GetLeaseSpecification().LeaseId());
    return worker;
  }
};

TEST_F(WorkerKillingGroupByOwnerTest, TestEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto worker_to_kill =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillingGroupByOwnerTest, TestSameGroupKillLIFO) {
  auto owner_id = TaskID::ForDriverTask(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(owner_id);
  auto second_submitted = WorkerKillingGroupByOwnerTest::CreateTaskWorker(owner_id);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);

  std::vector<std::shared_ptr<WorkerInterface>> expected;
  expected.push_back(second_submitted);

  auto worker_to_kill =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());

  ASSERT_EQ(worker_to_kill->WorkerId(), second_submitted->WorkerId());
}

TEST_F(WorkerKillingGroupByOwnerTest, TestDifferentGroupKillLIFO) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_));
  auto second_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_));
  auto third_submitted = WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(
      TaskID::FromRandom(job_id_));
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);

  std::vector<std::shared_ptr<WorkerInterface>> expected;
  expected.push_back(third_submitted);
  expected.push_back(second_submitted);
  expected.push_back(first_submitted);

  for (const auto &entry : expected) {
    auto worker_to_kill =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    ASSERT_EQ(worker_to_kill->WorkerId(), entry->WorkerId());
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

TEST_F(WorkerKillingGroupByOwnerTest, TestGroupSortedByGroupSizeThenFirstSubmittedTask) {
  auto first_group_owner_id = TaskID::FromRandom(job_id_);
  auto second_group_owner_id = TaskID::FromRandom(job_id_);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(first_group_owner_id);
  auto second_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(second_group_owner_id);
  auto third_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(second_group_owner_id);
  auto fourth_submitted =
      WorkerKillingGroupByOwnerTest::CreateActorCreationWorker(second_group_owner_id);
  auto fifth_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(first_group_owner_id);
  auto sixth_submitted =
      WorkerKillingGroupByOwnerTest::CreateTaskWorker(first_group_owner_id);
  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  std::vector<std::shared_ptr<WorkerInterface>> expected;
  expected.push_back(fourth_submitted);
  expected.push_back(sixth_submitted);
  expected.push_back(third_submitted);
  expected.push_back(fifth_submitted);
  expected.push_back(second_submitted);
  expected.push_back(first_submitted);

  for (const auto &entry : expected) {
    auto worker_to_kill =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    ASSERT_EQ(worker_to_kill->WorkerId(), entry->WorkerId());
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
