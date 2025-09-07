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

#include "ray/raylet/worker_killing_policy_fifo.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/raylet/tests/util.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

class WorkerKillerTest : public ::testing::Test {
 protected:
  int32_t port_ = 2389;
  FIFOWorkerKillingPolicy worker_killing_policy_;

  std::shared_ptr<WorkerInterface> CreateActorCreationWorker() {
    rpc::LeaseSpec message;
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    LeaseSpecification lease_spec(message);
    RayLease lease(lease_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->GrantLease(lease);
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateTaskWorker() {
    rpc::LeaseSpec message;
    message.set_type(ray::rpc::TaskType::NORMAL_TASK);
    LeaseSpecification lease_spec(message);
    RayLease lease(lease_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->GrantLease(lease);
    return worker;
  }
};

TEST_F(WorkerKillerTest, TestEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto worker_to_kill =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillerTest, TestOrderByTimestampAscending) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillerTest::CreateActorCreationWorker();
  auto second_submitted = WorkerKillerTest::CreateActorCreationWorker();
  auto third_submitted = WorkerKillerTest::CreateTaskWorker();
  auto fourth_submitted = WorkerKillerTest::CreateTaskWorker();

  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);

  MemorySnapshot memory_snapshot;
  auto worker_to_kill =
      worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
  ASSERT_EQ(worker_to_kill->WorkerId(), first_submitted->WorkerId());
  workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                workers.end());

  worker_to_kill = worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
  ASSERT_EQ(worker_to_kill->WorkerId(), second_submitted->WorkerId());
  workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                workers.end());

  worker_to_kill = worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
  ASSERT_EQ(worker_to_kill->WorkerId(), third_submitted->WorkerId());
  workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                workers.end());

  worker_to_kill = worker_killing_policy_.SelectWorkerToKill(workers, memory_snapshot);
  ASSERT_EQ(worker_to_kill->WorkerId(), fourth_submitted->WorkerId());
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
