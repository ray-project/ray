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

#include "ray/raylet/worker_killing_policy.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/raylet/tests/util.h"

namespace ray {

namespace raylet {

class WorkerKillerTest : public ::testing::Test {
 protected:
  instrumented_io_context io_context_;
  int32_t port_ = 2389;
  RetriableLIFOWorkerKillingPolicy worker_killing_policy_;

  std::shared_ptr<WorkerInterface> CreateActorCreationWorker(int32_t max_restarts) {
    rpc::LeaseSpec message;
    message.set_max_actor_restarts(max_restarts);
    message.set_type(ray::rpc::TaskType::ACTOR_CREATION_TASK);
    LeaseSpecification lease_spec(message);
    RayLease lease(lease_spec);
    auto worker = std::make_shared<MockWorker>(ray::WorkerID::FromRandom(), port_);
    worker->GrantLease(lease);
    return worker;
  }

  std::shared_ptr<WorkerInterface> CreateTaskWorker(int32_t max_retries) {
    rpc::LeaseSpec message;
    message.set_max_retries(max_retries);
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
  auto worker_to_kill_and_should_retry =
      worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  auto worker_to_kill = worker_to_kill_and_should_retry.first;
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillerTest,
       TestPreferRetriableOverNonRetriableAndOrderByTimestampDescending) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted =
      WorkerKillerTest::CreateActorCreationWorker(false /* max_restarts */);
  auto second_submitted =
      WorkerKillerTest::CreateActorCreationWorker(true /* max_restarts */);
  auto third_submitted = WorkerKillerTest::CreateTaskWorker(false /* max_retries */);
  auto fourth_submitted = WorkerKillerTest::CreateTaskWorker(true /* max_retries */);
  auto fifth_submitted =
      WorkerKillerTest::CreateActorCreationWorker(false /* max_restarts */);
  auto sixth_submitted = WorkerKillerTest::CreateTaskWorker(true /* max_retries */);

  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  std::vector<std::shared_ptr<WorkerInterface>> expected_order;
  expected_order.push_back(sixth_submitted);
  expected_order.push_back(fourth_submitted);
  expected_order.push_back(second_submitted);
  expected_order.push_back(fifth_submitted);
  expected_order.push_back(third_submitted);
  expected_order.push_back(first_submitted);

  for (const auto &expected : expected_order) {
    auto worker_to_kill_and_should_retry =
        worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
    auto worker_to_kill = worker_to_kill_and_should_retry.first;
    ASSERT_EQ(worker_to_kill->WorkerId(), expected->WorkerId());
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
