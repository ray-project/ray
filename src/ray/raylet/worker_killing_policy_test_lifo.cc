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

#include <sys/sysinfo.h>

#include "gtest/gtest.h"
#include "ray/common/task/task_spec.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/raylet/worker_killing_policy_test_util.h"

namespace ray {

namespace raylet {

class WorkerKillerTestLifo : public WorkerKillerTest {
 protected:
  RetriableLIFOWorkerKillingPolicy prefer_retriable_worker_killing_policy_;
};

TEST_F(WorkerKillerTestLifo, TestRetriableEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> worker_to_kill =
      prefer_retriable_worker_killing_policy_.SelectWorkerToKill(workers,
                                                                 MemorySnapshot());
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillerTestLifo,
       TestPreferRetriableOverNonRetriableAndOrderByTimestampDescending) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  auto first_submitted = WorkerKillerTestLifo::CreateActorWorker(7 /* max_restarts */);
  auto second_submitted =
      WorkerKillerTestLifo::CreateActorCreationWorker(5 /* max_restarts */);
  auto third_submitted = WorkerKillerTestLifo::CreateTaskWorker(0 /* max_restarts */);
  auto fourth_submitted = WorkerKillerTestLifo::CreateTaskWorker(11 /* max_restarts */);
  auto fifth_submitted =
      WorkerKillerTestLifo::CreateActorCreationWorker(0 /* max_restarts */);
  auto sixth_submitted = WorkerKillerTestLifo::CreateActorWorker(0 /* max_restarts */);

  workers.push_back(first_submitted);
  workers.push_back(second_submitted);
  workers.push_back(third_submitted);
  workers.push_back(fourth_submitted);
  workers.push_back(fifth_submitted);
  workers.push_back(sixth_submitted);

  std::vector<std::shared_ptr<WorkerInterface>> expected_order;
  expected_order.push_back(fourth_submitted);
  expected_order.push_back(second_submitted);
  expected_order.push_back(sixth_submitted);
  expected_order.push_back(fifth_submitted);
  expected_order.push_back(third_submitted);
  expected_order.push_back(first_submitted);

  for (const auto &expected : expected_order) {
    std::shared_ptr<WorkerInterface> worker_to_kill =
        prefer_retriable_worker_killing_policy_.SelectWorkerToKill(workers,
                                                                   MemorySnapshot());
    ASSERT_EQ(worker_to_kill->WorkerId(), expected->WorkerId());
    workers.erase(std::remove(workers.begin(), workers.end(), worker_to_kill),
                  workers.end());
  }
}

}  // namespace raylet

}  // namespace ray
