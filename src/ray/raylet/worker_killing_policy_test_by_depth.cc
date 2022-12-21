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

class WorkerKillerTestByDepth : public WorkerKillerTest {
 protected:
  GroupByDepthWorkerKillingPolicy groupby_depth_worker_killing_policy_;
};

TEST_F(WorkerKillerTestByDepth, TestDepthGroupingEmptyWorkerPoolSelectsNullWorker) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;
  std::shared_ptr<WorkerInterface> worker_to_kill =
      groupby_depth_worker_killing_policy_.SelectWorkerToKill(workers, MemorySnapshot());
  ASSERT_TRUE(worker_to_kill == nullptr);
}

TEST_F(WorkerKillerTestByDepth, TestDepthGroupingTwoNestedTasks) {
  std::vector<std::shared_ptr<WorkerInterface>> workers{
      WorkerKillerTestByDepth::CreateTaskWorker(0, 1),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 1),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 2),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 2),
  };

  std::vector<std::shared_ptr<WorkerInterface>> expected_order{
      workers[3],
      workers[1],
      workers[2],
      workers[0],
  };
  for (const auto &expected : expected_order) {
    auto killed = groupby_depth_worker_killing_policy_.SelectWorkerToKill(
        workers, MemorySnapshot());
    ASSERT_EQ(killed->WorkerId(), expected->WorkerId());
    workers.erase(std::remove(workers.begin(), workers.end(), killed), workers.end());
  }
}

TEST_F(WorkerKillerTestByDepth, TestDepthGroupingTwoNestedTasksOnlyOneAtHighestDepth) {
  std::vector<std::shared_ptr<WorkerInterface>> workers{
      WorkerKillerTestByDepth::CreateTaskWorker(0, 1),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 1),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 2),
  };

  std::vector<std::shared_ptr<WorkerInterface>> expected_order{
      workers[1],
      workers[2],
      workers[0],
  };
  for (const auto &expected : expected_order) {
    auto killed = groupby_depth_worker_killing_policy_.SelectWorkerToKill(
        workers, MemorySnapshot());
    ASSERT_EQ(killed->WorkerId(), expected->WorkerId());
    workers.erase(std::remove(workers.begin(), workers.end(), killed), workers.end());
  }
}

TEST_F(WorkerKillerTestByDepth, TestDepthGroupingOnlyOneAtAllDepths) {
  std::vector<std::shared_ptr<WorkerInterface>> workers{
      WorkerKillerTestByDepth::CreateTaskWorker(0, 1),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 2),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 3),
      WorkerKillerTestByDepth::CreateTaskWorker(0, 4),
  };

  std::vector<std::shared_ptr<WorkerInterface>> expected_order{
      workers[3],
      workers[2],
      workers[1],
      workers[0],
  };
  for (const auto &expected : expected_order) {
    auto killed = groupby_depth_worker_killing_policy_.SelectWorkerToKill(
        workers, MemorySnapshot());
    ASSERT_EQ(killed->WorkerId(), expected->WorkerId());
    workers.erase(std::remove(workers.begin(), workers.end(), killed), workers.end());
  }
}

}  // namespace raylet

}  // namespace ray
