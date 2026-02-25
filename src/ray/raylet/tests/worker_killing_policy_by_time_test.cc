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

#include "ray/raylet/worker_killing_policy_by_time.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "ray/common/lease/lease.h"
#include "ray/common/lease/lease_spec.h"
#include "ray/common/memory_monitor_interface.h"
#include "ray/raylet/tests/util.h"

namespace ray {

namespace raylet {

class WorkerKillingPolicyByTimeTest : public ::testing::Test {
 protected:
  int32_t port_ = 2389;
  JobID job_id_ = JobID::FromInt(75);
  int32_t no_retry_ = 0;
  int32_t has_retry_ = 1;

  static constexpr int64_t TOTAL_SYSTEM_MEMORY_BYTES = 2000;
  static constexpr int64_t KILL_BUFFER_BYTES = 100;
  // The threshold calculation takes the max of the fraction and min_memory_free_bytes.
  // Here we make the threshold 1000 bytes since total - min_memory_free_bytes >
  // threshold_fraction * total
  static constexpr float THRESHOLD_FRACTION = 0.1;
  static constexpr int64_t MIN_MEMORY_FREE_BYTES = 1000;

  TimeBasedWorkerKillingPolicy policy_ = TimeBasedWorkerKillingPolicy(
      THRESHOLD_FRACTION, MIN_MEMORY_FREE_BYTES, KILL_BUFFER_BYTES);

  SystemMemorySnapshot CreateSystemSnapshot(
      int64_t used_bytes, int64_t total_bytes = TOTAL_SYSTEM_MEMORY_BYTES) {
    SystemMemorySnapshot snapshot;
    snapshot.used_bytes = used_bytes;
    snapshot.total_bytes = total_bytes;
    return snapshot;
  }

  ProcessesMemorySnapshot CreateProcessSnapshot(
      const std::vector<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
          &worker_memory) {
    ProcessesMemorySnapshot snapshot;
    for (const auto &entry : worker_memory) {
      snapshot[entry.first->GetProcess().GetId()] = entry.second;
    }
    return snapshot;
  }
};

TEST_F(WorkerKillingPolicyByTimeTest, TestPolicySelectsNoWorkersOnEmptyWorkerPool) {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(2000);
  ProcessesMemorySnapshot process_snapshot;

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);
  ASSERT_TRUE(workers_to_kill.empty());
}

TEST_F(WorkerKillingPolicyByTimeTest, TestPolicyPrioritizesRetriableOverNonRetriable) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> retriable_worker =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> non_retriable_worker =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::NORMAL_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(non_retriable_worker);
  workers.push_back(retriable_worker);

  // used_bytes - threshold + buffer = 1200 - 1000 + 100 = 300 bytes to free
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(1200);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{non_retriable_worker, 500}, {retriable_worker, 500}});

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);

  ASSERT_EQ(workers_to_kill.size(), 1);
  ASSERT_EQ(workers_to_kill[0].first->WorkerId(), retriable_worker->WorkerId());
}

TEST_F(WorkerKillingPolicyByTimeTest,
       TestPolicyPrioritizesNewerWorkersWithinSameRetriability) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> older_retriable =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> newer_retriable =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> older_non_retriable =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> newer_non_retriable =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::NORMAL_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(older_retriable);
  workers.push_back(newer_retriable);
  workers.push_back(older_non_retriable);
  workers.push_back(newer_non_retriable);

  // used_bytes - threshold + buffer = 2000 - 1000 + 100 = 1100 bytes to free
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(2000);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{older_retriable, 400},
                             {newer_retriable, 400},
                             {older_non_retriable, 400},
                             {newer_non_retriable, 400}});

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);

  ASSERT_EQ(workers_to_kill.size(), 3);

  ASSERT_EQ(workers_to_kill[0].first->WorkerId(), newer_retriable->WorkerId());
  ASSERT_TRUE(workers_to_kill[0].second);
  ASSERT_EQ(workers_to_kill[1].first->WorkerId(), older_retriable->WorkerId());
  ASSERT_TRUE(workers_to_kill[1].second);

  ASSERT_EQ(workers_to_kill[2].first->WorkerId(), newer_non_retriable->WorkerId());
  ASSERT_FALSE(workers_to_kill[2].second);
}

TEST_F(WorkerKillingPolicyByTimeTest, TestPolicyFreesEnoughWorkersToGetUnderThreshold) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> worker1 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> worker2 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> worker3 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> worker4 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(worker1);
  workers.push_back(worker2);
  workers.push_back(worker3);
  workers.push_back(worker4);

  // Memory to free: 1500 - 1000 + 100 = 600 bytes
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(1500);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{worker1, 100},  // oldest
                             {worker2, 200},
                             {worker3, 150},
                             {worker4, 250}});  // newest

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);

  ASSERT_EQ(workers_to_kill.size(), 3);
  ASSERT_EQ(workers_to_kill[0].first->WorkerId(), worker4->WorkerId());
  ASSERT_EQ(workers_to_kill[1].first->WorkerId(), worker3->WorkerId());
  ASSERT_EQ(workers_to_kill[2].first->WorkerId(), worker2->WorkerId());

  for (const auto &entry : workers_to_kill) {
    ASSERT_NE(entry.first->WorkerId(), worker1->WorkerId());
  }
}

TEST_F(WorkerKillingPolicyByTimeTest, TestPolicyRetriableFlagSetCorrectly) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);

  std::shared_ptr<WorkerInterface> retriable_task =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> non_retriable_task =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> retriable_actor =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);
  std::shared_ptr<WorkerInterface> non_retriable_actor =
      CreateTaskWorker(owner_id, no_retry_, port_, rpc::TaskType::ACTOR_CREATION_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(retriable_task);
  workers.push_back(non_retriable_task);
  workers.push_back(retriable_actor);
  workers.push_back(non_retriable_actor);

  // Need to kill all workers
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(2000);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{retriable_task, 300},
                             {non_retriable_task, 300},
                             {retriable_actor, 300},
                             {non_retriable_actor, 300}});

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);

  ASSERT_EQ(workers_to_kill.size(), 4);

  // Verify each worker's should_retry flag matches its retriability
  for (const auto &entry : workers_to_kill) {
    bool is_retriable =
        entry.first->GetGrantedLease().GetLeaseSpecification().IsRetriable();
    ASSERT_EQ(entry.second, is_retriable);
  }
}

TEST_F(WorkerKillingPolicyByTimeTest, TestPolicySelectsNoWorkersWhenKillingInProgress) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> worker1 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> worker2 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(worker1);
  workers.push_back(worker2);

  // Memory to free: 1200 - 1000 + 100 = 300 bytes
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(1200);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{worker1, 400}, {worker2, 400}});

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);
  ASSERT_EQ(workers_to_kill.size(), 1);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill_second =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);
  ASSERT_TRUE(workers_to_kill_second.empty());
}

TEST_F(WorkerKillingPolicyByTimeTest,
       TestPolicySelectsNewWorkersAfterPreviousSelectedIsKilled) {
  TaskID owner_id = TaskID::ForDriverTask(job_id_);
  std::shared_ptr<WorkerInterface> worker1 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);
  std::shared_ptr<WorkerInterface> worker2 =
      CreateTaskWorker(owner_id, has_retry_, port_, rpc::TaskType::NORMAL_TASK);

  std::vector<std::shared_ptr<WorkerInterface>> workers;
  workers.push_back(worker1);
  workers.push_back(worker2);

  // Memory to free: 1200 - 1000 + 100 = 300 bytes
  SystemMemorySnapshot system_snapshot = CreateSystemSnapshot(1200);
  ProcessesMemorySnapshot process_snapshot =
      CreateProcessSnapshot({{worker1, 400}, {worker2, 400}});

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);
  ASSERT_EQ(workers_to_kill.size(), 1);
  std::shared_ptr<WorkerInterface> killed_worker = workers_to_kill[0].first;

  KillWorkerProcess(killed_worker);
  workers.pop_back();

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_to_kill_third =
      policy_.SelectWorkersToKill(workers, process_snapshot, system_snapshot);
  ASSERT_EQ(workers_to_kill_third.size(), 1);
}

}  // namespace raylet

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
