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

#include <gtest/gtest_prod.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy_group_by_owner.h"
#include "ray/raylet/worker_killing_policy_retriable_fifo.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

RetriableLIFOWorkerKillingPolicy::RetriableLIFOWorkerKillingPolicy() {}

const std::pair<std::shared_ptr<WorkerInterface>, bool>
RetriableLIFOWorkerKillingPolicy::SelectWorkerToKill(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const MemorySnapshot &system_memory) const {
  if (workers.empty()) {
    RAY_LOG_EVERY_MS(INFO, 5000) << "Worker list is empty. Nothing can be killed";
    return std::make_pair(nullptr, /*should retry*/ false);
  }

  std::vector<std::shared_ptr<WorkerInterface>> sorted = workers;

  std::sort(sorted.begin(),
            sorted.end(),
            [](std::shared_ptr<WorkerInterface> const &left,
               std::shared_ptr<WorkerInterface> const &right) -> bool {
              // First sort by retriable tasks and then by task time in descending order.
              int left_retriable =
                  left->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              int right_retriable =
                  right->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              if (left_retriable == right_retriable) {
                return left->GetAssignedTaskTime() > right->GetAssignedTaskTime();
              }
              return left_retriable < right_retriable;
            });

  const static int32_t max_to_print = 10;
  RAY_LOG(INFO) << "The top 10 workers to be killed based on the worker killing policy:\n"
                << WorkersDebugString(sorted, max_to_print, system_memory);

  return std::make_pair(sorted.front(), /*should retry*/ true);
}

std::string WorkerKillingPolicy::WorkersDebugString(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    int32_t num_workers,
    const MemorySnapshot &system_memory) {
  std::stringstream result;
  int64_t index = 1;
  for (auto &worker : workers) {
    auto pid = worker->GetProcess().GetId();
    int64_t used_memory = 0;
    const auto pid_entry = system_memory.process_used_bytes.find(pid);
    if (pid_entry != system_memory.process_used_bytes.end()) {
      used_memory = pid_entry->second;
    } else {
      RAY_LOG_EVERY_MS(INFO, 60000)
          << "Can't find memory usage for PID, reporting zero. PID: " << pid;
    }
    result << "Worker " << index << ": task assigned time "
           << absl::FormatTime(worker->GetAssignedTaskTime(), absl::UTCTimeZone())
           << " worker id " << worker->WorkerId() << " memory used " << used_memory
           << " task spec "
           << worker->GetAssignedTask().GetTaskSpecification().DebugString() << "\n";

    index += 1;
    if (index > num_workers) {
      break;
    }
  }
  return result.str();
}

std::shared_ptr<WorkerKillingPolicy> CreateWorkerKillingPolicy(
    std::string killing_policy_str) {
  if (killing_policy_str == kLifoPolicy) {
    RAY_LOG(INFO) << "Running RetriableLIFO policy.";
    return std::make_shared<RetriableLIFOWorkerKillingPolicy>();
  } else if (killing_policy_str == kGroupByOwner) {
    RAY_LOG(INFO) << "Running GroupByOwner policy.";
    return std::make_shared<GroupByOwnerIdWorkerKillingPolicy>();
  } else if (killing_policy_str == kFifoPolicy) {
    RAY_LOG(INFO) << "Running RetriableFIFO policy.";
    return std::make_shared<RetriableFIFOWorkerKillingPolicy>();
  } else {
    RAY_LOG(ERROR)
        << killing_policy_str
        << " is an invalid killing policy. Defaulting to RetriableLIFO policy.";
    return std::make_shared<RetriableLIFOWorkerKillingPolicy>();
  }
}

}  // namespace raylet

}  // namespace ray
