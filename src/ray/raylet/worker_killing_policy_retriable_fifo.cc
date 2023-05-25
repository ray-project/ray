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

#include "ray/raylet/worker_killing_policy_retriable_fifo.h"

#include <gtest/gtest_prod.h>

#include <boost/container_hash/hash.hpp>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

RetriableFIFOWorkerKillingPolicy::RetriableFIFOWorkerKillingPolicy() {}

const std::pair<std::shared_ptr<WorkerInterface>, bool>
RetriableFIFOWorkerKillingPolicy::SelectWorkerToKill(
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
              // First sort by retriable tasks and then by task time in ascending order.
              int left_retriable =
                  left->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              int right_retriable =
                  right->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              if (left_retriable == right_retriable) {
                return left->GetAssignedTaskTime() < right->GetAssignedTaskTime();
              }
              return left_retriable < right_retriable;
            });

  const static int32_t max_to_print = 10;
  RAY_LOG(INFO) << "The top 10 workers to be killed based on the worker killing policy:\n"
                << WorkerKillingPolicy::WorkersDebugString(
                       sorted, max_to_print, system_memory);

  return std::make_pair(sorted.front(), /*should retry*/ true);
}

}  // namespace raylet

}  // namespace ray
