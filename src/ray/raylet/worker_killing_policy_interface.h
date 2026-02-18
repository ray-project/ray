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

#pragma once

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ray/common/memory_monitor_interface.h"
#include "ray/raylet/worker_interface.h"
#include "ray/raylet/worker_pool.h"

namespace ray {

namespace raylet {

/**
 * @brief Implementations of this interface provide the policy on which worker to
 * prioritize killing.
 */
class WorkerKillingPolicyInterface {
 public:
  /**
   * @brief Selects workers to be killed.
   *
   * @param workers the list of candidate workers.
   * @param system_memory snapshot of memory usage.
   * @return the worker to kill and whether the task on the worker should be retried.
   */
  virtual std::pair<std::shared_ptr<WorkerInterface>, bool> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot) const = 0;

  virtual ~WorkerKillingPolicyInterface() = default;
};

}  // namespace raylet

}  // namespace ray
