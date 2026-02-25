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

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "ray/raylet/worker_killing_policy_interface.h"

namespace ray {

namespace raylet {

/**
 * @brief No-op worker killing policy for non-Linux platforms.
 * This policy never selects any workers to kill.
 */
class NoopWorkerKillingPolicy : public WorkerKillingPolicyInterface {
 public:
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> SelectWorkersToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot,
      const SystemMemorySnapshot &_) override {
    return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
  }
};

}  // namespace raylet

}  // namespace ray
