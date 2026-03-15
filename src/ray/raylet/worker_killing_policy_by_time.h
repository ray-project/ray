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
#include <string>
#include <utility>
#include <vector>

#include "ray/raylet/worker_killing_policy_interface.h"

namespace ray {

namespace raylet {

/**
 * @brief Policy that selects workers to kill based on:
 * 1. Retriable tasks first (to maximize retry opportunities)
 * 2. Among tasks with the same retriability, most recent task first (newest granted
 *    lease time)
 * The policy will select enough workers to kill to put the system back
 * under the memory usage threshold - kill_buffer_bytes.
 */
class TimeBasedWorkerKillingPolicy : public WorkerKillingPolicyInterface {
 public:
  /**
   * @param usage_threshold The fraction to indicate max user memory usage.
   *        Used to compute the memory usage threshold to free to.
   * @param min_memory_free_bytes The minimum amount of memory to leave free.
   *        Used to compute the memory usage threshold to free to.
   * @param kill_buffer_bytes The amount of memory buffer under
   * the memory usage threshold to leave free after killing workers.
   */
  TimeBasedWorkerKillingPolicy(float usage_threshold,
                               int64_t min_memory_free_bytes,
                               int64_t kill_buffer_bytes);

  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> SelectWorkersToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot,
      const SystemMemorySnapshot &system_memory_snapshot) override;

 private:
  /**
   * @brief Executes the worker selection policy.
   * Prioritizes killing retriable workers first, killing newest workers next
   * based on the most recent granted lease time.
   * Ensures total memory consumption of selected workers
   * will put us back under the memory usage threshold - kill_buffer_bytes.
   *
   * @param workers The list of candidate workers.
   * @param process_memory_snapshot Snapshot of per-process memory usage.
   * @param system_memory_snapshot Snapshot of system memory usage.
   * @return A list of pairs containing workers to kill and whether each task should be
   * retried.
   */
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> Policy(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot,
      const SystemMemorySnapshot &system_memory_snapshot) const;

  /**
   * @brief Creates the debug string showing workers sorted by the policy priority.
   *
   * @param workers The list of workers with their retry flags.
   * @param process_memory_snapshot Snapshot of per-process memory usage.
   * @return A formatted debug string.
   */
  static std::string PolicyDebugString(
      const std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot);

  /// Targets to be killed.
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_being_killed_;

  /// The max user memory usage fraction.
  float usage_threshold_;

  /// The min amount of free space to maintain before it is
  /// exceeding the threshold.
  int64_t min_memory_free_bytes_;

  /// The kill memory buffer in bytes
  int64_t kill_buffer_bytes_;
};

}  // namespace raylet

}  // namespace ray
