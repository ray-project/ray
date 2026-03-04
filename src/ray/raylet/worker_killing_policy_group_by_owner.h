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

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/raylet/worker_killing_policy_interface.h"

namespace ray {

namespace raylet {

/**
 * @brief Key groups on its owner id. For non-retriable lease the owner id is Nil,
 * Since non-retriable lease forms its own group.
 */
struct GroupKey {
  explicit GroupKey(const TaskID &owner_id) : owner_id_(owner_id) {}
  const TaskID &owner_id_;
};

/**
 * @brief group of workers with the same owner id.
 */
struct Group {
 public:
  Group(const TaskID &owner_id, bool retriable)
      : owner_id_(owner_id), retriable_(retriable) {}

  /// The parent task id of the leases belonging to this group
  const TaskID &OwnerId() const;

  /// Whether leases in this group are retriable.
  const bool IsRetriable() const;

  /// Gets the assigned lease time of the earliest lease of this group, to be
  /// used for group priority.
  const absl::Time GetGrantedLeaseTime() const;

  /// Returns the worker to be killed in this group, in LIFO order.
  const std::shared_ptr<WorkerInterface> SelectWorkerToKill() const;

  /// Leases belonging to this group.
  const std::vector<std::shared_ptr<WorkerInterface>> GetAllWorkers() const;

  /// Adds worker that the lease belongs to to the group.
  void AddToGroup(std::shared_ptr<WorkerInterface> worker);

 private:
  /// Leases belonging to this group.
  std::vector<std::shared_ptr<WorkerInterface>> workers_;

  /// The earliest creation time of the leases.
  absl::Time earliest_granted_lease_time_ = absl::Now();

  /// The owner id shared by leases of this group.
  TaskID owner_id_;

  /// Whether the leases are retriable.
  bool retriable_;
};

/**
 * @brief Groups leases by its owner id. Non-retriable leases (whether it be task or
 * actor) forms its own group. Prioritizes selecting groups that are retriable first, else
 * it picks the largest group, else it picks the newest group. The "age" of a group is
 * based on the time of its earliest granted leases. When a group is selected for killing
 * it selects the last submitted worker.
 *
 * When selecting a worker / task to be killed, it will set the task to-be-killed to be
 * non-retriable if it is the last member of the group, and is retriable otherwise.
 */
class GroupByOwnerIdWorkerKillingPolicy : public WorkerKillingPolicyInterface {
 public:
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> SelectWorkersToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot,
      const SystemMemorySnapshot &system_memory_snapshot) override;

 private:
  /**
   * Executes the worker killing selection policy.
   * Here we prioritize killing workers from the group that are retriable first.
   * Else we prioritize killing workers from the group with the largest number of workers.
   * Else we prioritize killing workers from the newest group.
   * Once a group is selected, we select the last submitted worker from the group.
   *
   * \param workers the list of workers to select and kill from.
   * \param system_memory snapshot of memory usage. Used to print the memory usage of the
   * workers.
   * \return the list of workers to kill and whether the task on each worker
   * should be retried.
   */
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> Policy(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const ProcessesMemorySnapshot &process_memory_snapshot) const;

  /**
   * Creates the debug string of the groups created by the policy.
   *
   * \param groups groups to print
   * \param system_memory snapshot of memory usage.
   * \return the debug string.
   */
  static std::string PolicyDebugString(
      const std::vector<Group> &groups,
      const ProcessesMemorySnapshot &process_memory_snapshot);

  // The current selected workers being killed and whether the task on each worker
  // should be retried.
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> workers_being_killed_;
};

}  // namespace raylet

}  // namespace ray
