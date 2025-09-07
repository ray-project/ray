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

#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/memory_monitor.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

/// Key groups on its owner id.
struct GroupKey {
  explicit GroupKey(const TaskID &owner_id) : owner_id_(owner_id) {}
  const TaskID &owner_id_;
};

struct Group {
 public:
  explicit Group(const TaskID &owner_id) : owner_id_(owner_id) {}

  /// The parent task id of the leases belonging to this group
  const TaskID &OwnerId() const;

  /// Gets the assigned lease time of the earliest lease of this group, to be
  /// used for group priority.
  const absl::Time GetGrantedLeaseTime() const;

  /// Returns the worker to be killed in this group, in LIFO order.
  std::shared_ptr<WorkerInterface> SelectWorkerToKill() const;

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
};

/// Groups leases by its owner id. Prioritizes killing the largest groups first else it
/// picks the newest group. The "age" of a group is based on the time of its earliest
/// granted leases. When a group is selected for killing it selects the last submitted
/// task.
class GroupByOwnerIdWorkerKillingPolicy : public WorkerKillingPolicy {
 public:
  GroupByOwnerIdWorkerKillingPolicy() = default;
  std::shared_ptr<WorkerInterface> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const override;

 private:
  /// Creates the debug string of the groups created by the policy.
  static std::string PolicyDebugString(const std::vector<Group> &groups,
                                       const MemorySnapshot &system_memory);
};

}  // namespace raylet

}  // namespace ray
