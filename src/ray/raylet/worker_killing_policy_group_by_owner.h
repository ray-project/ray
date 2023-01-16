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

#include "absl/container/flat_hash_set.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "ray/common/memory_monitor.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"

namespace ray {

namespace raylet {

/// Key groups on its owner id. For non-retriable task the owner id is itself,
/// Since non-retriable task forms its own group.
struct GroupKey {
  GroupKey(const TaskID &owner_id) : owner_id(owner_id) {}
  const TaskID &owner_id;
};

struct Group {
 public:
  Group(const TaskID &owner_id, bool retriable)
      : owner_id_(owner_id), retriable_(retriable) {}

  /// The parent task id of the tasks belonging to this group
  const TaskID &OwnerId() const;

  /// Whether tasks in this group are retriable.
  bool IsRetriable() const;

  /// Gets the task time of the earliest task of this group, to be
  /// used for group priority.
  const absl::Time GetAssignedTaskTime() const;

  /// Returns the worker to be killed in this group, in LIFO order.
  const std::shared_ptr<WorkerInterface> SelectWorkerToKill() const;

  /// Tasks belonging to this group.
  const std::vector<std::shared_ptr<WorkerInterface>> GetAllWorkers() const;

  /// Adds worker that the task belongs to to the group.
  void AddToGroup(std::shared_ptr<WorkerInterface> worker);

 private:
  /// Tasks belonging to this group.
  std::vector<std::shared_ptr<WorkerInterface>> workers_;

  /// The earliest creation time of the tasks.
  absl::Time time_ = absl::Now();

  /// The owner id shared by tasks of this group.
  TaskID owner_id_;

  /// Whether the tasks are retriable.
  bool retriable_;
};

/// Groups task by its owner id. Non-retriable task (whether it be task or actor) forms
/// its own group. Prioritizes killing groups that are retriable first, else it picks the
/// largest group, else it picks the newest group. The "age" of a group is based on the
/// time of its earliest submitted task. When a group is selected for killing it selects
/// the last submitted task.
///
/// When selecting a worker / task to be killed, it will set the task to-be-killed to be
/// non-retriable if it is the last member of the group, and is retriable otherwise.
class GroupByOwnerIdWorkerKillingPolicy : public WorkerKillingPolicy {
 public:
  GroupByOwnerIdWorkerKillingPolicy();
  const std::pair<std::shared_ptr<WorkerInterface>, bool> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const;

 private:
  /// Creates the debug string of the groups created by the policy.
  static std::string PolicyDebugString(const std::vector<Group> &groups,
                                       const MemorySnapshot &system_memory);
};

}  // namespace raylet

}  // namespace ray
