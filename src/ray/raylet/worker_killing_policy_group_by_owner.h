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

#include "ray/common/memory_monitor.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_killing_policy.h"
#include "absl/container/flat_hash_set.h"


namespace ray {

namespace raylet {

/// Groups worker by its owner id if it is a task. Each actor belongs to its own group.
/// The inter-group policy prioritizes killing groups that are retriable first, then in LIFO order,
/// where each group's priority is based on the time of its earliest submitted member.
/// The intra-group policy prioritizes killing in LIFO order.
/// 
/// It will set the task to-be-killed to be non-retriable if it is the last member of the group.
/// Otherwise the task is set to be retriable.
class GroupByOwnerIdWorkerKillingPolicy : public WorkerKillingPolicy {
 public:
  GroupByOwnerIdWorkerKillingPolicy();
  const std::pair<std::shared_ptr<WorkerInterface>, bool> SelectWorkerToKill(
      const std::vector<std::shared_ptr<WorkerInterface>> &workers,
      const MemorySnapshot &system_memory) const;
};

struct GroupKey {
  GroupKey(const TaskID& owner_id, bool retriable): owner_id(owner_id), retriable(retriable) {}
  const TaskID& owner_id;
  bool retriable;
};

class Group {
 public:
  Group(const TaskID& owner_id, bool retriable): owner_id_(owner_id), retriable_(retriable) {}
  bool IsRetriable() const;
  const std::chrono::steady_clock::time_point GetAssignedTaskTime() const;
  const std::shared_ptr<WorkerInterface> SelectWorkerToKill() const;
  int32_t WorkerCount() const;
  void AddToGroup(std::shared_ptr<WorkerInterface> worker);
  
 private:
  /// Tasks belonging to this group.
  absl::flat_hash_set<std::shared_ptr<WorkerInterface>> workers_;

  /// The earliest creation time of the tasks.
  std::chrono::steady_clock::time_point time_ = std::chrono::steady_clock::now();

  /// The owner id shared by tasks of this group.
  TaskID owner_id_;

  /// Whether the tasks are retriable.
  bool retriable_;
};

typedef std::unordered_map<GroupKey, Group,
    std::function<unsigned long(const GroupKey&)>,
    std::function<bool(const GroupKey&, const GroupKey&)>> GroupMap;

unsigned long group_key_hashing_func(const GroupKey& key) {
    unsigned long hash = 0;
    // for(size_t i=0; i<key.size(); i++)
    //   hash += (71*hash + key[i]) % 5;
    return hash;
}

bool group_key_equal_fn(const GroupKey& left, const GroupKey& right) {
  return left.owner_id == right.owner_id && left.retriable == right.retriable;
}

}  // namespace raylet

}  // namespace ray
