// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "ray/common/id.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

namespace ray {
namespace core {

// Structure to represent the relationship between LeaseId and TaskId
struct LeaseTaskMapping {
  // Map from LeaseId to the set of TaskIds that are executed under this lease
  absl::flat_hash_map<LeaseID, absl::flat_hash_set<TaskID>> lease_to_tasks;
  
  // Map from TaskId to the LeaseId that is executing this task
  absl::flat_hash_map<TaskID, LeaseID> task_to_lease;
  
  // Add a task to a lease mapping
  void AddTaskToLease(const LeaseID& lease_id, const TaskID& task_id) {
    lease_to_tasks[lease_id].insert(task_id);
    task_to_lease[task_id] = lease_id;
  }
  
  // Remove a task from its lease mapping
  void RemoveTask(const TaskID& task_id) {
    auto it = task_to_lease.find(task_id);
    if (it != task_to_lease.end()) {
      LeaseID lease_id = it->second;
      task_to_lease.erase(it);
      
      auto lease_it = lease_to_tasks.find(lease_id);
      if (lease_it != lease_to_tasks.end()) {
        lease_it->second.erase(task_id);
        if (lease_it->second.empty()) {
          lease_to_tasks.erase(lease_it);
        }
      }
    }
  }
  
  // Get all tasks for a given lease
  std::vector<TaskID> GetTasksForLease(const LeaseID& lease_id) const {
    auto it = lease_to_tasks.find(lease_id);
    if (it != lease_to_tasks.end()) {
      return std::vector<TaskID>(it->second.begin(), it->second.end());
    }
    return {};
  }
  
  // Get the lease for a given task
  LeaseID GetLeaseForTask(const TaskID& task_id) const {
    auto it = task_to_lease.find(task_id);
    if (it != task_to_lease.end()) {
      return it->second;
    }
    return LeaseID::Nil();
  }
  
  // Check if a task has a lease mapping
  bool HasTaskMapping(const TaskID& task_id) const {
    return task_to_lease.find(task_id) != task_to_lease.end();
  }
  
  // Check if a lease has any tasks
  bool HasLeaseTasks(const LeaseID& lease_id) const {
    auto it = lease_to_tasks.find(lease_id);
    return it != lease_to_tasks.end() && !it->second.empty();
  }
  
  // Get the number of tasks for a lease
  size_t GetTaskCountForLease(const LeaseID& lease_id) const {
    auto it = lease_to_tasks.find(lease_id);
    return it != lease_to_tasks.end() ? it->second.size() : 0;
  }
  
  // Clear all mappings
  void Clear() {
    lease_to_tasks.clear();
    task_to_lease.clear();
  }
  
  // Get total number of tracked tasks
  size_t GetTotalTaskCount() const {
    return task_to_lease.size();
  }
};

}  // namespace core
}  // namespace ray