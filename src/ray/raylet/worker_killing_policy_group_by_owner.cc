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

#include "ray/raylet/worker_killing_policy_group_by_owner.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/str_format.h"
#include "absl/time/time.h"
#include "ray/common/lease/lease.h"

namespace ray {

namespace raylet {

std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
GroupByOwnerIdWorkerKillingPolicy::SelectWorkersToKill(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const ProcessesMemorySnapshot &process_memory_snapshot,
    const SystemMemorySnapshot &system_memory_snapshot) {
  RAY_UNUSED(system_memory_snapshot);
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>> remaining_alive_targets;
  for (const std::pair<std::shared_ptr<WorkerInterface>, bool>
           &worker_to_kill_or_should_retry : workers_being_killed_) {
    std::shared_ptr<WorkerInterface> worker = worker_to_kill_or_should_retry.first;
    if (worker->GetProcess().IsAlive()) {
      RAY_LOG(INFO).WithField(worker->WorkerId()).WithField(worker->GetGrantedLeaseId())
          << absl::StrFormat(
                 "Still waiting for worker eviction to free up memory. worker pid: %d",
                 worker->GetProcess().GetId());
      remaining_alive_targets.push_back(worker_to_kill_or_should_retry);
    }
  }
  workers_being_killed_ = remaining_alive_targets;
  if (workers_being_killed_.empty()) {
    workers_being_killed_ = Policy(workers, process_memory_snapshot);
    if (workers_being_killed_.empty()) {
      RAY_LOG_EVERY_MS(WARNING, 5000)
          << "Worker killer did not select any workers to "
             "kill even though memory usage is high. Object store "
             "may be causing high memory pressure. Consider checking "
             "if too many objects are unintentionally being stored.";
    }
    return workers_being_killed_;
  }
  // Else, there are workers still alive from the previous iteration.
  // We need to wait until they are dead before we can kill more workers.
  return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
}

std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
GroupByOwnerIdWorkerKillingPolicy::Policy(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const ProcessesMemorySnapshot &process_memory_snapshot) const {
  if (workers.empty()) {
    return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
  }

  // Prioritize killing workers that don't have any lease granted and occupy
  // a large amount of memory first.
  std::shared_ptr<WorkerInterface> idle_worker_to_kill = nullptr;
  int64_t max_idle_worker_used_memory = 0;
  for (const std::shared_ptr<WorkerInterface> &worker : workers) {
    if (worker->GetGrantedLeaseId().IsNil()) {
      int64_t used_memory = GetProcessUsedMemoryBytes(process_memory_snapshot,
                                                      worker->GetProcess().GetId());
      if (used_memory > idle_worker_killing_memory_threshold_bytes_ &&
          used_memory > max_idle_worker_used_memory) {
        max_idle_worker_used_memory = used_memory;
        idle_worker_to_kill = worker;
      }
    }
  }

  if (idle_worker_to_kill) {
    RAY_LOG(INFO)
            .WithField("worker_id", idle_worker_to_kill->WorkerId())
            .WithField("worker_pid", idle_worker_to_kill->GetProcess().GetId())
            .WithField("worker_used_memory", max_idle_worker_used_memory)
            .WithField("memory_threshold", idle_worker_killing_memory_threshold_bytes_)
        << "Selected a worker that doesn't have any lease granted and occupies large "
           "amount of memory to kill. ";
    return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>{
        std::make_pair(idle_worker_to_kill, /*should retry*/ false)};
  }

  // Group workers by owner id
  RAY_LOG(INFO) << "No workers found that don't have any lease granted and occupies "
                   "large amount of memory. "
                << "Grouping the workers by owner id, sorting the groups by the "
                << "policy, and picking a worker to kill from the top group.";
  TaskID non_retriable_owner_id = TaskID::Nil();
  std::unordered_map<TaskID, Group> group_map;
  for (std::shared_ptr<WorkerInterface> worker : workers) {
    // Skip workers that don't have any lease granted.
    if (worker->GetGrantedLeaseId().IsNil()) {
      continue;
    }
    bool retriable = worker->GetGrantedLease().GetLeaseSpecification().IsRetriable();
    TaskID owner_id =
        retriable ? worker->GetGrantedLease().GetLeaseSpecification().ParentTaskId()
                  : non_retriable_owner_id;

    std::unordered_map<TaskID, Group>::iterator it = group_map.find(owner_id);

    if (it == group_map.end()) {
      Group group(owner_id, retriable);
      group.AddToGroup(worker);
      group_map.emplace(owner_id, std::move(group));
    } else {
      Group &group = it->second;
      group.AddToGroup(worker);
    }
  }

  if (group_map.empty()) {
    return std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>();
  }

  std::vector<Group> sorted;
  for (std::unordered_map<TaskID, Group>::iterator it = group_map.begin();
       it != group_map.end();
       ++it) {
    sorted.push_back(it->second);
  }

  /// Prioritizes selecting groups that are retriable, else it picks the largest group,
  /// else it picks the newest group.
  std::sort(
      sorted.begin(), sorted.end(), [](const Group &left, const Group &right) -> bool {
        int left_retriable = left.IsRetriable() ? 0 : 1;
        int right_retriable = right.IsRetriable() ? 0 : 1;

        if (left_retriable == right_retriable) {
          if (left.GetAllWorkers().size() == right.GetAllWorkers().size()) {
            return left.GetGrantedLeaseTime() > right.GetGrantedLeaseTime();
          }
          return left.GetAllWorkers().size() > right.GetAllWorkers().size();
        }
        return left_retriable < right_retriable;
      });

  Group selected_group = sorted.front();
  bool should_retry =
      selected_group.GetAllWorkers().size() > 1 && selected_group.IsRetriable();
  std::shared_ptr<WorkerInterface> worker_to_kill = selected_group.SelectWorkerToKill();

  RAY_LOG(INFO) << absl::StrFormat(
      "Sorted list of leases based on the policy: %s, Lease should be retried? %s",
      PolicyDebugString(sorted, process_memory_snapshot),
      should_retry ? "true" : "false");
  std::vector<std::pair<std::shared_ptr<WorkerInterface>, bool>>
      workers_to_kill_and_should_retry;
  workers_to_kill_and_should_retry.emplace_back(worker_to_kill, should_retry);

  return workers_to_kill_and_should_retry;
}

std::string GroupByOwnerIdWorkerKillingPolicy::PolicyDebugString(
    const std::vector<Group> &groups,
    const ProcessesMemorySnapshot &process_memory_snapshot) {
  std::stringstream result;
  int32_t group_index = 0;
  for (const Group &group : groups) {
    if (group_index > 0) {
      result << ", ";
    }
    result << absl::StrFormat(
        "Leases (retriable: %d) (parent task id: %s) (Earliest granted "
        "time: %s): ",
        group.IsRetriable(),
        group.OwnerId().Hex(),
        absl::FormatTime(group.GetGrantedLeaseTime(), absl::UTCTimeZone()));

    int64_t worker_index = 0;
    for (const std::shared_ptr<WorkerInterface> &worker : group.GetAllWorkers()) {
      if (worker_index > 0) {
        result << ", ";
      }
      int64_t used_memory = GetProcessUsedMemoryBytes(process_memory_snapshot,
                                                      worker->GetProcess().GetId());
      const LeaseSpecification &lease_spec =
          worker->GetGrantedLease().GetLeaseSpecification();
      result << absl::StrFormat(
          "Lease granted time %s worker id %s memory used %d lease_id %s "
          "task_name %s",
          absl::FormatTime(worker->GetGrantedLeaseTime(), absl::UTCTimeZone()),
          worker->WorkerId().Hex(),
          used_memory,
          lease_spec.LeaseId().Hex(),
          lease_spec.GetTaskName());

      worker_index += 1;
      if (worker_index > 10) {
        break;
      }
    }

    group_index += 1;
    if (group_index > 10) {
      break;
    }
  }

  return result.str();
}

const TaskID &Group::OwnerId() const { return owner_id_; }

const bool Group::IsRetriable() const { return retriable_; }

const absl::Time Group::GetGrantedLeaseTime() const {
  return earliest_granted_lease_time_;
}

void Group::AddToGroup(std::shared_ptr<WorkerInterface> worker) {
  if (worker->GetGrantedLeaseTime() < earliest_granted_lease_time_) {
    earliest_granted_lease_time_ = worker->GetGrantedLeaseTime();
  }
  bool retriable = worker->GetGrantedLease().GetLeaseSpecification().IsRetriable();
  RAY_CHECK_EQ(retriable_, retriable);
  workers_.push_back(worker);
}

const std::shared_ptr<WorkerInterface> Group::SelectWorkerToKill() const {
  RAY_CHECK(!workers_.empty());
  std::vector<std::shared_ptr<WorkerInterface>> sorted(workers_.begin(), workers_.end());

  std::sort(sorted.begin(),
            sorted.end(),
            [](std::shared_ptr<WorkerInterface> const &left,
               std::shared_ptr<WorkerInterface> const &right) -> bool {
              return left->GetGrantedLeaseTime() > right->GetGrantedLeaseTime();
            });

  return sorted.front();
}

const std::vector<std::shared_ptr<WorkerInterface>> Group::GetAllWorkers() const {
  return workers_;
}

}  // namespace raylet

}  // namespace ray
