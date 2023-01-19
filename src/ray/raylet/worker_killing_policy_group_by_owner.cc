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

#include "ray/raylet/worker_killing_policy_group_by_owner.h"

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

GroupByOwnerIdWorkerKillingPolicy::GroupByOwnerIdWorkerKillingPolicy() {}

const std::pair<std::shared_ptr<WorkerInterface>, bool>
GroupByOwnerIdWorkerKillingPolicy::SelectWorkerToKill(
    const std::vector<std::shared_ptr<WorkerInterface>> &workers,
    const MemorySnapshot &system_memory) const {
  if (workers.empty()) {
    RAY_LOG_EVERY_MS(INFO, 5000) << "Worker list is empty. Nothing can be killed";
    return std::make_pair(nullptr, /*should retry*/ false);
  }

  TaskID non_retriable_owner_id = TaskID::Nil();
  std::unordered_map<TaskID, Group> group_map;
  for (auto worker : workers) {
    bool retriable = worker->GetAssignedTask().GetTaskSpecification().IsRetriable();
    TaskID owner_id =
        retriable ? worker->GetAssignedTask().GetTaskSpecification().ParentTaskId()
                  : non_retriable_owner_id;

    auto it = group_map.find(owner_id);

    if (it == group_map.end()) {
      Group group(owner_id, retriable);
      group.AddToGroup(worker);
      group_map.emplace(owner_id, std::move(group));
    } else {
      auto &group = it->second;
      group.AddToGroup(worker);
    }
  }

  std::vector<Group> sorted;
  for (auto it = group_map.begin(); it != group_map.end(); ++it) {
    sorted.push_back(it->second);
  }

  /// Prioritizes killing groups that are retriable, else it picks the largest group,
  /// else it picks the newest group.
  std::sort(
      sorted.begin(), sorted.end(), [](const Group &left, const Group &right) -> bool {
        int left_retriable = left.IsRetriable() ? 0 : 1;
        int right_retriable = right.IsRetriable() ? 0 : 1;

        if (left_retriable == right_retriable) {
          if (left.GetAllWorkers().size() == right.GetAllWorkers().size()) {
            return left.GetAssignedTaskTime() > right.GetAssignedTaskTime();
          }
          return left.GetAllWorkers().size() > right.GetAllWorkers().size();
        }
        return left_retriable < right_retriable;
      });

  Group selected_group = sorted.front();
  bool should_retry =
      selected_group.GetAllWorkers().size() > 1 && selected_group.IsRetriable();
  auto worker_to_kill = selected_group.SelectWorkerToKill();

  RAY_LOG(INFO) << "Sorted list of tasks based on the policy:\n"
                << PolicyDebugString(sorted, system_memory);

  return std::make_pair(worker_to_kill, should_retry);
}

std::string GroupByOwnerIdWorkerKillingPolicy::PolicyDebugString(
    const std::vector<Group> &groups, const MemorySnapshot &system_memory) {
  std::stringstream result;
  int32_t group_index = 0;
  for (auto &group : groups) {
    result << "Tasks (retriable: " << group.IsRetriable()
           << ") (parent task id: " << group.OwnerId() << ") (Earliest assigned time: "
           << absl::FormatTime(group.GetAssignedTaskTime(), absl::UTCTimeZone())
           << "):\n";

    int64_t worker_index = 0;
    for (auto &worker : group.GetAllWorkers()) {
      auto pid = worker->GetProcess().GetId();
      int64_t used_memory = 0;
      const auto pid_entry = system_memory.process_used_bytes.find(pid);
      if (pid_entry != system_memory.process_used_bytes.end()) {
        used_memory = pid_entry->second;
      } else {
        RAY_LOG_EVERY_MS(INFO, 60000)
            << "Can't find memory usage for PID, reporting zero. PID: " << pid;
      }
      result << "Task assigned time "
             << absl::FormatTime(worker->GetAssignedTaskTime(), absl::UTCTimeZone())
             << " worker id " << worker->WorkerId() << " memory used " << used_memory
             << " task spec "
             << worker->GetAssignedTask().GetTaskSpecification().DebugString() << "\n";

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

const absl::Time Group::GetAssignedTaskTime() const { return earliest_task_time_; }

void Group::AddToGroup(std::shared_ptr<WorkerInterface> worker) {
  if (worker->GetAssignedTaskTime() < earliest_task_time_) {
    earliest_task_time_ = worker->GetAssignedTaskTime();
  }
  bool retriable = worker->GetAssignedTask().GetTaskSpecification().IsRetriable();
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
              return left->GetAssignedTaskTime() > right->GetAssignedTaskTime();
            });

  return sorted.front();
}

const std::vector<std::shared_ptr<WorkerInterface>> Group::GetAllWorkers() const {
  return workers_;
}

}  // namespace raylet

}  // namespace ray
