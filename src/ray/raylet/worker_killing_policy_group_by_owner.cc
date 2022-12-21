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

#include "ray/raylet/worker_killing_policy.h"

#include <gtest/gtest_prod.h>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/raylet/worker.h"
#include "ray/raylet/worker_pool.h"
#include "ray/raylet/worker_killing_policy_group_by_owner.h"
#include "absl/container/flat_hash_map.h"
#include <boost/container_hash/hash.hpp>
#include <unordered_map>

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

  GroupMap group_map(10, group_key_hashing_func, group_key_equal_fn);

  for (auto worker : workers) {
    TaskID owner_id = worker->GetAssignedTask().GetTaskSpecification().ParentTaskId();
    bool retriable = worker->GetAssignedTask().GetTaskSpecification().IsRetriable();
    
    GroupKey group_key(owner_id, retriable);
    auto it = group_map.find(group_key);

    if (it == group_map.end()) {
        Group group(owner_id, retriable);
        group_map.insert({group_key, group});
        group.AddToGroup(worker);
    } else {
        auto group = it->second;
        group.AddToGroup(worker);
    }
  }
  
  std::vector<Group> sorted;
  for(auto it = group_map.begin(); it != group_map.end(); ++it) {
    sorted.push_back(it->second);
  }

  std::sort(sorted.begin(),
            sorted.end(),
            [](Group const &left,
               Group const &right) -> bool {
              int left_retriable = left.IsRetriable() ? 0 : 1;
              int right_retriable = right.IsRetriable() ? 0 : 1;
              if (left_retriable == right_retriable) {
                return left.GetAssignedTaskTime() > right.GetAssignedTaskTime();
              }
              return left_retriable < right_retriable;
              return 0;
            });
  
  Group selected_group = sorted.front();
  auto worker_to_kill = selected_group.SelectWorkerToKill();
  bool should_retry = selected_group.WorkerCount() > 1;

  // const static int32_t max_to_print = 10;
  // RAY_LOG(INFO) << "The top 10 workers to be killed based on the worker killing policy:\n"
  //               << WorkersDebugString(sorted, max_to_print, system_memory);

  // return std::make_pair(sorted.front(), /*should retry*/ true);

  return std::make_pair(worker_to_kill, should_retry);
}

bool Group::IsRetriable() const {
  return retriable_;
}

const std::chrono::steady_clock::time_point Group::GetAssignedTaskTime() const {
  return time_;
}

void Group::AddToGroup(std::shared_ptr<WorkerInterface> worker) {
  if (worker->GetAssignedTaskTime() < time_) {
    time_ = worker->GetAssignedTaskTime();
  }
  if (workers_.empty()) {
    retriable_ = worker->GetAssignedTask().GetTaskSpecification().IsRetriable();
  } else {
    RAY_CHECK_EQ(retriable_, worker->GetAssignedTask().GetTaskSpecification().IsRetriable());
  }
  workers_.insert(worker);
}

const std::shared_ptr<WorkerInterface> Group::SelectWorkerToKill() const {
  RAY_CHECK(!workers_.empty());
  
  std::vector<std::shared_ptr<WorkerInterface>> sorted(workers_.begin(), workers_.end());

  std::sort(sorted.begin(),
            sorted.end(),
            [](std::shared_ptr<WorkerInterface> const &left,
               std::shared_ptr<WorkerInterface> const &right) -> bool {
              int left_retriable =
                  left->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              int right_retriable =
                  right->GetAssignedTask().GetTaskSpecification().IsRetriable() ? 0 : 1;
              if (left_retriable == right_retriable) {
                return left->GetAssignedTaskTime() > right->GetAssignedTaskTime();
              }
              return left_retriable < right_retriable;
            });

  return sorted.front();
}

int32_t Group::WorkerCount() const {
  return workers_.size();
}

}  // namespace raylet

}  // namespace ray
