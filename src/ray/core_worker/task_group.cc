// Copyright 2017 The Ray Authors.
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

#include "ray/core_worker/task_group.h"

namespace ray {
namespace core {

void TaskGroup::AddPendingTask(const TaskSpecification &spec) {
  auto name = spec.GetName();
  tasks_by_name_[name] += 1;
}

void TaskGroup::FinishTask(const TaskSpecification &spec) {
  auto name = spec.GetName();
  finished_tasks_by_name_[name] += 1;
}

void TaskGroup::FillTaskGroup(rpc::TaskGroupInfoEntry *entry) {
  if (task_spec_ != nullptr) {
    entry->set_name(task_spec_->GetName());
  }
  for (const auto &pair : tasks_by_name_) {
    auto child_group = entry->add_child_group();
    child_group->set_name(pair.first);
    child_group->set_count(pair.second);
    child_group->set_finished_count(finished_tasks_by_name_[pair.first]);
  }
}

void TaskGroupManager::FillTaskGroupInfo(rpc::GetCoreWorkerStatsReply *reply,
                                         const int64_t limit) const {
  for (const auto &group_data : groups_) {
    auto group = reply->add_task_group_infos();
    group_data->FillTaskGroup(group);
  }
}

void TaskGroupManager::AddPendingTask(const TaskSpecification &spec) {
  auto &group = GetOrCreateCurrentTaskGroup();
  group.AddPendingTask(spec);
}

void TaskGroupManager::FinishTask(const TaskSpecification &spec) {
  // TODO(ekl) tasks should be finished in their starting task group?
  auto &group = GetOrCreateCurrentTaskGroup();
  group.FinishTask(spec);
}

TaskGroup &TaskGroupManager::GetOrCreateCurrentTaskGroup() {
  // TODO(ekl) also check if task id has changed, then also create a new task group
  if (groups_.size() == 0) {
    // TODO(ekl) bound groups size
    groups_.push_back(std::make_unique<TaskGroup>(worker_context_.GetCurrentTask()));
    RAY_LOG(ERROR) << "Create new task group";
  }
  RAY_CHECK(groups_.size() >= 1);
  return *groups_.back();
}

}  // namespace core
}  // namespace ray
