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

#include "absl/time/time.h"

namespace ray {
namespace core {

void TaskGroup::AddPendingTask(const TaskSpecification &spec) {
  auto name = spec.GetName();
  if (creation_time_by_name_.find(name) == creation_time_by_name_.end()) {
    creation_time_by_name_[name] = absl::GetCurrentTimeNanos();
  }
  tasks_by_name_[name] += 1;
}

void TaskGroup::FinishTask(const TaskSpecification &spec) {
  auto name = spec.GetName();
  finished_tasks_by_name_[name] += 1;
}

void TaskGroup::FillTaskGroup(rpc::TaskGroupInfoEntry *entry) {
  if (task_spec_ != nullptr) {
    entry->set_name(task_spec_->GetName());
    entry->set_parent_task_id(task_spec_->ParentTaskId().Binary());
    entry->set_depth(task_spec_->GetDepth());
  }
  entry->set_task_id(current_task_id_.Binary());
  for (const auto &pair : tasks_by_name_) {
    auto child_group = entry->add_child_group();
    child_group->set_name(pair.first);
    child_group->set_count(pair.second);
    child_group->set_finished_count(finished_tasks_by_name_[pair.first]);
    child_group->set_creation_time_nanos(creation_time_by_name_[pair.first]);
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
  auto cur_task = worker_context_.GetCurrentTask();
  if (groups_.size() == 0 ||
      (cur_task != nullptr && groups_.back()->TaskId() != cur_task->TaskId())) {
    RAY_LOG(INFO) << "New task group " << worker_context_.GetCurrentTaskID();
    groups_.push_back(
        std::make_unique<TaskGroup>(cur_task, worker_context_.GetCurrentTaskID()));
    // TODO(ekl) bound groups size
  }
  RAY_CHECK(groups_.size() >= 1);
  return *groups_.back();
}

}  // namespace core
}  // namespace ray
