// Copyright 2019-2020 The Ray Authors.
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

#include "ray/common/task/task.h"

#include "absl/strings/str_format.h"

namespace ray {

RayTask::RayTask(rpc::TaskSpec task_spec) : task_spec_(std::move(task_spec)) {
  ComputeDependencies();
}

RayTask::RayTask(rpc::Task message)
    : task_spec_(std::move(*message.mutable_task_spec())) {
  ComputeDependencies();
}

RayTask::RayTask(TaskSpecification task_spec) : task_spec_(std::move(task_spec)) {
  ComputeDependencies();
}

RayTask::RayTask(TaskSpecification task_spec, std::string preferred_node_id)
    : task_spec_(std::move(task_spec)), preferred_node_id_(std::move(preferred_node_id)) {
  ComputeDependencies();
}

const TaskSpecification &RayTask::GetTaskSpecification() const { return task_spec_; }

const std::vector<rpc::ObjectReference> &RayTask::GetDependencies() const {
  return dependencies_;
}

const std::string &RayTask::GetPreferredNodeID() const { return preferred_node_id_; }

void RayTask::ComputeDependencies() { dependencies_ = task_spec_.GetDependencies(); }

std::string RayTask::DebugString() const {
  return absl::StrFormat("task_spec={%s}", task_spec_.DebugString());
}

}  // namespace ray
