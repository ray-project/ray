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

#include <sstream>

namespace ray {

RayTask::RayTask(const rpc::Task &message, int64_t backlog_size)
    : task_spec_(message.task_spec()),
      task_execution_spec_(message.task_execution_spec()),
      backlog_size_(backlog_size) {
  ComputeDependencies();
}

RayTask::RayTask(TaskSpecification task_spec,
                 TaskExecutionSpecification task_execution_spec)
    : task_spec_(std::move(task_spec)),
      task_execution_spec_(std::move(task_execution_spec)) {
  ComputeDependencies();
}

const TaskExecutionSpecification &RayTask::GetTaskExecutionSpec() const {
  return task_execution_spec_;
}

const TaskSpecification &RayTask::GetTaskSpecification() const { return task_spec_; }

void RayTask::IncrementNumForwards() { task_execution_spec_.IncrementNumForwards(); }

const std::vector<rpc::ObjectReference> &RayTask::GetDependencies() const {
  return dependencies_;
}

void RayTask::ComputeDependencies() { dependencies_ = task_spec_.GetDependencies(); }

void RayTask::CopyTaskExecutionSpec(const RayTask &task) {
  task_execution_spec_ = task.task_execution_spec_;
}

void RayTask::SetBacklogSize(int64_t backlog_size) { backlog_size_ = backlog_size; }

int64_t RayTask::BacklogSize() const { return backlog_size_; }

std::string RayTask::DebugString() const {
  std::ostringstream stream;
  stream << "task_spec={" << task_spec_.DebugString() << "}, task_execution_spec={"
         << task_execution_spec_.DebugString() << "}";
  return stream.str();
}

}  // namespace ray
