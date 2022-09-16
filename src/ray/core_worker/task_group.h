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

#pragma once

#include "absl/synchronization/mutex.h"
#include "ray/common/grpc_util.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/context.h"
#include "ray/util/macros.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

class TaskGroup {
 public:
  TaskGroup(std::shared_ptr<const TaskSpecification> task_spec, TaskID current_task_id)
      : task_spec_(task_spec), current_task_id_(current_task_id){};
  void AddPendingTask(const TaskSpecification &spec);
  void FinishTask(const TaskSpecification &spec);
  void FillTaskGroup(rpc::TaskGroupInfoEntry *entry);

  TaskID TaskId() const {
    if (task_spec_ == nullptr) {
      return TaskID::Nil();
    } else {
      return task_spec_->TaskId();
    }
  };

 private:
  // Note: nullable.
  std::shared_ptr<const TaskSpecification> task_spec_;
  TaskID current_task_id_;
  absl::flat_hash_map<std::string, int64_t> tasks_by_name_;
  // TODO: revisit the task state strategy. We need executor workers to provide
  // additional task state info that we merge with the group infos (e.g., RUNNING).
  absl::flat_hash_map<std::string, int64_t> finished_tasks_by_name_;
  absl::flat_hash_map<std::string, int64_t> creation_time_by_name_;
  RAY_DISALLOW_COPY_AND_ASSIGN(TaskGroup);
};

// This class is NOT thread-safe.
class TaskGroupManager {
  // TODO: add enabled config flag
 public:
  TaskGroupManager(WorkerContext &ctx) : worker_context_(ctx){};
  void FillTaskGroupInfo(rpc::GetCoreWorkerStatsReply *reply, const int64_t limit) const;
  void AddPendingTask(const TaskSpecification &spec);
  void FinishTask(const TaskSpecification &spec);

 private:
  TaskGroup &GetOrCreateCurrentTaskGroup();

  WorkerContext &worker_context_;

  std::deque<std::unique_ptr<TaskGroup>> groups_;
};

}  // namespace core
}  // namespace ray
