// Copyright 2025 The Ray Authors.
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

#include <vector>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/status.h"
#include "ray/common/task/task.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

class TaskFinisherInterface {
 public:
  virtual ~TaskFinisherInterface() = default;

  virtual void CompletePendingTask(const TaskID &task_id,
                                   const rpc::PushTaskReply &reply,
                                   const rpc::Address &actor_addr,
                                   bool is_application_error) = 0;

  virtual bool RetryTaskIfPossible(const TaskID &task_id,
                                   const rpc::RayErrorInfo &error_info) = 0;

  virtual void FailPendingTask(const TaskID &task_id,
                               rpc::ErrorType error_type,
                               const Status *status = nullptr,
                               const rpc::RayErrorInfo *ray_error_info = nullptr) = 0;

  virtual bool FailOrRetryPendingTask(const TaskID &task_id,
                                      rpc::ErrorType error_type,
                                      const Status *status,
                                      const rpc::RayErrorInfo *ray_error_info = nullptr,
                                      bool mark_task_object_failed = true,
                                      bool fail_immediately = false) = 0;

  virtual void MarkTaskWaitingForExecution(const TaskID &task_id,
                                           const NodeID &node_id,
                                           const WorkerID &worker_id) = 0;

  virtual void OnTaskDependenciesInlined(
      const std::vector<ObjectID> &inlined_dependency_ids,
      const std::vector<ObjectID> &contained_ids) = 0;

  virtual void MarkDependenciesResolved(const TaskID &task_id) = 0;

  virtual bool MarkTaskCanceled(const TaskID &task_id) = 0;

  virtual std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const = 0;

  virtual bool IsTaskPending(const TaskID &task_id) const = 0;
};

}  // namespace core
}  // namespace ray
