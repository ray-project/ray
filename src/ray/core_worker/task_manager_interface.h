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

#include <string>
#include <vector>

#include "absl/types/optional.h"
#include "ray/common/id.h"
#include "ray/common/lease/lease.h"
#include "ray/common/scheduling/scheduling_ids.h"
#include "ray/common/status.h"
#include "ray/common/task/task_spec.h"
#include "src/ray/protobuf/common.pb.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace core {

class TaskManagerInterface {
 public:
  virtual ~TaskManagerInterface() = default;

  /// Add a task that is pending execution.
  ///
  /// The local ref count for all return refs (excluding actor creation tasks)
  /// will be initialized to 1 so that the ref is considered in scope before
  /// returning to the language frontend. The caller is responsible for
  /// decrementing the ref count once the frontend ref has gone out of scope.
  ///
  /// \param[in] caller_address The rpc address of the calling task.
  /// \param[in] spec The spec of the pending task.
  /// \param[in] max_retries Number of times this task may be retried
  /// on failure.
  /// \return ObjectRefs returned by this task.
  virtual std::vector<rpc::ObjectReference> AddPendingTask(
      const rpc::Address &caller_address,
      const TaskSpecification &spec,
      const std::string &call_site,
      int max_retries = 0) = 0;

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \param[in] worker_addr Address of the worker that executed the task.
  /// \param[in] is_application_error Whether this is an Exception return.
  virtual void CompletePendingTask(const TaskID &task_id,
                                   const rpc::PushTaskReply &reply,
                                   const rpc::Address &actor_addr,
                                   bool is_application_error) = 0;

  /// Returns true if task can be retried.
  ///
  /// \param[in] task_id ID of the task to be retried.
  /// \return true if task is scheduled to be retried.
  virtual bool RetryTaskIfPossible(const TaskID &task_id,
                                   const rpc::RayErrorInfo &error_info) = 0;

  /// A pending task failed. This will mark the task as failed.
  /// This doesn't always mark the return object as failed
  /// depending on mark_task_object_failed.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \param[in] status Optional status message.
  /// \param[in] ray_error_info The error information of a given error type.
  /// \param[in] mark_task_object_failed whether or not it marks the task
  /// return object as failed.
  virtual void FailPendingTask(const TaskID &task_id,
                               rpc::ErrorType error_type,
                               const Status *status = nullptr,
                               const rpc::RayErrorInfo *ray_error_info = nullptr) = 0;

  /// A pending task failed. This will either retry the task or mark the task
  /// as failed if there are no retries left.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \param[in] status Optional status message.
  /// \param[in] ray_error_info The error information of a given error type.
  /// Nullptr means that there's no error information.
  /// TODO(sang): Remove nullptr case. Every error message should have metadata.
  /// \param[in] mark_task_object_failed whether or not it marks the task
  /// return object as failed. If this is set to false, then the caller is
  /// responsible for later failing or completing the task.
  /// \param[in] fail_immediately whether to fail the task and ignore
  /// the retries that are available.
  /// \return Whether the task will be retried or not.
  virtual bool FailOrRetryPendingTask(const TaskID &task_id,
                                      rpc::ErrorType error_type,
                                      const Status *status,
                                      const rpc::RayErrorInfo *ray_error_info = nullptr,
                                      bool mark_task_object_failed = true,
                                      bool fail_immediately = false) = 0;

  /// Resubmit a task that has completed execution before. This is used to
  /// reconstruct objects stored in Plasma that were lost.
  ///
  /// \param[in] task_id The ID of the task to resubmit.
  /// \param[out] task_deps The object dependencies of the resubmitted task,
  /// i.e. all arguments that were not inlined in the task spec. The caller is
  /// responsible for making sure that these dependencies become available, so
  /// that the resubmitted task can run. This is only populated if the task was
  /// not already pending and was successfully resubmitted.
  /// \return nullopt if the task was successfully resubmitted (task or actor being
  /// scheduled, but no guarantee on completion), or was already pending. Return the
  /// appopriate error type to propagate for the object if the task was not successfully
  /// resubmitted.
  virtual std::optional<rpc::ErrorType> ResubmitTask(
      const TaskID &task_id, std::vector<ObjectID> *task_deps) = 0;

  /// Record that the given task is scheduled and wait for execution.
  ///
  /// \param[in] task_id The task that is will be running.
  /// \param[in] node_id The node id that this task wil be running.
  /// \param[in] worker_id The worker id that this task wil be running.
  virtual void MarkTaskWaitingForExecution(const TaskID &task_id,
                                           const NodeID &node_id,
                                           const WorkerID &worker_id) = 0;

  /// A task's dependencies were inlined in the task spec. This will decrement
  /// the ref count for the dependency IDs. If the dependencies contained other
  /// ObjectIDs, then the ref count for these object IDs will be incremented.
  ///
  /// \param[in] inlined_dependency_ids The args that were originally passed by
  /// reference into the task, but have now been inlined.
  /// \param[in] contained_ids Any ObjectIDs that were newly inlined in the
  /// task spec, because a serialized copy of the ID was contained in one of
  /// the inlined dependencies.
  virtual void OnTaskDependenciesInlined(
      const std::vector<ObjectID> &inlined_dependency_ids,
      const std::vector<ObjectID> &contained_ids) = 0;

  /// Record that the given task's dependencies have been created and the task
  /// can now be scheduled for execution.
  ///
  /// \param[in] task_id The task that is now scheduled.
  virtual void MarkDependenciesResolved(const TaskID &task_id) = 0;

  /// Sets the task state to no-retry. This is used when Ray overrides the user-specified
  /// retry count for a task (e.g., a task belonging to a dead actor).
  /// Unlike `MarkTaskCanceled`, this does not mark the task as canceled—`ray.get()` will
  /// raise the specific error that caused the retry override (e.g., ACTOR_ERROR).
  ///
  /// \param[in] task_id to set no retry.
  virtual void MarkTaskNoRetry(const TaskID &task_id) = 0;

  /// Marks the task as canceled and sets its retry count to zero. This function
  /// should only be used for task cancellation. Unlike `MarkTaskNoRetry`, a
  /// canceled task is not retriable and `ray.get()` will raise a
  /// `TASK_CANCELLED` error.
  ///
  /// \param[in] task_id to cancel.
  virtual void MarkTaskCanceled(const TaskID &task_id) = 0;

  /// Check if a task has been marked as cancelled.
  ///
  /// \param[in] task_id The task ID to check.
  /// \return true if the task was cancelled, false otherwise.
  virtual bool IsTaskCanceled(const TaskID &task_id) const = 0;

  /// Return the spec for a pending task.
  virtual std::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const = 0;

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  virtual bool IsTaskPending(const TaskID &task_id) const = 0;

  /// Called by submitter when a generator task marked for resubmission for intermediate
  /// object recovery comes back from the executing worker. We mark the attempt as failed
  /// and resubmit it, so we can recover the intermediate return.
  virtual void MarkGeneratorFailedAndResubmit(const TaskID &task_id) = 0;
};

}  // namespace core
}  // namespace ray
