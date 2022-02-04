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

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "src/ray/protobuf/core_worker.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace core {

class TaskFinisherInterface {
 public:
  virtual void CompletePendingTask(const TaskID &task_id, const rpc::PushTaskReply &reply,
                                   const rpc::Address &actor_addr) = 0;

  virtual bool RetryTaskIfPossible(const TaskID &task_id) = 0;

  virtual void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type,
                               const Status *status = nullptr,
                               const rpc::RayErrorInfo *ray_error_info = nullptr,
                               bool mark_task_object_failed = true) = 0;

  virtual bool FailOrRetryPendingTask(const TaskID &task_id, rpc::ErrorType error_type,
                                      const Status *status,
                                      const rpc::RayErrorInfo *ray_error_info = nullptr,
                                      bool mark_task_object_failed = true) = 0;

  virtual void OnTaskDependenciesInlined(
      const std::vector<ObjectID> &inlined_dependency_ids,
      const std::vector<ObjectID> &contained_ids) = 0;

  virtual bool MarkTaskCanceled(const TaskID &task_id) = 0;

  virtual void MarkTaskReturnObjectsFailed(
      const TaskSpecification &spec, rpc::ErrorType error_type,
      const rpc::RayErrorInfo *ray_error_info = nullptr) = 0;

  virtual absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const = 0;

  virtual ~TaskFinisherInterface() {}
};

class TaskResubmissionInterface {
 public:
  virtual bool ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) = 0;

  virtual ~TaskResubmissionInterface() {}
};

using PutInLocalPlasmaCallback =
    std::function<void(const RayObject &object, const ObjectID &object_id)>;
using RetryTaskCallback = std::function<void(TaskSpecification &spec, bool delay)>;
using ReconstructObjectCallback = std::function<void(const ObjectID &object_id)>;
using PushErrorCallback =
    std::function<Status(const JobID &job_id, const std::string &type,
                         const std::string &error_message, double timestamp)>;

class TaskManager : public TaskFinisherInterface, public TaskResubmissionInterface {
 public:
  TaskManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
              std::shared_ptr<ReferenceCounter> reference_counter,
              PutInLocalPlasmaCallback put_in_local_plasma_callback,
              RetryTaskCallback retry_task_callback,
              const std::function<bool(const NodeID &node_id)> &check_node_alive,
              ReconstructObjectCallback reconstruct_object_callback,
              PushErrorCallback push_error_callback, int64_t max_lineage_bytes)
      : in_memory_store_(in_memory_store),
        reference_counter_(reference_counter),
        put_in_local_plasma_callback_(put_in_local_plasma_callback),
        retry_task_callback_(retry_task_callback),
        check_node_alive_(check_node_alive),
        reconstruct_object_callback_(reconstruct_object_callback),
        push_error_callback_(push_error_callback),
        max_lineage_bytes_(max_lineage_bytes) {
    reference_counter_->SetReleaseLineageCallback(
        [this](const ObjectID &object_id, std::vector<ObjectID> *ids_to_release) {
          return RemoveLineageReference(object_id, ids_to_release);
          ShutdownIfNeeded();
        });
  }

  /// Add a task that is pending execution.
  ///
  /// \param[in] caller_address The rpc address of the calling task.
  /// \param[in] spec The spec of the pending task.
  /// \param[in] max_retries Number of times this task may be retried
  /// on failure.
  /// \return ObjectRefs returned by this task.
  std::vector<rpc::ObjectReference> AddPendingTask(const rpc::Address &caller_address,
                                                   const TaskSpecification &spec,
                                                   const std::string &call_site,
                                                   int max_retries = 0);

  /// Resubmit a task that has completed execution before. This is used to
  /// reconstruct objects stored in Plasma that were lost.
  ///
  /// \param[in] task_id The ID of the task to resubmit.
  /// \param[out] task_deps The object dependencies of the resubmitted task,
  /// i.e. all arguments that were not inlined in the task spec. The caller is
  /// responsible for making sure that these dependencies become available, so
  /// that the resubmitted task can run. This is only populated if the task was
  /// not already pending and was successfully resubmitted.
  /// \return OK if the task was successfully resubmitted or was
  /// already pending, Invalid if the task spec is no longer present.
  bool ResubmitTask(const TaskID &task_id, std::vector<ObjectID> *task_deps) override;

  /// Wait for all pending tasks to finish, and then shutdown.
  ///
  /// \param shutdown The shutdown callback to call.
  void DrainAndShutdown(std::function<void()> shutdown);

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \param[in] worker_addr Address of the worker that executed the task.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id, const rpc::PushTaskReply &reply,
                           const rpc::Address &worker_addr) override;

  bool RetryTaskIfPossible(const TaskID &task_id) override;

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
  /// return object as failed.
  /// \return Whether the task will be retried or not.
  bool FailOrRetryPendingTask(const TaskID &task_id, rpc::ErrorType error_type,
                              const Status *status = nullptr,
                              const rpc::RayErrorInfo *ray_error_info = nullptr,
                              bool mark_task_object_failed = true) override;

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
  void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type,
                       const Status *status = nullptr,
                       const rpc::RayErrorInfo *ray_error_info = nullptr,
                       bool mark_task_object_failed = true) override;

  /// Treat a pending task's returned Ray object as failed. The lock should not be held
  /// when calling this method because it may trigger callbacks in this or other classes.
  ///
  /// \param[in] spec The TaskSpec that contains return object.
  /// \param[in] error_type The error type the returned Ray object will store.
  /// \param[in] ray_error_info The error information of a given error type.
  void MarkTaskReturnObjectsFailed(
      const TaskSpecification &spec, rpc::ErrorType error_type,
      const rpc::RayErrorInfo *ray_error_info = nullptr) override LOCKS_EXCLUDED(mu_);

  /// A task's dependencies were inlined in the task spec. This will decrement
  /// the ref count for the dependency IDs. If the dependencies contained other
  /// ObjectIDs, then the ref count for these object IDs will be incremented.
  ///
  /// \param[in] inlined_dependency_ids The args that were originally passed by
  /// reference into the task, but have now been inlined.
  /// \param[in] contained_ids Any ObjectIDs that were newly inlined in the
  /// task spec, because a serialized copy of the ID was contained in one of
  /// the inlined dependencies.
  void OnTaskDependenciesInlined(const std::vector<ObjectID> &inlined_dependency_ids,
                                 const std::vector<ObjectID> &contained_ids) override;

  /// Set number of retries to zero for a task that is being canceled.
  ///
  /// \param[in] task_id to cancel.
  /// \return Whether the task was pending and was marked for cancellation.
  bool MarkTaskCanceled(const TaskID &task_id) override;

  /// Return the spec for a pending task.
  absl::optional<TaskSpecification> GetTaskSpec(const TaskID &task_id) const override;

  /// Return specs for pending children tasks of the given parent task.
  std::vector<TaskID> GetPendingChildrenTasks(const TaskID &parent_task_id) const;

  /// Return whether this task can be submitted for execution.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task can be submitted for execution.
  bool IsTaskSubmissible(const TaskID &task_id) const;

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  bool IsTaskPending(const TaskID &task_id) const;

  /// Return the number of submissible tasks. This includes both tasks that are
  /// pending execution and tasks that have finished but that may be
  /// re-executed to recover from a failure.
  size_t NumSubmissibleTasks() const;

  /// Return the number of pending tasks.
  size_t NumPendingTasks() const;

  int64_t TotalLineageFootprintBytes() const {
    absl::MutexLock lock(&mu_);
    return total_lineage_footprint_bytes_;
  }

 private:
  struct TaskEntry {
    TaskEntry(const TaskSpecification &spec_arg, int num_retries_left_arg,
              size_t num_returns)
        : spec(spec_arg), num_retries_left(num_retries_left_arg) {
      for (size_t i = 0; i < num_returns; i++) {
        reconstructable_return_ids.insert(spec.ReturnId(i));
      }
    }
    /// The task spec. This is pinned as long as the following are true:
    /// - The task is still pending execution. This means that the task may
    /// fail and so it may be retried in the future.
    /// - The task finished execution, but it has num_retries_left > 0 and
    /// reconstructable_return_ids is not empty. This means that the task may
    /// be retried in the future to recreate its return objects.
    /// TODO(swang): The TaskSpec protobuf must be copied into the
    /// PushTaskRequest protobuf when sent to a worker so that we can retry it if
    /// the worker fails. We could avoid this by either not caching the full
    /// TaskSpec for tasks that cannot be retried (e.g., actor tasks), or by
    /// storing a shared_ptr to a PushTaskRequest protobuf for all tasks.
    const TaskSpecification spec;
    // Number of times this task may be resubmitted. If this reaches 0, then
    // the task entry may be erased.
    int num_retries_left;
    // Number of times this task successfully completed execution so far.
    int num_successful_executions = 0;
    // Whether this task is currently pending execution. This is used to pin
    // the task entry if the task is still pending but all of its return IDs
    // are out of scope.
    bool pending = true;
    // Objects returned by this task that are reconstructable. This is set
    // initially to the task's return objects, since if the task fails, these
    // objects may be reconstructed by resubmitting the task. Once the task
    // finishes its first execution, then the objects that the task returned by
    // value are removed from this set because they can be inlined in any
    // dependent tasks. Objects that the task returned through plasma are
    // reconstructable, so they are only removed from this set once:
    // 1) The language frontend no longer has a reference to the object ID.
    // 2) There are no tasks that depend on the object. This includes both
    //    pending tasks and tasks that finished execution but that may be
    //    retried in the future.
    absl::flat_hash_set<ObjectID> reconstructable_return_ids;
    // The size of this (serialized) task spec in bytes, if the task spec is
    // not pending, i.e. it is being pinned because it's in another object's
    // lineage. We cache this because the task spec protobuf can mutate
    // out-of-band.
    int64_t lineage_footprint_bytes = 0;
  };

  /// Remove a lineage reference to this object ID. This should be called
  /// whenever a task that depended on this object ID can no longer be retried.
  ///
  /// \param[in] object_id The object ID whose lineage to delete.
  /// \param[out] ids_to_release If a task was deleted, then these are the
  /// task's arguments whose lineage should also be released.
  /// \param[out] The amount of lineage in bytes that was removed.
  int64_t RemoveLineageReference(const ObjectID &object_id,
                                 std::vector<ObjectID> *ids_to_release)
      LOCKS_EXCLUDED(mu_);

  /// Helper function to call RemoveSubmittedTaskReferences on the remaining
  /// dependencies of the given task spec after the task has finished or
  /// failed. The remaining dependencies are plasma objects and any ObjectIDs
  /// that were inlined in the task spec.
  void RemoveFinishedTaskReferences(
      TaskSpecification &spec, bool release_lineage, const rpc::Address &worker_addr,
      const ReferenceCounter::ReferenceTableProto &borrowed_refs);

  /// Shutdown if all tasks are finished and shutdown is scheduled.
  void ShutdownIfNeeded() LOCKS_EXCLUDED(mu_);

  /// Used to store task results.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used for reference counting objects.
  /// The task manager is responsible for managing all references related to
  /// submitted tasks (dependencies and return objects).
  std::shared_ptr<ReferenceCounter> reference_counter_;

  /// Callback to store objects in plasma. This is used for objects that were
  /// originally stored in plasma. During reconstruction, we ensure that these
  /// objects get stored in plasma again so that any reference holders can
  /// retrieve them.
  const PutInLocalPlasmaCallback put_in_local_plasma_callback_;

  /// Called when a task should be retried.
  const RetryTaskCallback retry_task_callback_;

  /// Called to check whether a raylet is still alive. This is used when
  /// processing a worker's reply to check whether the node that the worker
  /// was on is still alive. If the node is down, the plasma objects returned by the task
  /// are marked as failed.
  const std::function<bool(const NodeID &node_id)> check_node_alive_;
  /// Called when processing a worker's reply if the node that the worker was
  /// on died. This should be called to attempt to recover a plasma object
  /// returned by the task (or store an error if the object is not
  /// recoverable).
  const ReconstructObjectCallback reconstruct_object_callback_;

  // Called to push an error to the relevant driver.
  const PushErrorCallback push_error_callback_;

  const int64_t max_lineage_bytes_;

  // The number of task failures we have logged total.
  int64_t num_failure_logs_ GUARDED_BY(mu_) = 0;

  // The last time we logged a task failure.
  int64_t last_log_time_ms_ GUARDED_BY(mu_) = 0;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// This map contains one entry per task that may be submitted for
  /// execution. This includes both tasks that are currently pending execution
  /// and tasks that finished execution but that may be retried again in the
  /// future.
  absl::flat_hash_map<TaskID, TaskEntry> submissible_tasks_ GUARDED_BY(mu_);

  /// Number of tasks that are pending. This is a count of all tasks in
  /// submissible_tasks_ that have been submitted and are currently pending
  /// execution.
  size_t num_pending_tasks_ = 0;

  int64_t total_lineage_footprint_bytes_ GUARDED_BY(mu_) = 0;

  /// Optional shutdown hook to call when pending tasks all finish.
  std::function<void()> shutdown_hook_ GUARDED_BY(mu_) = nullptr;

  friend class TaskManagerTest;
};

}  // namespace core
}  // namespace ray
