#ifndef RAY_CORE_WORKER_TASK_MANAGER_H
#define RAY_CORE_WORKER_TASK_MANAGER_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/actor_manager.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

class TaskFinisherInterface {
 public:
  virtual void CompletePendingTask(const TaskID &task_id, const rpc::PushTaskReply &reply,
                                   const rpc::Address &actor_addr) = 0;

  virtual void PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type,
                                 Status *status = nullptr) = 0;

  virtual void OnTaskDependenciesInlined(
      const std::vector<ObjectID> &inlined_dependency_ids,
      const std::vector<ObjectID> &contained_ids) = 0;

  virtual ~TaskFinisherInterface() {}
};

using RetryTaskCallback = std::function<void(const TaskSpecification &spec)>;

class TaskManager : public TaskFinisherInterface {
 public:
  TaskManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
              std::shared_ptr<ReferenceCounter> reference_counter,
              std::shared_ptr<ActorManagerInterface> actor_manager,
              RetryTaskCallback retry_task_callback)
      : in_memory_store_(in_memory_store),
        reference_counter_(reference_counter),
        actor_manager_(actor_manager),
        retry_task_callback_(retry_task_callback) {}

  /// Add a task that is pending execution.
  ///
  /// \param[in] caller_id The TaskID of the calling task.
  /// \param[in] caller_address The rpc address of the calling task.
  /// \param[in] spec The spec of the pending task.
  /// \param[in] max_retries Number of times this task may be retried
  /// on failure.
  /// \return Void.
  void AddPendingTask(const TaskID &caller_id, const rpc::Address &caller_address,
                      const TaskSpecification &spec, const std::string &call_site, int max_retries = 0);

  /// Wait for all pending tasks to finish, and then shutdown.
  ///
  /// \param shutdown The shutdown callback to call.
  void DrainAndShutdown(std::function<void()> shutdown);

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  bool IsTaskPending(const TaskID &task_id) const;

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \param[in] worker_addr Address of the worker that executed the task.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id, const rpc::PushTaskReply &reply,
                           const rpc::Address &worker_addr) override;

  /// A pending task failed. This will either retry the task or mark the task
  /// as failed if there are no retries left.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \param[in] status Optional status message.
  void PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type,
                         Status *status = nullptr) override;

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

  /// Return the spec for a pending task.
  TaskSpecification GetTaskSpec(const TaskID &task_id) const;

  /// Return the number of pending tasks.
  int NumPendingTasks() const { return pending_tasks_.size(); }

 private:
  /// Treat a pending task as failed. The lock should not be held when calling
  /// this method because it may trigger callbacks in this or other classes.
  void MarkPendingTaskFailed(const TaskID &task_id, const TaskSpecification &spec,
                             rpc::ErrorType error_type) LOCKS_EXCLUDED(mu_);

  /// Helper function to call RemoveSubmittedTaskReferences on the remaining
  /// dependencies of the given task spec after the task has finished or
  /// failed. The remaining dependencies are plasma objects and any ObjectIDs
  /// that were inlined in the task spec.
  void RemoveFinishedTaskReferences(
      TaskSpecification &spec, const rpc::Address &worker_addr,
      const ReferenceCounter::ReferenceTableProto &borrowed_refs);

  /// Shutdown if all tasks are finished and shutdown is scheduled.
  void ShutdownIfNeeded() LOCKS_EXCLUDED(mu_);

  /// Used to store task results.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Used for reference counting objects.
  /// The task manager is responsible for managing all references related to
  /// submitted tasks (dependencies and return objects).
  std::shared_ptr<ReferenceCounter> reference_counter_;

  // Interface for publishing actor creation.
  std::shared_ptr<ActorManagerInterface> actor_manager_;

  /// Called when a task should be retried.
  const RetryTaskCallback retry_task_callback_;

  // The number of task failures we have logged total.
  int64_t num_failure_logs_ GUARDED_BY(mu_) = 0;

  // The last time we logged a task failure.
  int64_t last_log_time_ms_ GUARDED_BY(mu_) = 0;

  /// Protects below fields.
  mutable absl::Mutex mu_;

  /// Map from task ID to a pair of:
  ///   {task spec, number of allowed retries left}
  /// This map contains one entry per pending task that we submitted.
  /// TODO(swang): The TaskSpec protobuf must be copied into the
  /// PushTaskRequest protobuf when sent to a worker so that we can retry it if
  /// the worker fails. We could avoid this by either not caching the full
  /// TaskSpec for tasks that cannot be retried (e.g., actor tasks), or by
  /// storing a shared_ptr to a PushTaskRequest protobuf for all tasks.
  absl::flat_hash_map<TaskID, std::pair<TaskSpecification, int>> pending_tasks_
      GUARDED_BY(mu_);

  /// Optional shutdown hook to call when pending tasks all finish.
  std::function<void()> shutdown_hook_ GUARDED_BY(mu_) = nullptr;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_MANAGER_H
