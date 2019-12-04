#ifndef RAY_CORE_WORKER_TASK_MANAGER_H
#define RAY_CORE_WORKER_TASK_MANAGER_H

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/id.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/protobuf/core_worker.pb.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {

class TaskFinisherInterface {
 public:
  virtual void CompletePendingTask(const TaskID &task_id,
                                   const rpc::PushTaskReply &reply) = 0;

  virtual void PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type) = 0;

  virtual ~TaskFinisherInterface() {}
};

using RetryTaskCallback = std::function<void(const TaskSpecification &spec)>;

class TaskManager : public TaskFinisherInterface {
 public:
  TaskManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store,
              RetryTaskCallback retry_task_callback)
      : in_memory_store_(in_memory_store), retry_task_callback_(retry_task_callback) {}

  /// Add a task that is pending execution.
  ///
  /// \param[in] spec The spec of the pending task.
  /// \param[in] max_retries Number of times this task may be retried
  /// on failure.
  /// \return Void.
  void AddPendingTask(const TaskSpecification &spec, int max_retries = 0);

  /// Return whether the task is pending.
  ///
  /// \param[in] task_id ID of the task to query.
  /// \return Whether the task is pending.
  bool IsTaskPending(const TaskID &task_id) const {
    return pending_tasks_.count(task_id) > 0;
  }

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply) override;

  /// A pending task failed. This will either retry the task or mark the task
  /// as failed if there are no retries left.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  void PendingTaskFailed(const TaskID &task_id, rpc::ErrorType error_type) override;

 private:
  /// Treat a pending task as failed. The lock should not be held when calling
  /// this method because it may trigger callbacks in this or other classes.
  void MarkPendingTaskFailed(const TaskID &task_id, int64_t num_returns,
                             rpc::ErrorType error_type) LOCKS_EXCLUDED(mu_);

  /// Used to store task results.
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  /// Called when a task should be retried.
  const RetryTaskCallback retry_task_callback_;

  /// Protects below fields.
  absl::Mutex mu_;

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
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_MANAGER_H
