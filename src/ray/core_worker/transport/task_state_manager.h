#ifndef RAY_CORE_WORKER_TASK_STATE_MANAGER_H
#define RAY_CORE_WORKER_TASK_STATE_MANAGER_H

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

  virtual void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type) = 0;

  ~TaskFinisherInterface() {}
};

class TaskStateManager : public TaskFinisherInterface {
 public:
  TaskStateManager(std::shared_ptr<CoreWorkerMemoryStore> in_memory_store)
      : in_memory_store_(in_memory_store) {}

  void AddPendingTask(const TaskSpecification &spec);

  /// Write return objects for a pending task to the memory store.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] reply Proto response to a direct actor or task call.
  /// \return Void.
  void CompletePendingTask(const TaskID &task_id,
                           const rpc::PushTaskReply &reply) override;

  /// Treat a pending task as failed.
  ///
  /// \param[in] task_id ID of the pending task.
  /// \param[in] error_type The type of the specific error.
  /// \return Void.
  void FailPendingTask(const TaskID &task_id, rpc::ErrorType error_type) override;

 private:
  std::shared_ptr<CoreWorkerMemoryStore> in_memory_store_;

  absl::Mutex mu_;

  absl::flat_hash_map<TaskID, int64_t> pending_tasks_ GUARDED_BY(mu_);
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_STATE_MANAGER_H
