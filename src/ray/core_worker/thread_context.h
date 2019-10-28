#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "ray/core_worker/common.h"

namespace ray {

struct ThreadContext {
  ThreadContext()
      : current_task_id_(TaskID::ForFakeTask()), task_index_(0), put_index_(0) {}

  /// Get the per-thread worker context for the currently executing thread.
  static ThreadContext &Get(bool for_main_thread = false);

  int GetNextTaskIndex() { return ++task_index_; }

  int GetNextPutIndex() { return ++put_index_; }

  const TaskID &GetCurrentTaskID() const { return current_task_id_; }

  void SetCurrentTaskId(const TaskID &task_id) { current_task_id_ = task_id; }

  void ResetCurrentTaskId() {
    SetCurrentTaskId(TaskID::Nil());
    task_index_ = 0;
    put_index_ = 0;
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id_;

  /// Number of tasks that have been submitted from current task.
  int task_index_;

  /// Number of objects that have been put from current task.
  int put_index_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
