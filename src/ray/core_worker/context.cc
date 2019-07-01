
#include "ray/core_worker/context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext()
      : current_task_id(TaskID::FromRandom()), task_index(0), put_index(0) {}

  int GetNextTaskIndex() { return ++task_index; }

  int GetNextPutIndex() { return ++put_index; }

  const TaskID &GetCurrentTaskID() const { return current_task_id; }

  std::shared_ptr<const raylet::TaskSpecification> GetCurrentTask() const {
    return current_task;
  }

  void SetCurrentTaskId(const TaskID &task_id) {
    current_task_id = task_id;
    task_index = 0;
    put_index = 0;
  }

  void SetCurrentTask(const raylet::TaskSpecification &spec) {
    SetCurrentTaskId(spec.TaskId());
    current_task = std::make_shared<const raylet::TaskSpecification>(spec);
  }

 private:
  /// The task ID for current task.
  TaskID current_task_id;

  /// The current task.
  std::shared_ptr<const raylet::TaskSpecification> current_task;

  /// Number of tasks that have been submitted from current task.
  int task_index;

  /// Number of objects that have been put from current task.
  int put_index;
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context_ =
    nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const JobID &job_id)
    : worker_type(worker_type),
      // TODO(qwang): Assign the driver id to worker id
      // once we treat driver id as a special worker id.
      worker_id(worker_type == WorkerType::DRIVER ? WorkerID::FromBinary(job_id.Binary())
                                                  : WorkerID::FromRandom()),
      current_job_id(worker_type == WorkerType::DRIVER ? job_id : JobID::Nil()) {
  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to random ID via GetThreadContext).
  GetThreadContext().SetCurrentTaskId(
      (worker_type == WorkerType::DRIVER) ? TaskID::FromRandom() : TaskID::Nil());
}

const WorkerType WorkerContext::GetWorkerType() const { return worker_type; }

const WorkerID &WorkerContext::GetWorkerID() const { return worker_id; }

int WorkerContext::GetNextTaskIndex() { return GetThreadContext().GetNextTaskIndex(); }

int WorkerContext::GetNextPutIndex() { return GetThreadContext().GetNextPutIndex(); }

const JobID &WorkerContext::GetCurrentJobID() const { return current_job_id; }

const TaskID &WorkerContext::GetCurrentTaskID() const {
  return GetThreadContext().GetCurrentTaskID();
}

void WorkerContext::SetCurrentTask(const raylet::TaskSpecification &spec) {
  current_job_id = spec.JobId();
  GetThreadContext().SetCurrentTask(spec);
}

std::shared_ptr<const raylet::TaskSpecification> WorkerContext::GetCurrentTask() const {
  return GetThreadContext().GetCurrentTask();
}

WorkerThreadContext &WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray
