
#include "context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext()
    : current_task_id(TaskID::FromRandom()),
      task_index(0),
      put_index(0) {}

  int GetNextTaskIndex() {
    return ++task_index;
  }

  int GetNextPutIndex() {
    return ++put_index;
  }

  const TaskID &GetCurrentTaskID() {
    return current_task_id;
  }

  void SetCurrentTask(const raylet::TaskSpecification &spec) {
    current_task_id = spec.TaskId();
    task_index = 0;
    put_index = 0;
  }

  /// The task ID for current task.
  TaskID current_task_id;

  /// Number of tasks that have been submitted from current task.
  int task_index;

  /// Number of objects that have been put from current task.
  int put_index;  
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context_ = nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const DriverID &driver_id)
  : worker_type(worker_type),
    worker_id(worker_type == WorkerType::DRIVER ?
        ClientID::FromBinary(driver_id.Binary()) :
        ClientID::FromRandom()),
    current_driver_id(worker_type == WorkerType::DRIVER ?
        driver_id : DriverID::Nil()) {

  // For worker main thread which initializes the WorkerContext,
  // set task_id according to whether current worker is a driver.
  // (For other threads it's set to randmom ID via GetThreadContext).
  GetThreadContext().current_task_id = (worker_type == WorkerType::DRIVER) ?
      TaskID::FromRandom() : TaskID::Nil();
}

const WorkerType WorkerContext::GetWorkerType() const {
  return worker_type;
}

const ClientID &WorkerContext::GetWorkerID() const {
  return worker_id;
}

int WorkerContext::GetNextTaskIndex() {
  return GetThreadContext().GetNextTaskIndex();
}

int WorkerContext::GetNextPutIndex() {
  return GetThreadContext().GetNextPutIndex();
}

const DriverID &WorkerContext::GetCurrentDriverID() {
  return current_driver_id;
}

const TaskID &WorkerContext::GetCurrentTaskID() {
  return GetThreadContext().GetCurrentTaskID();
}

void WorkerContext::SetCurrentTask(const raylet::TaskSpecification &spec) {
  current_driver_id = spec.DriverId();
  GetThreadContext().SetCurrentTask(spec);
}

WorkerThreadContext& WorkerContext::GetThreadContext() {
  if (thread_context_ == nullptr) {
    thread_context_ = std::unique_ptr<WorkerThreadContext>(
        new WorkerThreadContext());
  }

  return *thread_context_;
}

}  // namespace ray