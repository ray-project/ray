
#include "context.h"

namespace ray {

/// per-thread context for core worker.
struct WorkerThreadContext {
  WorkerThreadContext(WorkerType worker_type, const DriverID &driver_id)
    : task_index(0),
      put_index(0) {

    auto initial_driver_id = driver_id;
    if (initial_driver_id.is_nil()) {
      initial_driver_id = DriverID::from_random();
    }
  
    if (worker_type == WorkerType::DRIVER) {
      current_task_id = TaskID::from_random();
    } else {
      current_task_id = TaskID::nil();
    }
  }

  int GetNextTaskIndex() {
    return ++task_index;
  }

  int GetNextPutIndex() {
    return ++put_index;
  }

  const DriverID &GetCurrentDriverID() {
    return current_driver_id;
  }

  const TaskID &GetCurrentTaskID() {
    return current_task_id;
  }

  void SetCurrentTask(const raylet::TaskSpecification &spec) {
    current_driver_id = spec.DriverId();
    current_task_id = spec.TaskId();
    task_index = 0;
    put_index = 0;
  }

  /// The driver ID for current task.
  DriverID current_driver_id;

  /// The task ID for current task.
  TaskID current_task_id;

  /// Number of tasks that have been submitted from current task.
  int task_index;

  /// Number of objects that have been put from current task.
  int put_index;  
};

thread_local std::unique_ptr<WorkerThreadContext> WorkerContext::thread_context = nullptr;

WorkerContext::WorkerContext(WorkerType worker_type, const DriverID &driver_id)
  : worker_type(worker_type),
    worker_id(worker_type == WorkerType::DRIVER ?
        ClientID::from_binary(driver_id.binary()) :
        ClientID::from_random()),
    current_driver_id(worker_type == WorkerType::DRIVER ?
        driver_id : DriverID::nil()) {}

int WorkerContext::GetNextTaskIndex() {
  return GetThreadContext().GetNextTaskIndex();
}

int WorkerContext::GetNextPutIndex() {
  return GetThreadContext().GetNextPutIndex();
}

const DriverID &WorkerContext::GetCurrentDriverID() {
  return GetThreadContext().GetCurrentDriverID();
}

const TaskID &WorkerContext::GetCurrentTaskID() {
  return GetThreadContext().GetCurrentTaskID();
}

void WorkerContext::SetCurrentTask(const raylet::TaskSpecification &spec) {
  GetThreadContext().SetCurrentTask(spec);
}

WorkerThreadContext& WorkerContext::GetThreadContext() {
  return GetThreadContext(worker_type, current_driver_id);
}

WorkerThreadContext& WorkerContext::GetThreadContext(
    const enum WorkerType worker_type, DriverID driver_id) {
  if (thread_context == nullptr) {
    thread_context = std::unique_ptr<WorkerThreadContext>(
        new WorkerThreadContext(worker_type, driver_id));
  }

  return *thread_context;
}

}  // namespace ray