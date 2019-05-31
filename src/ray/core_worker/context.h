#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "common.h"
#include "ray/raylet/task_spec.h"

namespace ray {

// Context for core worker.
struct WorkerContext {
  WorkerContext(WorkerType worker_type, const DriverID &driver_id)
    : task_index(0),
      put_index(0) {

    auto initial_driver_id = driver_id;
    if (initial_driver_id.is_nil()) {
      initial_driver_id = DriverID::from_random();
    }
  
    if (worker_type == WorkerType::DRIVER) {
      current_driver_id = initial_driver_id;
      current_task_id = TaskID::from_random();
    } else {
      current_driver_id = DriverID::nil();
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

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H