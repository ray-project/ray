#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "common.h"
#include "object_interface.h"
#include "ray/common/buffer.h"
#include "task_execution.h"
#include "task_interface.h"

namespace ray {

// Task related context for worker. 
// TODO: This is actually shared between TaskInterface and TaskExecutionInterface,
// consider combine TaskInterface and TaskExecutionInterface, and make
// this structure to be part of the combined one.
struct TaskContext {
  TaskContext(WorkerType worker_type)
    : worker_type(worker_type),
      task_index(0) {
    if (worker_type == WorkerType::DRIVER) {
      current_driver_id = DriverID::from_random();
      current_task_id = TaskID::from_random();
    } else {
      current_driver_id = DriverID::nil();
      current_task_id = TaskID::nil();
    }
  }

  int GetNextTaskIndex() {
    return ++task_index;
  }

  const DriverID &GetCurrentDriverID() {
    return current_driver_id;
  }

  const TaskID &GetCurrentTaskID() {
    return current_task_id;
  }

  void SetCurrentTask(const TaskSpecification &spec) {
    current_driver_id = spec.DriverId();
    current_task_id = spec.TaskId();
    task_index = 0;
  }

  /// The type of the worker (Driver/Worker).
  const WorkerType worker_type;

  /// The driver ID for current task.
  static thread_local DriverID current_driver_id;

  /// The task ID for current task.
  static thread_local TaskID current_task_id;

  /// Number of tasks that have been submitted from current task.
  int task_index;
};

/// The root class that contains all the core and language-independent functionalities
/// of the worker. This class is supposed to be used to implement app-language (Java,
/// Python, etc) workers.
class CoreWorker {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] langauge Language of this worker.
  CoreWorker(const WorkerType worker_type, const Language language)
      : worker_type_(worker_type),
        language_(language),
        task_interface_(this),
        object_interface_(this),
        task_execution_interface_(this){};

  /// Connect this worker to Raylet.
  Status Connect() { return Status::OK(); }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() { return task_execution_interface_; }

  /// Return the type of the worker.
  const WorkerType GetWorkerType() const { return worker_type_; }

  TaskContext &GetTaskContext() { return context_; }

 private:
  /// Type of this worker.
  const WorkerType worker_type_;

  /// Language of this worker.
  const Language language_;

  /// The `CoreWorkerTaskInterface` instance.
  const CoreWorkerTaskInterface task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  const CoreWorkerObjectInterface object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  const CoreWorkerTaskExecutionInterface task_execution_interface_;

  /// Task context shared by task_interface_ and task_execution_interface_.
  TaskContext context_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
