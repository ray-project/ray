#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "common.h"
#include "object_interface.h"
#include "ray/common/buffer.h"
#include "task_execution.h"
#include "task_interface.h"

namespace ray {

// Task related context for worker. 
struct WorkerContext {
  WorkerContext(WorkerType worker_type, const DriverID &driver_id)
    : worker_type(worker_type),
      task_index(0),
      put_index(0) {

    if (driver_id.is_nil()) {
      driver_id = DriverID::from_random();
    }

    if (worker_type == WorkerType::DRIVER) {
      worker_id = driver_id;
      current_driver_id = driver_id;
      current_task_id = TaskID::from_random();
    } else {
      worker_id = ClientID::from_random();
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

  void SetCurrentTask(const TaskSpecification &spec) {
    current_driver_id = spec.DriverId();
    current_task_id = spec.TaskId();
    task_index = 0;
    put_index = 0;
  }

  /// The type of the worker (Driver/Worker).
  const WorkerType worker_type;

  /// The ID for this worker (aka ClientID).
  ClientID worker_id;

  /// The driver ID for current task.
  static thread_local DriverID current_driver_id;

  /// The task ID for current task.
  static thread_local TaskID current_task_id;

  /// Number of tasks that have been submitted from current task.
  static thread_local int task_index;

  /// Number of objects that have been put from current task.
  static thread_local int put_index;  
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
  CoreWorker(const WorkerType worker_type,
      const Language language,
      const std::string &store_socket,
      const std::string &raylet_socket,
      DriverID driver_id = DriverID::nil())
      : worker_type_(worker_type),
        language_(language),
        task_interface_(*this),
        object_interface_(*this),
        task_execution_interface_(*this),
        context_(worker_type, driver_id),
        store_client_(store_socket),
        raylet_client_(raylet_socket, context_.worker_id, (worker_type == WorkerType::Worker),
            context_.current_driver_id, language) {}

  /// Connect this worker to Raylet.
  Status Connect() { return Status::OK(); }

  /// Type of this worker.
  WorkerType WorkerType() const { return worker_type_; }

  /// Language of this worker.
  Language Language() const { return language_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() { return task_execution_interface_; }

  WorkerContext &GetWorkerContext() { return context_; }

 private:
  /// Type of this worker.
  const enum WorkerType worker_type_;

  /// Language of this worker.
  const enum Language language_;

  /// The `CoreWorkerTaskInterface` instance.
  CoreWorkerTaskInterface task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  CoreWorkerObjectInterface object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  CoreWorkerTaskExecutionInterface task_execution_interface_;

  /// Task context shared by task_interface_ and task_execution_interface_.
  WorkerContext context_;

  /// Plasma store client.
  PlasmaClient store_client_;

  /// Raylet client.
  RayletClient raylet_client_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
