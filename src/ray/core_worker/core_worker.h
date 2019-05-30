#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "common.h"
#include "object_interface.h"
#include "ray/common/buffer.h"
#include "task_execution.h"
#include "task_interface.h"

namespace ray {

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
        task_interface_(*this),
        object_interface_(*this),
        task_execution_interface_(*this) {}

  /// Connect this worker to Raylet.
  Status Connect() { return Status::OK(); }

  /// Type of this worker.
  enum WorkerType WorkerType() const { return worker_type_; }

  /// Language of this worker.
  enum Language Language() const { return language_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() { return task_execution_interface_; }

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
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
