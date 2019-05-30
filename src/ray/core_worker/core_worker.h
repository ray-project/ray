#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "common.h"
#include "object_interface.h"
#include "ray/common/buffer.h"
#include "task_execution.h"
#include "task_interface.h"
#include "ray/raylet/raylet_client.h"

namespace ray {

struct WorkerContext;

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
      DriverID driver_id = DriverID::nil());

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

 private:
  /// Get worker context.
  WorkerContext& GetContext() { return GetPerThreadContext(worker_type_, driver_id_); }

  /// Get per thread worker context.
  static WorkerContext& GetPerThreadContext(const enum WorkerType worker_type, DriverID driver_id);

  /// Type of this worker.
  const enum WorkerType worker_type_;

  /// Language of this worker.
  const enum Language language_;

  /// Worker context per thread.
  static thread_local std::unique_ptr<WorkerContext> context_;

  /// Plasma store socket name.
  std::string store_socket_;

  /// Raylet client.
  std::unique_ptr<RayletClient> raylet_client_;

  /// ID of the driver (valid when this worker is a driver).
  DriverID driver_id_;

  /// The `CoreWorkerTaskInterface` instance.
  CoreWorkerTaskInterface task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  CoreWorkerObjectInterface object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  CoreWorkerTaskExecutionInterface task_execution_interface_;

  friend class CoreWorkerTaskInterface;
  friend class CoreWorkerObjectInterface;
  friend class CoreWorkerTaskExecutionInterface;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
