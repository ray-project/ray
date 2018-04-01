#ifndef RAY_RAYLET_WORKER_POOL_H
#define RAY_RAYLET_WORKER_POOL_H

#include <inttypes.h>
#include <list>

#include "ray/common/client_connection.h"
#include "ray/raylet/worker.h"

namespace ray {

namespace raylet {

class Worker;

/// \class WorkerPool
///
/// The WorkerPool is responsible for managing a pool of Workers. Each Worker
/// is a container for a unit of work.
class WorkerPool {
 public:
  /// Create a pool and asynchronously start the specified number of workers.
  /// Once each worker process has registered with an external server, the
  /// process should create and register a new Worker, then add itself to the
  /// pool.
  ///
  /// \param num_workers The number of workers to start.
  WorkerPool(int num_workers, const std::vector<const char *> &worker_command);

  /// Destructor responsible for freeing a set of workers owned by this class.
  ~WorkerPool();

  /// Get the number of idle workers in the pool.
  ///
  /// \return The number of idle workers.
  uint32_t PoolSize() const;

  /// Asynchronously start a new worker process. Once the worker process has
  /// registered with an external server, the process should create and
  /// register a new Worker, then add itself to the pool. Failure to start
  /// the worker process is a fatal error.
  void StartWorker();

  /// Register a new worker. The Worker should be added by the caller to the
  /// pool after it becomes idle (e.g., requests a work assignment).
  ///
  /// \param The Worker to be registered.
  void RegisterWorker(std::shared_ptr<Worker> worker);

  /// Get the client connection's registered worker.
  ///
  /// \param The client connection owned by a registered worker.
  /// \return The Worker that owns the given client connection. Returns nullptr
  /// if the client has not registered a worker yet.
  const std::shared_ptr<Worker> GetRegisteredWorker(
      std::shared_ptr<LocalClientConnection> connection) const;

  /// Disconnect a registered worker.
  ///
  /// \param The worker to disconnect. The worker must be registered.
  /// \return Whether the given worker was in the pool of idle workers.
  bool DisconnectWorker(std::shared_ptr<Worker> worker);

  /// Add an idle worker to the pool. The worker's task assignment will be
  /// reset.
  ///
  /// \param The idle worker to add.
  void PushWorker(std::shared_ptr<Worker> worker);

  /// Pop an idle worker from the pool. The caller is responsible for pushing
  /// the worker back onto the pool once the worker has completed its work.
  ///
  /// \return An idle worker. Returns nullptr if the pool is empty.
  std::shared_ptr<Worker> PopWorker();

 private:
  std::vector<const char *> worker_command_;
  /// The pool of idle workers.
  std::list<std::shared_ptr<Worker>> pool_;
  /// All workers that have registered and are still connected, including both
  /// idle and executing.
  // TODO(swang): Make this a map to make GetRegisteredWorker faster.
  std::list<std::shared_ptr<Worker>> registered_workers_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_POOL_H
