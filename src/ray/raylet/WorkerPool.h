#ifndef WORKER_POOL_H
#define WORKER_POOL_H

#include <inttypes.h>
#include <list>

#include "Worker.h"

using namespace std;
namespace ray {

class ClientConnection;
class Worker;

/// WorkerPool class is responsible for managing a pool of workers and
/// abstracts away the implementation details of workers. From Raylet's
/// perspective a Worker is a container that encapsulates a unit of work.
class WorkerPool {
public:
  /// Constructor that creates a pool with a set of workers of specified size.
  WorkerPool(int num_workers);
  /// Get the number of idle workers.
  uint32_t PoolSize() const;
  /// Start a worker. The worker will be added to the pool after it
  /// connects, registers, and requests a task assignment.
  bool StartWorker();
  /// Register a new worker. The worker will be added to the pool after it
  /// requests a task assignment.
  void RegisterWorker(std::shared_ptr<Worker> worker);
  /// Get the client connection's registered worker. Returns nullptr if the
  /// client has not registered a worker yet.
  const std::shared_ptr<Worker> GetRegisteredWorker(std::shared_ptr<ClientConnection> connection) const;
  /// Disconnect the given worker. Returns true if the given worker was in the
  /// pool of idle workers.
  bool DisconnectWorker(shared_ptr<Worker> worker);

  /// Add an idle worker to the pool.
  void PushWorker(std::shared_ptr<Worker> worker);
  /// Pop an idle worker from the pool. Returns nullptr if the pool is empty.
  std::shared_ptr<Worker> PopWorker();

  /// Destructor responsible for freeing a set of workers owned by this class.
  ~WorkerPool();
private:
  /// The initial size of the worker pool. Current size is the size of the
  /// worker pool container.
  int init_size_;
  /// The pool of idle workers.
  std::list<std::shared_ptr<Worker>> pool_;
  /// All workers that have registered and are still connected, including both
  /// idle and executing.
  // TODO(swang): Make this a map to make GetRegisteredWorker faster.
  std::list<std::shared_ptr<Worker>> registered_workers_;
};
} // end namespace ray

#endif
