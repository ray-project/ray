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
  /// connects and registers itself.
  bool StartWorker();
  /// Add an idle worker to the pool.
  void AddWorker(Worker &&worker);
  /// Pop an idle worker from the pool. The pool must be nonempty.
  Worker PopWorker();
  /// Remove the worker with the given connection.
  void RemoveWorker(shared_ptr<ClientConnection> connection);

  /// Destructor responsible for freeing a set of workers owned by this class.
  ~WorkerPool() {}
private:
  /// The initial size of the worker pool. Current size is the size of the
  /// worker pool container.
  int init_size_;
  std::list<Worker> pool_;
};
} // end namespace ray

#endif
