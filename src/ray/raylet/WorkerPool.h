#ifndef WORKER_POOL_H
#define WORKER_POOL_H

#include "Worker.h"

#include <inttypes.h>
#include <list>

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
  /// Add a worker to the pool
  bool AddWorker();
  uint32_t PoolSize() const;

  Worker PopWorker();

  void AddWorkerConnection(pid_t pid);

  /// Destructor responsible for freeing a set of workers owned by this class.
  ~WorkerPool() {}
private:
  /// The initial size of the worker pool. Current size is the size of the
  /// worker pool container.
  int init_size_;
  std::list<Worker> pool_;

  std::list<std::shared_ptr<ClientConnection>> connections_;
};
} // end namespace ray

#endif
