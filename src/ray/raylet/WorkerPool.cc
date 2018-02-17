#ifndef WORKER_POOL_CC
#define WORKER_POOL_CC

#include "WorkerPool.h"

using namespace std;
namespace ray {

/// A constructor that initializes a worker pool with num_workers workers.
WorkerPool::WorkerPool(int num_workers) {
  for (int i = 0; i < num_workers; i++) {
    pool_.push_back(std::move(Worker()));
  }
}

/// Create a new worker and add it to the pool
WorkerPool::AddWorker() {
  pool_.push_back(std::move(Worker()));
}

uint32_t WorkerPool::PoolSize() const{
  return pool_.size();
}

} // end namespace ray

#endif
