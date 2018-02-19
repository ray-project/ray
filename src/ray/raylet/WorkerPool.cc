#ifndef WORKER_POOL_CC
#define WORKER_POOL_CC

#include "WorkerPool.h"

#include "common.h"

using namespace std;
namespace ray {

/// A constructor that initializes a worker pool with num_workers workers.
WorkerPool::WorkerPool(int num_workers) {
  for (int i = 0; i < num_workers; i++) {
    pool_.push_back(Worker());
  }
}

/// Create a new worker and add it to the pool
bool WorkerPool::AddWorker() {
  pool_.push_back(Worker());
  return true;
}

uint32_t WorkerPool::PoolSize() const{
  return pool_.size();
}

void WorkerPool::AddWorkerConnection(pid_t pid) {
  LOG_INFO("worker with pid %d", pid);
}

} // end namespace ray

#endif
